var util = require('util'),
    flags = require('flags'),
    MongoClient = require('mongodb').MongoClient,
    SkyProvider = require('skyprovider'),
    WebSocket = require('ws'),
    WebSocketServer = require('ws').Server,
    Log = require('modulelog'),
    membersJustAdded = {},
    gracePeriod = 15 * 60 * 1000, //give clients 15 minutes to start up before trying to remove
    allClients = [];

flags.defineString('ip', '0.0.0.0', 'listen ip');
flags.defineInteger('port', 27018, 'listen port');
flags.defineString('mongo', 'mongodb://127.0.0.1:27017', 'mongodb connect string');
flags.defineBoolean('clean', true, 'clean up dead replicas');
flags.defineString('skyapi', '', 'skyapi address to advertise to');
flags.defineString('logger', 'default', 'the class to use for logging');
flags.defineString('log-level', 'info', 'the log level');
flags.parse();

Log.setClass(flags.get('logger'), 'mongodb-replica-maintainer');
Log.setLevel(flags.get('log-level'));

function cleanupDeadReplicas(db) {
    var adminDB = db.admin();
    adminDB.command({replSetGetStatus: 1}, function(err, status) {
        if (!status || !status.ok || !status.members) {
            if (!status) {
                Log.error('Error calling replSetGetStatus', {error: err});
            } else {
                Log.error('Error calling replSetGetStatus', {status: status});
            }
            return;
        }
        var i = 0,
            now = Date.now(),
            idsToRemove = [],
            member;
        for (; i < status.members.length; i++) {
            member = status.members[i];
            //state of 1 means primary
            if (member.state !== 1 && !member.health && status.date - member.lastHeartbeatRecv > (3600 * 1000)) {
                if (membersJustAdded.hasOwnProperty(member.name)) {
                    if (now - membersJustAdded[member.name] < gracePeriod) {
                        continue;
                    }
                    delete membersJustAdded[member.name];
                }
                idsToRemove.push(status.members[i]._id);
            }
        }
        Log.debug('Removing dead replicas', {count: idsToRemove.length});
        if (idsToRemove.length < 1) {
            return;
        }
        db.collection('system.replset').findOne({}, function(err, doc) {
            if (err) {
                cb(err);
                return;
            }
            var changed = false;
            for (i = 0; i < doc.members.length; i++) {
                if (idsToRemove.indexOf(doc.members[i]._id) !== -1) {
                    Log.info('Removing dead replica', {host: doc.members[i].host});
                    broadcastRemoval(doc.members[i].host);
                    doc.members.splice(i, 1);
                    i--;
                    changed = true;
                }
            }
            if (!changed) {
                return;
            }
            doc.version++;
            db.admin().command({replSetReconfig: doc}, function(err, res) {
                if (err) {
                    Log.error('Error removing dead replicas', {error: err});
                }
            });
        });
    });
}

function broadcastRemoval(host) {
    var msg = JSON.stringify({removed: host});
    for (var i = 0; i < allClients.length; i++) {
        try {
            allClients[i].send(msg);
        } catch (e) {
            Log.error('Error broadcasting removal', {error: e});
            allClients.splice(i, 1);
            i--;
        }
    }
}

function addNewReplicaMember(db, host, hidden, priority, votes, cb) {
    db.collection('system.replset').findOne({}, function(err, doc) {
        if (err) {
            cb(err);
            return;
        }
        if (!doc || !Array.isArray(doc.members)) {
            Log.error('invalid replset doc', {doc: doc});
            cb(new TypeError('Invalid members array in replset collection'));
            return;
        }
        if (host.indexOf(':') === -1) {
            host += ':27017';
        }
        var found = -1,
            highestID = 0,
            i = 0,
            changed = false;
        for (; i < doc.members.length; i++) {
            if (doc.members[i].host === host) {
                found = i;
                break;
            }
            highestID = Math.max(highestID, doc.members[i]._id);
        }
        if (found < 0) {
            highestID++;
            Log.debug('Adding new replica', {host: host, id: highestID});
            //priority 0 means it will never be a primary
            doc.members.push({_id: highestID, host: host, priority: priority, votes: votes, hidden: hidden});
            changed = true;
        } else {
            if (doc.members[found].hidden !== hidden) {
                doc.members[found].hidden = hidden;
                changed = true;
            }
            if (doc.members[found].priority !== priority) {
                doc.members[found].priority = priority;
                changed = true;
            }
            if (doc.members[found].votes !== votes) {
                doc.members[found].votes = votes;
                changed = true;
            }
            if (changed) {
                Log.debug('Updating existing replica', {host: host});
            }
        }
        if (!changed) {
            cb(null, host);
            return;
        }
        doc.version++;
        db.admin().command({replSetReconfig: doc}, function(err, res) {
            if (err) {
                cb(err);
                return;
            }
            membersJustAdded[host] = Date.now();
            cb(null, host);
        });
    });
}

function handleMessage(db, ws, message) {
    var ip = ws._socket.remoteAddress,
        command;
    try {
        command = JSON.parse(message);
    } catch (e) {
        Log.error('Received invalid json', {error: e, ip: ip});
        ws.close();
        return;
    }
    if (!command.cmd) {
        Log.error('Received message without cmd', {command: command, ip: ip});
        ws.send('{"success":false,"error":"no cmd"}');
        return;
    }
    switch (command.cmd) {
        case 'add':
            //todo: validate the host
            if (!command.host) {
                Log.debug('Assuming ip from sender', {ip: ip});
                command.host = ip;
            }
            if (command.hidden === undefined) {
                command.hidden = false;
            }
            if (command.priority === undefined) {
                command.priority = 0;
            }
            if (command.votes === undefined) {
                command.votes = 0;
            }
            addNewReplicaMember(db, command.host, command.hidden, command.priority, command.votes, function(err, hostAdded) {
                if (err) {
                    Log.error('Failed to add replica', {ip: ip, host: command.host, error: err});
                    ws.send('{"success":false,"error":"internal error"}');
                    return;
                }
                Log.info('Added new replica', {host: hostAdded, ip: ip});
                //make sure the socket is still open before trying to respond
                if (ws.readyState !== WebSocket.OPEN) {
                    return;
                }
                ws.send(JSON.stringify({added: hostAdded, success: true}));
                allClients.push(ws);
            });
            break;
        default:
            Log.error('Unknown command received', {command: command.cmd, ip: ip});
            ws.send('{"success":false,"error":"unknown cmd"}');
            break;
    }
}

function advertiseToSkyAPI() {
    var skyapiAddr = flags.get('skyapi'),
        port = flags.get('port');
    if (!skyapiAddr) {
        return;
    }
    if (skyapiAddr.indexOf('://') === -1) {
        skyapiAddr = 'ws://' + skyapiAddr;
    }
    if (skyapiAddr.indexOf('/provide') === -1) {
        skyapiAddr += '/provide';
    }
    Log.info('Advertising to skyapi', {skyapi: skyapiAddr, port: port});
    var provider = new SkyProvider(skyapiAddr);
    provider.provideService('mongodb-replica-maintainer', port);
}

var options = {},
    serverOptions = {
        autoReconnect: true,
        keepAlive: 5000, //wait 5 seconds before starting keep-alive
        connectTimeoutMS: 5000,
        noDelay: true
    };
options.db = {
    bufferMaxEntries: 0 //do not buffer any commands
};
options.server = {
    socketOptions: serverOptions,
    reconnectTries: 432000, //5 days in seconds
    reconnectInterval: 1000
};
options.replSet = {
    socketOptions: serverOptions,
    connectWithNoPrimary: false
};
MongoClient.connect(flags.get('mongo'), options, function(err, db) {
    if (err) {
        Log.error('Error connecting to mongo', {error: err});
        throw err;
    }
    var topology = db.s.topology,
        localDB = db.db('local'),
        numNoPrimaries = 0,
        server = null;

    Log.info('Connected to mongo', {addr: flags.get('mongo')});

    if (topology) {
        topology.on('ha', function(type, data) {
            if (type !== 'end') {
                return;
            }
            if (data.state.primary) {
                numNoPrimaries = 0;
                return;
            }
            if (!data.state.primary) {
                Log.error('Mongo topology lost connection to primary!', {
                    state: data.state,
                    numNoPrimaries: numNoPrimaries
                });
                if (!data.state.secondaries || data.state.secondaries.length < 1) {
                    Log.fatal('Lost connection to all mongo instances!');
                    return;
                }
                if (numNoPrimaries++ >= 1) {
                    Log.fatal('Lost connection to all mongo instances!');
                }
            }
        }.bind(this));
        topology.on('left', function(serverType, server) {
            //so server.name should be a getter to get the serverDetails
            //see: https://github.com/christkv/mongodb-core/blob/master/lib/topologies/server.js#L556
            Log.warn('Lost connection to mongo server', {type: server.name});
        }.bind(this));
    }

    server = new WebSocketServer({port: flags.get('port'), host: flags.get('ip')}, advertiseToSkyAPI);
    server.on('connection', function(ws) {
        var ip = ws._socket.remoteAddress;
        Log.debug('New ws connection', {ip: ip});

        ws.on('message', function(message) {
            Log.debug('New ws message', {message: message, ip: ip});
            try {
                handleMessage(localDB, ws, message);
            } catch (e) {
                Log.error('Error handling message', {error: e, ip: ip});
            }
        });
        ws.on('error', function(err) {
            Log.error('Error from ws', {err: err, ip: ip});
            var i = allClients.indexOf(ws);
            if (i !== -1) {
                allClients.splice(i, 1);
            }
        });
        ws.on('close', function() {
            Log.debug('Close from ws', {ip: ip});
            var i = allClients.indexOf(ws);
            if (i !== -1) {
                allClients.splice(i, 1);
            }
        });
    });

    if (flags.get('clean')) {
        setInterval(cleanupDeadReplicas.bind(null, localDB), 60000);
    }
});