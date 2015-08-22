var util = require('util'),
    flags = require('flags'),
    MongoClient = require('mongodb').MongoClient,
    SkyProvider = require('skyprovider'),
    WebSocket = require('ws'),
    WebSocketServer = require('ws').Server,
    debug = util.debuglog('mongodb-replica-maintainer'),
    membersJustAdded = {},
    gracePeriod = 15 * 60 * 1000, //give clients 15 minutes to start up before trying to remove
    allClients = [];

flags.defineString('ip', '0.0.0.0', 'listen ip');
flags.defineInteger('port', 27018, 'listen port');
flags.defineString('mongo', 'mongodb://127.0.0.1:27017', 'mongodb connect string');
flags.defineBoolean('clean', true, 'clean up dead replicas');
flags.defineString('skyapi', '', 'skyapi address to advertise to');
flags.parse();

function cleanupDeadReplicas(db) {
    var adminDB = db.admin();
    adminDB.command({replSetGetStatus: 1}, function(err, status) {
        if (!status || !status.ok || !status.members) {
            if (!status) {
                debug('Error calling replSetGetStatus', err);
            } else {
                debug('Error calling replSetGetStatus', status);
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
                    debug('Removing dead replica', doc.members[i].host);
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
                    debug('Error removing dead replica', doc);
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
            debug('Error broadcasting removal', e);
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
            debug('invalid replset doc', doc);
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
            debug('adding', host, 'to replica set with id', highestID);
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
        debug(ip, '- invalid json:', e);
        ws.close();
        return;
    }
    if (!command.cmd) {
        debug(ip, '- command is missing cmd key');
        ws.send('{"success":false}');
        return;
    }
    switch (command.cmd) {
        case 'add':
            //todo: validate the host
            if (!command.host) {
                debug(ip, '- client didnt send host, assuming ip');
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
                    debug(ip, '- failed to add host', command.host, 'to replica:', err);
                    ws.send('{"success":false}');
                    return;
                }
                debug(ip, '- successfully added', hostAdded);
                //make sure the socket is still open before trying to respond
                if (ws.readyState !== WebSocket.OPEN) {
                    return;
                }
                ws.send(JSON.stringify({added: hostAdded, success: true}));
                allClients.push(ws);
            });
            break;
        default:
            debug(ip, '- unknown command received');
            ws.send('{"success":false}');
            break;
    }
}

function advertiseToSkyAPI() {
    var skyapiAddr = flags.get('skyapi');
    if (!skyapiAddr) {
        return;
    }
    if (skyapiAddr.indexOf('://') === -1) {
        skyapiAddr = 'ws://' + skyapiAddr;
    }
    if (skyapiAddr.indexOf('/provide') === -1) {
        skyapiAddr += '/provide';
    }
    var provider = new SkyProvider(skyapiAddr);
    provider.provideService('mongodb-replica-maintainer', flags.get('port'));
}

var options = {},
    serverOptions = {
        autoReconnect: true,
        connectTimeoutMS: 5000
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
    socketOptions: serverOptions
};
MongoClient.connect(flags.get('mongo'), options, function(err, db) {
    if (err) {
        debug('Error connecting to mongo:', err);
        throw err;
    }
    var localDB = db.db('local');
    debug('mongoclient connected to', flags.get('mongo'));

    var server = new WebSocketServer({port: flags.get('port'), host: flags.get('ip')}, advertiseToSkyAPI);
    server.on('connection', function(ws) {
        var ip = ws._socket.remoteAddress;
        debug(ip, '- connect');

        ws.on('message', function(message) {
            debug(ip,'- message', message);
            try {
                handleMessage(localDB, ws, message);
            } catch (e) {
                debug(ip, '- error handling message', e);
            }
        });
        ws.on('error', function(err) {
            debug(ip, '- ws error', err);
            var i = allClients.indexOf(ws);
            if (i !== -1) {
                allClients.splice(i, 1);
            }
        });
        ws.on('close', function() {
            debug(ip, '- close');
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