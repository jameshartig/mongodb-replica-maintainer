var util = require('util'),
    flags = require('flags'),
    MongoClient = require('mongodb').MongoClient,
    WebSocket = require('ws'),
    WebSocketServer = require('ws').Server,
    debug = util.debuglog('mongodb-replica-maintainer');

flags.defineString('ip', '0.0.0.0', 'listen ip');
flags.defineInteger('port', 27018, 'listen port');
flags.defineString('mongo', 'mongodb://127.0.0.1:27017', 'mongodb connect string');
flags.defineBoolean('clean', true, 'clean up dead replicas');
flags.parse();

function cleanupDeadReplicas(db) {
    var adminDB = db.admin();
    adminDB.command({replSetGetStatus: 1}, function(err, status) {
        var i = 0,
            idsToRemove = [];
        for (; i < status.members.length; i++) {
            //state of 1 means primary
            if (status.members[i].state !== 1 && !status.members[i].health && status.date - status.members[i].lastHeartbeatRecv > (3600 * 1000)) {
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

function addNewReplicaMember(db, host, hidden, cb) {
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
            doc.members.push({_id: highestID, host: host, priority: 0, votes: 0, hidden: hidden});
            changed = true;
        } else {
            if (doc.members[found].hidden !== hidden) {
                doc.members[found].hidden = hidden;
                changed = true;
            }
        }
        if (!changed) {
            cb(null, true);
            return;
        }
        doc.version++;
        db.admin().command({replSetReconfig: doc}, function(err, res) {
            if (err) {
                cb(err);
                return;
            }
            cb(null, true);
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
            addNewReplicaMember(db, command.host, command.hidden, function(err, doc) {
                if (err) {
                    debug(ip, '- failed to add host', command.host, 'to replica:', err);
                    ws.send('{"success":false}');
                    return;
                }
                debug(ip, '- successfully added', command.host);
                //make sure the socket is still open before trying to respond
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send('{"success":true}');
                }
            });
            break;
        default:
            debug(ip, '- unknown command received');
            ws.send('{"success":false}');
            break;
    }
}

MongoClient.connect(flags.get('mongo'), function(err, db) {
    if (err) {
        throw err;
    }
    var localDB = db.db('local');
    debug('mongoclient connected to', flags.get('mongo'));

    var server = new WebSocketServer({port: flags.get('port'), host: flags.get('ip')});
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
        });
        ws.on('close', function() {
            debug(ip, '- close');
        });
    });

    if (flags.get('clean')) {
        setInterval(cleanupDeadReplicas.bind(null, localDB), 60000);
    }
});