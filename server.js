var util = require('util'),
    flags = require('flags'),
    MongoClient = require('mongodb').MongoClient,
    WebSocketServer = require('ws').Server,
    debug = util.debuglog('mongodb-replica-maintainer');

flags.defineString('ip', '0.0.0.0', 'listen ip');
flags.defineInteger('port', 27018, 'listen port');
flags.defineString('mongo', 'mongodb://127.0.0.1:27017', 'mongodb connect string');
flags.parse();

function addNewReplicaMember(db, host, cb) {
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
        var found = false,
            highestID = 0,
            i = 0;
        for (; i < doc.members.length; i++) {
            if (doc.members[i].host === host) {
                found = true;
                break;
            }
            highestID = Math.max(highestID, doc.members[i]._id);
        }
        if (!found) {
            highestID++;
            debug('adding', host, 'to replica set with id', highestID);
            doc.version++;
            //priority 0 means it will never be a primary
            doc.members.push({_id: highestID, host: host, priority: 0, votes: 0});
            db.admin().command({replSetReconfig: doc}, function(err, res) {
                if (err) {
                    cb(err);
                    return;
                }
                debug('got result', res);
                cb(null);
            })
        }
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
        return;
    }
    switch (command.cmd) {
        case 'add':
            //todo: validate the host
            if (!command.host) {
                debug(ip, '- client didnt send host, assuming ip');
                command.host = ws.address;
            }
            addNewReplicaMember(db, command.host, function(err, doc) {
                if (err) {
                    debug(ip, '- failed to add host', command.host, 'to replica:', err);
                    return;
                }
                debug(ip, '- successfully added', command.host);
                //todo: return success
            });
            break;
        default:
            debug(ip, '- unknown command received');
            break;
    }
}

MongoClient.connect(flags.get('mongo'), function(err, db) {
    if (err) {
        throw err;
    }
    debug('mongoclient connected to', flags.get('mongo'));

    var server = new WebSocketServer({port: flags.get('port'), host: flags.get('ip')});
    server.on('connection', function(ws) {
        var ip = ws._socket.remoteAddress;
        debug(ip, '- connect');

        ws.on('message', function(message) {
            debug(ip,'- message', message);
            handleMessage(db, ws, message);
        });
        ws.on('error', function(err) {
            debug(ip, '- ws error', err);
        });
        ws.on('close', function() {
            debug(ip, '- close');
        });
    });
});