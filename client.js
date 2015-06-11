var util = require('util'),
    flags = require('flags'),
    WebSocket = require('ws'),
    debug = util.debuglog('mongodb-replica-maintainer'),
    pendingReconnect = null,
    pendingAdd = null,
    server = '';

flags.defineString('server', 'ws://10.0.0.2:27018', 'maintainer server endpoint');
flags.defineString('host', '', 'mongodb host string or "" to rely on server');
flags.defineBoolean('hidden', false, 'set the replica hidden option');
flags.parse();
server = flags.get('server');

function reconnect() {
    if (pendingReconnect) {
        return;
    }
    //if we lose the connection, try to reconnect in 5 seconds
    pendingReconnect = setTimeout(connect, 5000);
}

function sendAdd(ws) {
    clearTimeout(pendingAdd);
    debug('adding', flags.get('host'));
    ws.send(JSON.stringify({
        cmd: 'add',
        host: flags.get('host') || undefined,
        hidden: flags.get('hidden')
    }));
    pendingAdd = setTimeout(function() {
        debug(server, '- timed out waiting for response');
        sendAdd(ws);
    }, 15000);
}

function connect() {
    clearTimeout(pendingReconnect);
    pendingReconnect = null;
    debug(server, '- connecting...');

    var ws = new WebSocket(server);
    ws.on('open', function() {
        debug(server, '- connected');
        sendAdd(ws);
    });
    ws.on('message', function(message) {
        debug(server, '- message:', message);
        var result;
        try {
            result = JSON.parse(message);
        } catch (e) {
            debug(server, '- invalid json:', e);
            ws.close();
            return;
        }
        clearTimeout(pendingAdd);
        if (!result.success) {
            debug(server, '- failed to add:', message);
            pendingAdd = setTimeout(function() {
                sendAdd(ws);
            }, 5000);
        }
        debug(server, '- successfully added!');
    });
    ws.on('error', function(err) {
        debug(server, '- error:', err);
        reconnect();
    });
    ws.on('close', function() {
        debug(server, '- disconnected');
        reconnect();
    });
}
connect();
