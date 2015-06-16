var util = require('util'),
    flags = require('flags'),
    WebSocket = require('ws'),
    debug = util.debuglog('mongodb-replica-maintainer'),
    pendingConnect = null,
    pendingReconnect = null,
    pendingAdd = null,
    pingInterval = null,
    server = '',
    ourHostname = '';

flags.defineString('server', 'ws://10.0.0.2:27018', 'maintainer server endpoint');
flags.defineString('host', '', 'mongodb host string or "" to rely on server');
flags.defineBoolean('hidden', false, 'set the replica hidden option');
flags.defineInteger('ping', 15000, 'ms between pings (2 pings means failed)');
flags.defineInteger('reconnect', 30000, 'ms before reconnecting');
flags.parse();
server = flags.get('server');

function reconnect() {
    if (pendingReconnect || !flags.get('reconnect')) {
        return;
    }
    //if we lose the connection, try to reconnect in x seconds
    pendingReconnect = setTimeout(connect, Math.max(100, flags.get('reconnect')));
}

function sendAdd(ws) {
    clearTimeout(pendingAdd);
    pendingAdd = null;
    if (ws.readyState !== WebSocket.OPEN) {
        return;
    }
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

function startPing(ws) {
    clearInterval(pingInterval);
    if (!flags.get('ping')) {
        return;
    }
    var interval = Math.max(1000, flags.get('ping'));
    pingInterval = setInterval(function() {
        if (ws.readyState !== WebSocket.OPEN) {
            return;
        }
        ws.ping();
        if (Date.now() - ws.lastPong >= (2 * interval)) {
            debug(server, '- timeout waiting for pongs');
            ws.close();
        }
    }, interval);
}

function connect() {
    clearTimeout(pendingReconnect);
    pendingReconnect = null;
    clearTimeout(pendingConnect);
    pendingConnect = null;
    debug(server, '- connecting...');

    var ws = new WebSocket(server);
    ws.on('open', function() {
        debug(server, '- connected');
        clearTimeout(pendingConnect);
        pendingConnect = null;
        //since we just connected we got a "pong"
        ws.lastPong = Date.now();
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
        //todo: remove !result.hasOwnProperty('removed') once we don't care about backwards-compatiblity
        if (result.hasOwnProperty('added') || !result.hasOwnProperty('removed')) {
            clearTimeout(pendingAdd);
            pendingAdd = null;
            if (!result.success) {
                debug(server, '- failed to add:', message);
                pendingAdd = setTimeout(function() {
                    sendAdd(ws);
                }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
            }
            debug(server, '- successfully added!');
            ourHostname = result.host;
            //since we just received a message we got a "pong"
            ws.lastPong = Date.now();
            startPing(ws);
            return;
        }
        if (result.hasOwnProperty('removed') && result.removed === ourHostname && pendingAdd === null) {
            debug(server, '- removed us from replica set!');
            pendingAdd = setTimeout(function() {
                sendAdd(ws);
            }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
        }
    });
    ws.on('error', function(err) {
        debug(server, '- error:', err);
        clearInterval(pingInterval);
        pingInterval = null;
        clearInterval(pendingAdd);
        pendingAdd = null;
        reconnect();
    });
    ws.on('pong', function() {
        ws.lastPong = Date.now();
    });
    ws.on('close', function() {
        debug(server, '- disconnected');
        clearInterval(pingInterval);
        pingInterval = null;
        clearInterval(pendingAdd);
        pendingAdd = null;
        reconnect();
    });

    pendingConnect = setTimeout(function() {
        debug(server, '- timed out waiting 15 seconds to connect');
        ws.terminate();
        reconnect();
    }, 15000);
}
connect();
