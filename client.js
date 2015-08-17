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

flags.defineString('server', '', 'maintainer server endpoint');
flags.defineString('host', '', 'mongodb host string or "" to rely on server');
flags.defineBoolean('hidden', false, 'set the replica hidden option');
flags.defineInteger('ping', 15000, 'ms between pings (2 pings means failed)');
flags.defineInteger('reconnect', 30000, 'ms before reconnecting');
flags.defineInteger('priority', 0, 'this replica\'s priority');
flags.defineInteger('votes', 0, 'this replica\'s votes');
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
        hidden: flags.get('hidden'),
        priority: flags.get('priority'),
        votes: flags.get('votes')
    }));
    pendingAdd = setTimeout(function() {
        debug(ws.url, '- timed out waiting for response');
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
            debug(ws.url, '- timeout waiting for pongs');
            ws.close();
        }
    }, interval);
}

function connect(address) {
    clearTimeout(pendingReconnect);
    pendingReconnect = null;
    clearTimeout(pendingConnect);
    pendingConnect = null;

    if (address.indexOf('://') === -1) {
        address = 'ws://' + address;
    }

    debug(address, '- connecting...');

    var ws = new WebSocket(address);
    ws.on('open', function() {
        debug(address, '- connected');
        clearTimeout(pendingConnect);
        pendingConnect = null;
        //since we just connected we got a "pong"
        ws.lastPong = Date.now();
        sendAdd(ws);
    });
    ws.on('message', function(message) {
        debug(address, '- message:', message);
        var result;
        try {
            result = JSON.parse(message);
        } catch (e) {
            debug(address, '- invalid json:', e);
            ws.close();
            return;
        }
        if (result.hasOwnProperty('added')) {
            clearTimeout(pendingAdd);
            pendingAdd = null;
            if (!result.success) {
                debug(address, '- failed to add:', message);
                pendingAdd = setTimeout(function() {
                    sendAdd(ws);
                }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
            }
            debug(address, '- successfully added!');
            ourHostname = result.added;
            //since we just received a message we got a "pong"
            ws.lastPong = Date.now();
            startPing(ws);
            return;
        }
        if (result.hasOwnProperty('removed') && result.removed === ourHostname && pendingAdd === null) {
            debug(address, '- removed us from replica set!');
            pendingAdd = setTimeout(function() {
                sendAdd(ws);
            }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
        }
    });
    ws.on('error', function(err) {
        debug(address, '- error:', err);
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
        debug(address, '- disconnected');
        clearInterval(pingInterval);
        pingInterval = null;
        clearInterval(pendingAdd);
        pendingAdd = null;
        reconnect();
    });

    pendingConnect = setTimeout(function() {
        debug(address, '- timed out waiting 15 seconds to connect');
        ws.terminate();
        reconnect();
    }, 15000);
}

if (server.indexOf('srv://') === 0) {
    var srv = require('srvclient');
    srv.getTarget(server.substr(6), function(err, target) {
        if (err) {
            throw err;
        }
        target.resolve(function(err, addr) {
            if (err) {
                throw err;
            }
            connect('ws://' + [addr, target.port].join(':'));
        });
    });
} else {
    connect(server);
}
