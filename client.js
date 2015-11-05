var flags = require('flags'),
    WebSocket = require('ws'),
    Log = require('modulelog')('mongodb-replica-maintainer'),
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
flags.defineInteger('reconnect', 5000, 'ms before reconnecting');
flags.defineInteger('priority', 0, 'this replica\'s priority');
flags.defineInteger('votes', 0, 'this replica\'s votes');
flags.defineString('logger', 'default', 'the class to use for logging');
flags.defineString('log-level', 'info', 'the log level');
flags.parse();
server = flags.get('server');

Log.setClass(flags.get('logger'));
Log.setLevel(flags.get('log-level'));

function reconnect() {
    if (pendingReconnect || !flags.get('reconnect')) {
        return;
    }
    //if we lose the connection, try to reconnect in x seconds
    pendingReconnect = setTimeout(resolveAndConnect, Math.max(100, flags.get('reconnect')));
}

function resolveAndConnect() {
    if (server.indexOf('srv://') === 0) {
        var srv = require('srvclient'),
            hostname = server.substr(6);
        Log.debug('Resolving srv address', {hostname: hostname});
        srv.getTarget(hostname, function(err, target) {
            if (err) {
                Log.error('Failed to resolve srv hostname', {hostname: hostname, error: err});
                setTimeout(resolveAndConnect, Math.max(100, flags.get('reconnect')));
                return;
            }
            Log.debug('Resolving srv target', {target: target, hostname: hostname});
            target.resolve(function(err, addr) {
                if (err) {
                    Log.error('Failed to resolve srv target', {target: target, hostname: hostname, error: err});
                    setTimeout(resolveAndConnect, Math.max(100, flags.get('reconnect')));
                    return;
                }
                connect('ws://' + [addr, target.port].join(':'));
            });
        });
    } else {
        connect(server);
    }
}

function sendAdd(ws) {
    clearTimeout(pendingAdd);
    pendingAdd = null;
    if (ws.readyState !== WebSocket.OPEN) {
        return;
    }
    var host = flags.get('host') || undefined,
        hidden = flags.get('hidden'),
        pri = flags.get('priority'),
        votes = flags.get('votes');
    Log.debug('Sending add', {host: host, hidden: hidden, priority: pri, votes: votes});
    ws.send(JSON.stringify({
        cmd: 'add',
        host: host,
        hidden: hidden,
        priority: pri,
        votes: votes
    }));
    pendingAdd = setTimeout(function() {
        Log.error("Timed out waiting for response to add", {url: ws.url});
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
            Log.error("Timed out waiting for pong", {url: ws.url});
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

    Log.debug("Connecting to ws", {address: address});

    var ws = new WebSocket(address);
    ws.on('open', function() {
        Log.info("Connected to ws", {address: address});
        clearTimeout(pendingConnect);
        pendingConnect = null;
        //since we just connected we got a "pong"
        ws.lastPong = Date.now();
        sendAdd(ws);
    });
    ws.on('message', function(message) {
        Log.debug("Received message", {message: message});
        var result;
        try {
            result = JSON.parse(message);
        } catch (e) {
            Log.error("Received invalid json", {message: message, error: e});
            ws.close();
            return;
        }
        if (result.hasOwnProperty('added')) {
            clearTimeout(pendingAdd);
            pendingAdd = null;
            if (!result.success) {
                Log.error("Failed to add", {error: result.error});
                pendingAdd = setTimeout(function() {
                    sendAdd(ws);
                }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
            }
            Log.info("Added", {hostname: result.added});
            ourHostname = result.added;
            //since we just received a message we got a "pong"
            ws.lastPong = Date.now();
            startPing(ws);
            return;
        }
        if (result.hasOwnProperty('removed') && result.removed === ourHostname && pendingAdd === null) {
            Log.error("Removed from replica set", {hostname: result.removed});
            pendingAdd = setTimeout(function() {
                sendAdd(ws);
            }, (Math.random() * 5000) + 10000); //randomly between 10-15 seconds
        }
    });
    ws.on('error', function(err) {
        Log.error("Error from ws", {error: err});
        clearInterval(pingInterval);
        pingInterval = null;
        clearInterval(pendingAdd);
        pendingAdd = null;
        reconnect();
    });
    ws.on('pong', function() {
        Log.debug("Received pong");
        ws.lastPong = Date.now();
    });
    ws.on('close', function() {
        Log.error("Disconnected from ws", {address: address});
        clearInterval(pingInterval);
        pingInterval = null;
        clearInterval(pendingAdd);
        pendingAdd = null;
        reconnect();
    });

    pendingConnect = setTimeout(function() {
        Log.error("Timed out waiting to connect to ws", {address: address});
        ws.terminate();
        reconnect();
    }, 15000);
}

resolveAndConnect();
