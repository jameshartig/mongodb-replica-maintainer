var util = require('util'),
    flags = require('flags'),
    WebSocket = require('ws'),
    debug = util.debuglog('mongodb-replica-maintainer'),
    pendingReconnect = null,
    server = '';

flags.defineString('server', 'ws://10.0.0.2:27018', 'maintainer server endpoint');
flags.defineString('host', '', 'mongodb host string or "" to rely on server');
flags.parse();
server = flags.get('server');

function reconnect() {
    if (pendingReconnect) {
        return;
    }
    //if we lose the connection, try to reconnect in 5 seconds
    pendingReconnect = setTimeout(connect, 5000);
}

function connect() {
    pendingReconnect = null;
    debug(server, '- connecting...');

    var ws = new WebSocket(server);
    ws.on('open', function() {
        debug(server, '- connected');
        debug('adding', flags.get('host'));

        ws.send(JSON.stringify({
            cmd: 'add',
            host: flags.get('host') || undefined
        }));
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
