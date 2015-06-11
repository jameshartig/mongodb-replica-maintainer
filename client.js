var util = require('util'),
    flags = require('flags'),
    WebSocket = require('ws'),
    debug = util.debuglog('mongodb-replica-maintainer'),
    pendingReconnect = null;

flags.defineString('server', 'ws://10.0.0.2:27018', 'maintainer server endpoint');
flags.defineString('host', '10.0.0.1:27017', 'mongodb host string');
flags.parse();

function connect() {
    pendingReconnect = null;

    var ws = new WebSocket(flags.get('server'));
    ws.on('open', function() {
        ws.send(JSON.stringify({
            cmd: 'add',
            host: flags.get('host')
        }));
    });
    ws.on('close', function() {
        if (pendingReconnect) {
            return;
        }
        //if we lose the connection, try to reconnect in 5 seconds
        pendingReconnect = setTimeout(connect, 5000);
    });
}
connect();
