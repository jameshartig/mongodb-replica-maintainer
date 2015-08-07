# mongodb-replica-maintainer #

A server and client for adding/removing dynamic mongodb replica members.

### Usage ###

Example systemd service files are included in the `systemd` folder. You'll need to modify
the `--server` argument in the client service to point to the server.

```
# Client
$ node client.js --server=ws://127.0.0.1:27018
```

```
# Server
$ node server.js
```

### SkyDNS ###

If you want to use SkyAPI/SkyDNS for the client to discover the server, then use the
`--skyapi` flag when starting the server. The server will advertise itself as
`mongodb-replica-maintainer` so for the client you would run:
```
node client.js --server=srv://mongodb-replica-maintainer.services.domain
```
where `domain` is the SkyDNS suffix you're using.

By [James Hartig](https://github.com/fastest963/)
