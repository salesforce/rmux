[![Travis CI](https://travis-ci.org/dvankley/rmux.svg?branch=master)](https://travis-ci.org/dvankley/rmux)
# Rmux #

Rmux is a Redis connection pooler and multiplexer, written in Go.  Rmux is meant to be used for LAMP stacks, or other short-lived process applications, with high request volume.  It should be run as a client, on every server that connects to redis--to reduce the total inbound connection count to the redis servers, while handle consistent multiplexing.

## Motivation ##

At Pardot, we use redis (among other things) for our cache layer.  Early on, we saw occasional latency spikes.  After tuning our redis servers' net.ipv4.tcp.. settings , everything settled down--but as we grew, we began to see issues pop up again.

While our Memory usage remained remarkably low, we saw occasional CPU spikes during peak access times.  Adding more redis boxes, with key-based hashing in our application, surprisingly did not help.  Pardot application severs run on a LAMP stack, which means that each request has to create its own connection to Redis.  Since each application request hits multiple cache keys, destination redis boxes were receiving the same number of connections, but less commands.

Since the issue seemed to be purely connection rates, and not command count, we started looking for a connection pooler.  After finding none that were designed for redis, we built our own.  Along the way, we built in key-based multiplexing, with a failover strategy in place.

With rmux, our application servers all connect to a local unix socket, instead of the target destination redis port.  Rmux then parses the incomming request, reads the first key, and hashes it to find which server to execute the command on.  If a server is down, the command will instead be sent to a backup-hashed server.  Since rmux understands the redis protocol, it also handles connection pooling//recycling for you, and handles server id management for the connections.

When rmux hit production, we saw immediate gains in our 90th-percentile and upper-bound response times.

## Installing ##

- Install [Go](http://golang.org/doc/install) 
- go get -u github.com/dvankley/rmux
- go build -o /usr/local/bin/rmux github.com/dvankley/rmux/main


## Usage ##

```
Usage of rmux:
  -host="localhost": The host to listen for incoming connections on
  -localReadTimeout=0: Timeout to set locally (read)
  -localTimeout=0: Timeout to set locally (read+write)
  -localWriteTimeout=0: Timeout to set locally (write)
  -maxProcesses=0: The number of processes to use.  If this is not defined, go's default is used.
  -poolSize=50: The size of the connection pools to use
  -port="6379": The port to listen for incoming connections on
  -remoteConnectTimeout=0: Timeout to set for remote redises (connect)
  -remoteReadTimeout=0: Timeout to set for remote redises (read)
  -remoteTimeout=0: Timeout to set for remote redises (connect+read+write)
  -remoteWriteTimeout=0: Timeout to set for remote redises (write)
  -socket="": The socket to listen for incoming connections on.  If this is provided, host and port are ignored
  -tcpConnections="localhost:6380 localhost:6381": TCP connections (destination redis servers) to multiplex over
  -unixConnections="": Unix connections (destination redis servers) to multiplex over
  -config="": Path to configuration file
```

For more details about rmux configuration see [Configuration](doc/config.md)

Localhost example:
```
redis-server --port 6379 &
redis-server --port 6380 &
redis-server --port 6381 &
redis-server --port 6382 &
rmux -socket=/tmp/rmux.sock -tcpConnections="localhost:6379 localhost:6380 localhost:6381 localhost:6382" &
redis-cli -s /tmp/rmux.sock
```

- In the above example, all key-based commands will hash over ports 6379->6382 on localhost
- If the server that a key hashes to is down, a backup server is automatically used (hashed based over the servers that are currently up)
- All servers running production code should be running the same version (and destination flags) of rmux, and should be connecting over the rmux socket
- Select will always return +OK, even if the server id is invalid
- Ping will always return +PONG
- Quit will always return +OK
- Info will return an abbreviated response:

```
rmux_version: 1.0
go_version: go1.1.2
process_id: 48885
connected_clients: 0
active_endpoints: 4
total_endpoints: 4
role: master
```

Production equivalent:
```
rmux -socket=/tmp/rmux.sock -tcpConnections="redis1:6379 redis1:6380 redis2:6379 redis2:6380"
```

### Disabled commands ###

Redis commands that should only be run directly on a redis server are disabled.  Commands that operate on more than one key (or have the potential to) are disabled if multiplexing is enabled.

PubSub support is currently experimental, and only publish and subscribe are supported.
Disabled:
```
psubscribe
pubsub
punsubscribe
unsubscribe
```

[Full list of disabled commands](DISABLED_COMMANDS.md)

### Benchmarks ###

Benchmarks with keep-alive off (simulating a lamp stack) show rmux being ~10x as fast as a direct connection, under heavy load.

Benchmarks with keep-alive on (simulating how a java server would operate) show rmux being ~70% as fast as a direct connection.

[Benchmark results here](BENCHMARKS.md)

Rmux is currently used in production by Pardot.  We have seen a reduction in our upper and 90th percentile connection and command times.  The 90th percentile times are slightly improved, and the upper times are drastically improved.

[Production graphite data is here](PRODUCTION_BENCHMARKS.md)
