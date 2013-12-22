rmux
====

A Redis Connection Pooler and Multiplexer, written in Go

Example usage for package main:
```
./main -socket=/tmp/rmux.sock -tcpConnections="localhost:6379 localhost:6380 localhost:6381 localhost: 6382"
```
Alternatively, if you want to make your own main package, the below code will listen on unix socket "/tmp/rmux.sock"
```
package main

import (
	"github.com/forcedotcom/rmux"
)

func main() {
	rmuxInstance, err := rmux.NewRedisMultiplexer("unix", "/tmp/rmux.sock", 50)
	rmuxInstance.AddConnection("tcp", "localhost:6379")
	rmuxInstance.AddConnection("tcp", "localhost:6380")
	rmuxInstance.AddConnection("tcp", "localhost:6381")
	rmuxInstance.AddConnection("tcp", "localhost:6382")
	rmuxInstance.PrimaryConnectionKey = "localhost:6379"
	if err != nil {
		println("Rmux Initialization Error", err.Error())
		return
	}
	
	rmuxInstance.Start()
}
```
With either of these, all Key-based commands will hash over ports 6379->6382
Non key-based commands will failover to the default connection, which is localhost:6379


Select will always return +OK, even if the server id is invalid
Ping will always return +PONG
Quit will always return +OK
Del will only accept one argument, if multiplexing is enabled


The following redis commands are disabled, because they should generally be run on the actual redis server that you want information from:
```
bgrewriteaof
bgsave
client
config
dbsize
debug
flushall
flushdb
lastsave
move
monitor
migrate
object
randomkey
save
shutdown
slaveof
slowlog
sync
time
```

The following redis commands are disabled if multiplexing is enabled:
```
multi
watch
exec
unwatch
discard
eval
bitop
brpoplpush
keys
mget
mset
msetnx
rename
renamenx
rpoplpush
script
sdiff
sdiffstore
sinter
sinterstore
sinter
smove
sunion
sunionstore
zinterstore
zunionstore
```

PubSub support is currently experimental, and only publish and subscribe are supported.
Disabled:
```
psubscribe
pubsub
punsubscribe
unsubscribe
```

Benchmarks with keep-alive off show rmux being ~4.5x as fast as a direct connection:
```
$ redis-benchmark -q -n 1000 -c 50 -r 50 -k 0 
WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests
PING_INLINE: 7633.59 requests per second
PING_BULK: 5025.13 requests per second
SET: 4032.26 requests per second
GET: 2770.08 requests per second
INCR: 2652.52 requests per second
LPUSH: 2906.98 requests per second
LPOP: 2409.64 requests per second
SADD: 1381.22 requests per second
SPOP: 1126.13 requests per second
LPUSH (needed to benchmark LRANGE): 2645.50 requests per second
LRANGE_100 (first 100 elements): 2808.99 requests per second
LRANGE_300 (first 300 elements): 1510.57 requests per second
LRANGE_500 (first 450 elements): 1515.15 requests per second
LRANGE_600 (first 600 elements): 1483.68 requests per second
MSET (10 keys): 2801.12 requests per second

versus

$ redis-benchmark -q -n 1000 -c 50 -r 50 -k 0 -s /tmp/rmux.sock 
WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests
PING_INLINE: 21276.60 requests per second
PING_BULK: 25000.00 requests per second
SET: 18181.82 requests per second
GET: 16666.67 requests per second
INCR: 17241.38 requests per second
LPUSH: 17543.86 requests per second
LPOP: 17241.38 requests per second
SADD: 16949.15 requests per second
SPOP: 16949.15 requests per second
LPUSH (needed to benchmark LRANGE): 16949.15 requests per second
LRANGE_100 (first 100 elements): 8771.93 requests per second
LRANGE_300 (first 300 elements): 4504.50 requests per second
LRANGE_500 (first 450 elements): 3086.42 requests per second
LRANGE_600 (first 600 elements): 2192.98 requests per second
MSET (10 keys): 19607.84 requests per second
```
Benchmarks with keep-alive on show a direct connection to a redis server server being ~2.2x as fast:
```
$ redis-benchmark -q -n 1000 -c 50 -r 50 -s /tmp/rmux.sock 
PING_INLINE: 124999.99 requests per second
PING_BULK: 111111.12 requests per second
SET: 40000.00 requests per second
GET: 50000.00 requests per second
INCR: 47619.05 requests per second
LPUSH: 38461.54 requests per second
LPOP: 50000.00 requests per second
SADD: 41666.67 requests per second
SPOP: 50000.00 requests per second
LPUSH (needed to benchmark LRANGE): 38461.54 requests per second
LRANGE_100 (first 100 elements): 11764.71 requests per second
LRANGE_300 (first 300 elements): 4830.92 requests per second
LRANGE_500 (first 450 elements): 3205.13 requests per second
LRANGE_600 (first 600 elements): 2551.02 requests per second
MSET (10 keys): 66666.67 requests per second

versus

$ redis-benchmark -q -n 1000 -c 50 -r 50
PING_INLINE: 100000.00 requests per second
PING_BULK: 111111.12 requests per second
SET: 90909.09 requests per second
GET: 111111.12 requests per second
INCR: 100000.00 requests per second
LPUSH: 90909.09 requests per second
LPOP: 111111.12 requests per second
SADD: 111111.12 requests per second
SPOP: 90909.09 requests per second
LPUSH (needed to benchmark LRANGE): 111111.12 requests per second
LRANGE_100 (first 100 elements): 31250.00 requests per second
LRANGE_300 (first 300 elements): 12987.01 requests per second
LRANGE_500 (first 450 elements): 8928.57 requests per second
LRANGE_600 (first 600 elements): 6849.31 requests per second
MSET (10 keys): 58823.53 requests per second
```
