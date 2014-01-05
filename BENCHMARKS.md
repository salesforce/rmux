# Benchmarks #

Benchmarks with keep-alive off show a unix-socket rmux connection being ~10x as fast as a direct tcp connection, under heavy load:
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
```
versus
```
redis-benchmark -q -n 10000 -c 50 -r 50 -k 0 -s /tmp/rmux.sock 
WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests
PING_INLINE: 28169.02 requests per second
PING_BULK: 23364.49 requests per second
SET: 24875.62 requests per second
GET: 25062.66 requests per second
INCR: 23696.68 requests per second
LPUSH: 26178.01 requests per second
LPOP: 27247.96 requests per second
SADD: 28328.61 requests per second
SPOP: 25906.73 requests per second
LPUSH (needed to benchmark LRANGE): 24813.90 requests per second
LRANGE_100 (first 100 elements): 14970.06 requests per second
LRANGE_300 (first 300 elements): 8857.40 requests per second
LRANGE_500 (first 450 elements): 6570.30 requests per second
LRANGE_600 (first 600 elements): 4990.02 requests per second
MSET (10 keys): 26178.01 requests per second
```
====
Benchmarks with keep-alive on show a unix-socket rmux connection being ~70% as fast as a direct tcp connection, under heavy load
```
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
versus
```
$ redis-benchmark -q -n 1000 -c 50 -r 50 -s /tmp/rmux.sock 
PING_INLINE: 156250.00 requests per second
PING_BULK: 158730.16 requests per second
SET: 68965.52 requests per second
GET: 69930.07 requests per second
INCR: 70422.53 requests per second
LPUSH: 71942.45 requests per second
LPOP: 71428.57 requests per second
SADD: 70422.53 requests per second
SPOP: 75757.58 requests per second
LPUSH (needed to benchmark LRANGE): 72992.70 requests per second
LRANGE_100 (first 100 elements): 24390.24 requests per second
LRANGE_300 (first 300 elements): 11709.60 requests per second
LRANGE_500 (first 450 elements): 8382.23 requests per second
LRANGE_600 (first 600 elements): 6269.59 requests per second
MSET (10 keys): 114942.53 requests per second
```
