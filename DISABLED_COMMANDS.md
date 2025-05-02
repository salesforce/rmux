### Disabled commands ###

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

The following redis commands are disabled if multiplexing is enabled, because they have the potential to operate on multiple keys:
```
discard
eval
exec
bitop
brpoplpush
keys
mget
mset
msetnx
multi
rename
renamenx
rpoplpush
sdiff
sdiffstore
sinter
sinterstore
sinter
smove
sunion
sunionstore
unwatch
watch
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
