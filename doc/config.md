## Configuration

Configuration can be handled either via command-line arguments or via config file.

### Command-line arguments
```
  -host="localhost": The host to listen for incoming connections on
  -localReadTimeout=0: Timeout to set locally (read)
  -localTimeout=0: Timeout to set locally (read+write)
  -localTransactionTimeout=0: Timeout to set locally (transaction)
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

### Configuration file
`rmux` accepts a `-config=/path/to/file.json` argument that specifies a path to a json configuration file. The format
for the configuration json is as follows:
```
[
  {
    "host": string,
    "port": int,
    "socket": string,
    "maxProcesses": int,
    "poolSize": int,
    "tcpConnections": [string, string, ...],
    "unixConnections": [string, string, ...],

    "localTimeout": int,
    "localReadTimeout": int,
    "localWriteTimeout": int,
    "localTransactionTimeout": int,

    "remoteTimeout": int,
    "remoteReadTimeout": int,
    "remoteWriteTimeout": int,
    "remoteConnectTimeout": int
  },
  ...
]
```

`[host, port]` or `socket` is required, as is at least one of `tcpConnections` or `unixConnections`. Using the configuration file
you are capable of specifying and creating multiple rmux pools.
