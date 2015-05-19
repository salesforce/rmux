package main

import (
	"reflect"
	"testing"
)

var json1 = []byte(`
[
	{
		"socket": "/tmp/rmux-redis1.sock",
		"tcpConnections": [ "localhost:8001" ]
	},
	{
		"socket": "/tmp/rmux-redis2.sock",
		"tcpConnections": [ "localhost:8002" ]
	}
]
`)

func TestParseConfigJson_Json1(test *testing.T) {
	config, err := ParseConfigJson(json1)
	if err != nil {
		test.Fatalf("Should not have errored parsing json1")
	}

	expects := []PoolConfig{{
		Socket:         "/tmp/rmux-redis1.sock",
		TcpConnections: []string{"localhost:8001"},
	}, {
		Socket:         "/tmp/rmux-redis2.sock",
		TcpConnections: []string{"localhost:8002"},
	}}

	if !reflect.DeepEqual(expects, config) {
		test.Errorf("Did not parse configuration string as expected")
	}
}

var json2 = []byte(`
[{
	"host": "localhost",
	"port": 10001,
	"maxProcesses": 2,
	"poolSize": 30,

	"tcpConnections": [ "localhost:8001", "localhost:8002" ],

	"localTimeout": 30,
	"localReadTimeout": 35,
	"localWriteTimeout": 40,

	"remoteTimeout": 45,
	"remoteReadTimeout": 50,
	"remoteWriteTimeout": 55,
	"remoteConnectTimeout": 60
}]
`)

func TestParseConfigJson_Json2(test *testing.T) {
	config, err := ParseConfigJson(json2)
	if err != nil {
		test.Fatalf("Should not have errored parsing json2")
	}

	expects := []PoolConfig{{
		Host:         "localhost",
		Port:         10001,
		MaxProcesses: 2,
		PoolSize:     30,

		TcpConnections: []string{"localhost:8001", "localhost:8002"},

		LocalTimeout:      30,
		LocalReadTimeout:  35,
		LocalWriteTimeout: 40,

		RemoteTimeout:        45,
		RemoteReadTimeout:    50,
		RemoteWriteTimeout:   55,
		RemoteConnectTimeout: 60,
	}}

	if !reflect.DeepEqual(expects, config) {
		test.Errorf("Did not parse configuration string as expected")
	}
}

var json3 = []byte(`
[{
	"host": "localhost",
	"port": 10001,

	"tcpConnections": "localhost:8001"
}]
`)

func TestParseConfigJson_Json3_Error(test *testing.T) {
	_, err := ParseConfigJson(json3)
	test.Logf("%s", err)
	if err == nil {
		test.Fatalf("Should have errored attempting to parse json3")
	}
}
