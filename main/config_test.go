/*
 * Copyright (c) 2015, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
},
{
	"host": "localhost",
	"port": 10001,
	"maxProcesses": 2,
	"poolSize": 30,
	"failover": true,

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
		Failover:     false,

		TcpConnections: []string{"localhost:8001", "localhost:8002"},

		LocalTimeout:      30,
		LocalReadTimeout:  35,
		LocalWriteTimeout: 40,

		RemoteTimeout:        45,
		RemoteReadTimeout:    50,
		RemoteWriteTimeout:   55,
		RemoteConnectTimeout: 60,
	}, {
		Host:         "localhost",
		Port:         10001,
		MaxProcesses: 2,
		PoolSize:     30,
		Failover:     true,

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
	if err == nil {
		test.Fatalf("Should have errored attempting to parse json3")
	}
}
