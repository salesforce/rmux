//Copyright (c) 2013, Salesforce.com, Inc.
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
//	Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//	Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//	Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package rmux

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"
)

func TestCountActiveConnections(test *testing.T) {
	server, err := NewRedisMultiplexer("unix", "/tmp/rmuxTest.sock", 5)
	if err != nil {
		test.Fatal("Cannot listen on /tmp/rmuxTest.sock: ", err)
	}
	defer func() {
		server.Listener.Close()
	}()
	server.EndpointConnectTimeout = 1 * time.Millisecond
	server.EndpointReadTimeout = 1 * time.Millisecond

	//create a pong-responder on ports 6378 and 6379
	server.AddConnection("unix", "/tmp/rmuxTest1.sock")
	server.AddConnection("unix", "/tmp/rmuxTest2.sock")
	server.AddConnection("unix", "/tmp/rmuxTest3.sock")

	listenSock, err := net.Listen("unix", "/tmp/rmuxTest1.sock")
	if err != nil {
		test.Fatal("Cannot listen on /tmp/rmuxTest1.sock: ", err)
	}
	defer func() {
		listenSock.Close()
	}()

	connectionCount := server.countActiveConnections()

	if connectionCount != 0 {
		test.Fatal("Server thinks there are active connections, when there are none")
	}

	connection := server.ConnectionCluster[0].GetConnection()
	connection.Reader = bufio.NewReader(bytes.NewBufferString("+PONG\r\n"))
	server.ConnectionCluster[0].RecycleRemoteConnection(connection)

	listenSock2, err := net.Listen("unix", "/tmp/rmuxTest2.sock")
	if err != nil {
		test.Fatal("Cannot listen on /tmp/rmuxTest2.sock: ", err)
	}
	defer func() {
		listenSock2.Close()
	}()
	connection = server.ConnectionCluster[1].GetConnection()
	connection.Reader = bufio.NewReader(bytes.NewBufferString("+PONG\r\n"))
	server.ConnectionCluster[1].RecycleRemoteConnection(connection)

	connectionCount = server.countActiveConnections()
	if connectionCount != 2 {
		test.Fatal("Server's connection count is wrong: ", connectionCount, "instead of 2")
	}
}
