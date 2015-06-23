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

package rmux

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func StartPongResponseServer(t *testing.T, sock string) net.Listener {
	listenSock, err := net.Listen("unix", sock)
	if err != nil {
		t.Errorf("Cannot listen on %s: %s", sock, err)
		return nil
	}

	go func() {
		for {
			c, err := listenSock.Accept()
			if err != nil {
				break
			}
			rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
			rw.ReadLine()
			rw.Write([]byte("+PONG\r\n"))
			rw.Flush()
		}
	}()

	return listenSock
}

func StartNoResponseServer(t *testing.T, sock string) net.Listener {
	listenSock, err := net.Listen("unix", sock)
	if err != nil {
		t.Errorf("Cannot listen on %s: %s", sock, err)
		return nil
	}

	go func() {
		for {
			_, err := listenSock.Accept()
			if err != nil {
				break
			}
			// do nothing
		}
	}()

	return listenSock
}

func TestCountActiveConnections_NoResponse(t *testing.T) {
	server, err := NewRedisMultiplexer("unix", "/tmp/rmuxTest.sock", 5)
	if err != nil {
		t.Fatal("Cannot listen on /tmp/rmuxTest.sock: ", err)
	}
	defer func() {
		server.active = false
		server.Listener.Close()
	}()

	server.EndpointConnectTimeout = 10 * time.Millisecond
	server.EndpointReadTimeout = 10 * time.Millisecond
	server.EndpointWriteTimeout = 10 * time.Millisecond

	server.AddConnection("unix", "/tmp/rmuxTest1.sock")
	server.AddConnection("unix", "/tmp/rmuxTest2.sock")
	server.AddConnection("unix", "/tmp/rmuxTest3.sock")

	//create a non-responder on one socket
	sock1 := StartNoResponseServer(t, "/tmp/rmuxTest1.sock")
	if sock1 == nil {
		t.Error("Cannot listen on /tmp/rmuxTest1.sock: ", err)
		return
	}
	defer sock1.Close()

	connectionCount := server.countActiveConnections()

	if connectionCount != 0 {
		t.Error("Server thinks there are active connections, when there should be none")
		return
	}
}

func TestCountActiveConnections_SomeResponses(t *testing.T) {
	server, err := NewRedisMultiplexer("unix", "/tmp/rmuxTest.sock", 5)
	if err != nil {
		t.Fatal("Cannot listen on /tmp/rmuxTest.sock: ", err)
	}
	defer func() {
		server.active = false
		server.Listener.Close()
	}()

	server.EndpointConnectTimeout = 10 * time.Millisecond
	server.EndpointReadTimeout = 10 * time.Millisecond
	server.EndpointWriteTimeout = 10 * time.Millisecond

	server.AddConnection("unix", "/tmp/rmuxTest1.sock")
	server.AddConnection("unix", "/tmp/rmuxTest2.sock")
	server.AddConnection("unix", "/tmp/rmuxTest3.sock")

	//create a non-responder on one socket
	sock1 := StartNoResponseServer(t, "/tmp/rmuxTest1.sock")
	if sock1 == nil {
		return
	}
	defer sock1.Close()

	// create a pong-responder on another socket
	sock2 := StartPongResponseServer(t, "/tmp/rmuxTest2.sock")
	if sock2 == nil {
		return
	}
	defer sock2.Close()

	// no listener on socket 3

	connectionCount := server.countActiveConnections()
	if connectionCount != 1 {
		t.Errorf("Server's connection count is wrong: %d instead of 1", connectionCount)
	}
}

func TestCountActiveConnections_AllResponses(t *testing.T) {
	server, err := NewRedisMultiplexer("unix", "/tmp/rmuxTest.sock", 5)
	if err != nil {
		t.Fatal("Cannot listen on /tmp/rmuxTest.sock: ", err)
	}
	defer func() {
		server.active = false
		server.Listener.Close()
	}()

	server.EndpointConnectTimeout = 10 * time.Millisecond
	server.EndpointReadTimeout = 10 * time.Millisecond
	server.EndpointWriteTimeout = 10 * time.Millisecond

	server.AddConnection("unix", "/tmp/rmuxTest1.sock")
	server.AddConnection("unix", "/tmp/rmuxTest2.sock")
	server.AddConnection("unix", "/tmp/rmuxTest3.sock")

	// create pong responders
	sock1 := StartPongResponseServer(t, "/tmp/rmuxTest1.sock")
	if sock1 == nil {
		return
	}
	defer sock1.Close()
	sock2 := StartPongResponseServer(t, "/tmp/rmuxTest2.sock")
	if sock2 == nil {
		return
	}
	defer sock2.Close()
	sock3 := StartPongResponseServer(t, "/tmp/rmuxTest3.sock")
	if sock3 == nil {
		return
	}
	defer sock3.Close()

	connectionCount := server.countActiveConnections()
	if connectionCount != 3 {
		t.Errorf("Server's connection count is wrong: %d instead of 1", connectionCount)
	}
}
