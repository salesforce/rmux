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

package connection

import (
	"bufio"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"testing"
	"time"
)

func TestRecycleConnection(test *testing.T) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()

	//Setting the channel at size 2 makes this more interesting
	connectionPool := NewConnectionPool("unix", testSocket, 2)

	connection := connectionPool.GetConnection()
	if connection == nil {
		test.Fatal("Failed to get first connection")
	}

	connection2 := connectionPool.GetConnection()
	if connection2 == nil {
		test.Fatal("Failed to get second connection")
	}

	listenSock.Close()
	connectionPool.RecycleRemoteConnection(connection)
	connectionPool.RecycleRemoteConnection(connection2)
	test.Log("Listen socket was closed.  New connections should fail, but our current recycled connection count is 2")
	connection = connectionPool.GetConnection()
	if connection == nil {
		test.Fatal("Failed to get first connection")
	}

	connection = connectionPool.GetConnection()
	if connection == nil {
		test.Fatal("Failed to get second connection")
	}

	connection2 = connectionPool.GetConnection()
	if connection2 == nil {
		test.Log("Failed to get a new connection on a non-listened socket")
	} else {
		test.Fatal("Somehow, we got a new connection on a non-listened socket")
	}

	connectionPool.RecycleRemoteConnection(connection)
	test.Log("Recycled a good connection one more time, to verify cycling")

	connection = connectionPool.GetConnection()
	if connection == nil {
		test.Fatal("Failed to get recycled connection")
	} else {
		test.Log("Successfully retrieved recycled connection")
	}
}

func TestCheckConnectionState(test *testing.T) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()

	//Setting the channel at size 2 makes this more interesting
	connectionPool := NewConnectionPool("unix", testSocket, 2)
	connectionPool.ConnectTimeout = time.Millisecond * 10
	connectionPool.ReadTimeout = time.Millisecond * 10
	connectionPool.WriteTimeout = time.Millisecond * 10
	temporaryConnection := connectionPool.GetConnection()

	fd, err := listenSock.Accept()
	if err != nil {
		test.Fatal("Failed to accept connection")
	} else {
		localBuffer := bufio.NewReadWriter(bufio.NewReader(fd), bufio.NewWriter(fd))
		test.Log("Shoving a single +PONG response in the buffer for testing")
		protocol.FlushLine(protocol.PONG_RESPONSE, localBuffer.Writer)
	}

	temporaryConnection2 := connectionPool.GetConnection()
	test.Log("The first connection (out of two) in the channel now has a valid pong response ready")
	connectionPool.RecycleRemoteConnection(temporaryConnection)
	connectionPool.RecycleRemoteConnection(temporaryConnection2)

	if connectionPool.CheckConnectionState() {
		test.Log("Valid connection's check connection succeeded")
	} else {
		test.Fatal("Valid connection's checkheck connection failed")
	}

	if connectionPool.CheckConnectionState() {
		test.Fatal("In-valid connection's checkheck connection succeeded")
	} else {
		test.Log("In-valid connection's checkheck connection failed")
	}

	test.Log("Closed the listen socket, so future connections can only come through the (hopefully empty) channel")
	listenSock.Close()

	temporaryConnection = connectionPool.GetConnection()
	if temporaryConnection == nil {
		test.Log("Connection channel was flushed, upon invalid connection check")
	} else {
		test.Fatal("Connection channel was not flushed, upon invalid connection check")
	}
}
