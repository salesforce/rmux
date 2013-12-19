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
	"github.com/forcedotcom/rmux/connection"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"testing"
	"time"
)

func TestAddRemoveClients(test *testing.T) {
	subscription := NewSubscription(2, "test")
	client1 := &Client{}
	subscription.AddClient(client1)
	client2 := &Client{}
	subscription.AddClient(client2)
	client3 := &Client{}
	subscription.AddClient(client3)

	if subscription.sliceSize == 3 {
		test.Log("Subscription client slice has 3 members")
	} else {
		test.Fatal("Subscription client slice does not have 3 members")
	}

	if cap(subscription.clients) == 5 {
		test.Log("Subscription client slice has 5 capacity")
	} else {
		test.Fatal("Subscription client slice does not have 5 capacity")
	}

	subscription.RemoveClient(client2)
	if subscription.sliceSize == 2 {
		test.Log("Subscription client slice has 2 members")
	} else {
		test.Fatal("Subscription client slice does not have 2 members")
	}

	if subscription.clients[0] == client1 {
		test.Log("Subscription's first client is still client1")
	} else {
		test.Fatal("Subscription's first client is not client1")
	}

	if subscription.clients[1] == client3 {
		test.Log("Subscription's second client is still client3")
	} else {
		test.Fatal("Subscription's second client is not client3")
	}
}

func TestUpdateConnection(test *testing.T) {
	subscription := NewSubscription(2, "test")

	testSocket := "/tmp/rmuxConnectionTest"
	//Setting the channel at size 2 makes this more interesting
	connectionPool := connection.NewConnectionPool("unix", testSocket, 2)

	subscription.UpdateConnection("testKey", connectionPool)

	if subscription.ActiveConnection == nil {
		test.Log("Subscription's connection is still nil")
	} else {
		test.Fatal("Subscription's connection is somehow not nil")
	}

	if subscription.ActiveConnectionKey == "testKey" {
		test.Fatal("Subscription's connectionKey got incorrectly set")
	} else {
		test.Log("Subscription's connectionKey did not get updated")
	}

	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()

	myConnection := connection.NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	fd, err := listenSock.Accept()
	if err != nil {
		test.Fatal("Failed to accept connection")
	} else {
		test.Log("Accepted connection on local sock")
		localBuffer := bufio.NewReadWriter(bufio.NewReader(fd), bufio.NewWriter(fd))
		test.Log("Shoving a single Subscribe success response in the buffer for testing")
		protocol.FlushLine([]byte{'*', '3', '\r', '\n', '+', '1', '\r', '\n', 'a', '\r', '\n', '+', '1', '\r', '\n', 'a', '\r', '\n', '+', '1', '\r', '\n', 'a', '\r', '\n'}, localBuffer.Writer)
	}
	connectionPool.RecycleRemoteConnection(myConnection)

	subscription.UpdateConnection("testKey", connectionPool)
	if subscription.ActiveConnection != nil {
		test.Log("Subscription's connection is no longer nil")
	} else {
		test.Fatal("Subscription's connection is somehow still nil")
	}

	if subscription.ActiveConnectionKey == "testKey" {
		test.Log("Subscription's connectionKey is correct")
	} else {
		test.Fatal("Subscription's connectionKey did not get updated")
	}

	testSocket2 := "/tmp/rmuxConnectionTest2"
	//Setting the channel at size 2 makes this more interesting
	connectionPool2 := connection.NewConnectionPool("unix", testSocket2, 2)

	subscription.UpdateConnection("testKey2", connectionPool2)
	if subscription.ActiveConnection == nil {
		test.Log("Subscription's connection is nil again, since an invalid key was supplied")
	} else {
		test.Fatal("Subscription's connection is somehow not nil")
	}

	if subscription.ActiveConnectionKey == "" {
		test.Log("Subscription's connectionKey is empty, since an invalid pool was provided")
	} else {
		test.Log("Subscription's connectionKey did not get updated")
	}
}
