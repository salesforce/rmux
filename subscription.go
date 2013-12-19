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
	"github.com/forcedotcom/rmux/connection"
	"github.com/forcedotcom/rmux/protocol"
	"io"
	"net"
	"sync"
	"time"
)

//An individual subscription, that our server is managing
type Subscription struct {
	//list of all clients that have this subscription
	clients []*Client
	//The current active connection that we are listening on
	ActiveConnection *connection.Connection
	//The connectionKey that our activeConnection's connectionPool is using.
	ActiveConnectionKey string
	//The channelName that our subscription is listening in on
	channelName string
	//A mutex, used to ensure that we only add or remove 1 client at a time, for safety
	sliceMutex sync.Mutex
	//The current used size of our client slice
	sliceSize int
	//When a subscription runs out of connected clients, it is requested to stop
	//This tells the subscription to shut down and stop listening for new messages
	RequestStop bool
}

//Initializes a new subscription, for the given default capacity, and channel name
func NewSubscription(capacity int, channelName string) (newSubscription *Subscription) {
	newSubscription = &Subscription{}
	newSubscription.clients = make([]*Client, capacity)
	newSubscription.sliceMutex = sync.Mutex{}
	newSubscription.channelName = channelName
	newSubscription.ActiveConnectionKey = ""
	return
}

//Updates the subscription's connection, to use a new connectionKey and connectionPool
//If the connectionKey has not changed, nothing should happen
func (mySubscription *Subscription) UpdateConnection(myConnectionKey string, myConnectionPool *connection.ConnectionPool) {
	if myConnectionKey == mySubscription.ActiveConnectionKey && mySubscription.ActiveConnection != nil {
		return
	}
	mySubscription.sliceMutex.Lock()
	defer mySubscription.sliceMutex.Unlock()
	mySubscription.ActiveConnection = nil
	mySubscription.ActiveConnectionKey = ""
	connection := myConnectionPool.GetConnection()
	if connection == nil {
		protocol.Debug("Aborting, because no connection was returned")
		return
	}
	err := connection.SubscribeChannel(mySubscription.channelName)
	if err != nil {
		protocol.Debug("Subscription failed with an error: %s\r\n", err)
		return
	}
	if mySubscription.ActiveConnection != nil {
		protocol.FlushLine(protocol.QUIT_COMMAND, mySubscription.ActiveConnection.ReadWriter.Writer)
	}
	mySubscription.ActiveConnection = connection
	mySubscription.ActiveConnectionKey = myConnectionKey
	protocol.Debug("Subscription channel updated\r\n")
}

//Listens for mesasges from the ActiveConnection, that should be broadcasted out
//When a message is received, it is passed off to BroadcastMessage, where it gets sent out
func (mySubscription *Subscription) BroadcastMessages() (err error) {
	broadcastMessageBuffer := new(bytes.Buffer)
	broadcastMessageWriter := bufio.NewWriterSize(broadcastMessageBuffer, 4096)
	broadcastMessage := make([]byte, 0, 4096)
	firstLine := make([]byte, 0, 4096)
	for !mySubscription.RequestStop {
		protocol.Debug("Looping inside of subscription.broadcastMessages")
		if mySubscription.ActiveConnection == nil || mySubscription.ActiveConnectionKey == "" {
			time.Sleep(time.Second * 1)
			continue
		}
		broadcastMessageBuffer.Reset()
		firstLine, _, err = mySubscription.ActiveConnection.ReadWriter.ReadLine()
		protocol.Debug("I read %s\r\n", firstLine)
		if err != nil {
			if err == io.EOF {
				protocol.Debug("Subscription: EOF encountered, resetting our activeConnection")
				mySubscription.ActiveConnection = nil
				time.Sleep(time.Second * 1)
				continue
			}
			_, ok := err.(net.Error)
			if ok && err.(net.Error).Timeout() {
				//We had a read timeout.  Continue and try try again
				protocol.Debug("Subscription: Read error encountered, sleeping\r\n")
				time.Sleep(time.Millisecond * 100)
				continue
			}
			panic("New subscription Read Error: " + err.Error())
		}
		//If we don't have *4, this is a bad subscription message
		if firstLine[0] != '*' || firstLine[1] != '3' {
			//flag ourselves as being done.  The server should notice this and give us a fresh/working connection
			mySubscription.ActiveConnection = nil
			mySubscription.ActiveConnectionKey = ""
			protocol.Debug("Aborting this connection and establishing a new subscription, since the firstLine format is wrong: %s\r\n", firstLine)
			continue
		}

		protocol.Debug("Captured subscription message to broadcast over %s: %s\r\n", mySubscription.channelName, broadcastMessage)
		protocol.CopyMultiBulkMessage(firstLine, broadcastMessageWriter, mySubscription.ActiveConnection.Reader)
		broadcastMessage = broadcastMessageBuffer.Bytes()
		mySubscription.BroadcastMessage(broadcastMessage)

	}
	return
}

//Broadcasts an individual message to all connected clients
func (mySubscription *Subscription) BroadcastMessage(broadcastMessage []byte) {
	mySubscription.sliceMutex.Lock()
	defer mySubscription.sliceMutex.Unlock()
	protocol.Debug("Lock acquired, broadcasting message %s\r\n", broadcastMessage)
	for index, myClient := range mySubscription.clients {
		if index >= mySubscription.sliceSize {
			break
		}
		protocol.Debug("Writing message\r\n")
		myClient.ReadWriter.Write(broadcastMessage)
		protocol.Debug("Flushing message\r\n")
		myClient.ReadWriter.Flush()
		protocol.Debug("Flushed message\r\n")
	}
	protocol.Debug("Leaving broadcastMessage func\r\n")
}

//Adds a client to our clients slice.  If there is not room in the slice, the slice is double+1'd in size
func (mySubscription *Subscription) AddClient(newClient *Client) {
	mySubscription.sliceMutex.Lock()
	defer mySubscription.sliceMutex.Unlock()
	protocol.Debug("Number of subscribed clients is %d, capacity is %d\r\n", mySubscription.sliceSize, cap(mySubscription.clients))
	if mySubscription.sliceSize == cap(mySubscription.clients) {
		newSlice := make([]*Client, cap(mySubscription.clients)*2+1)
		copy(newSlice, mySubscription.clients)
		mySubscription.clients = newSlice
	}
	protocol.Debug("Subscribing client\r\n")
	mySubscription.clients[mySubscription.sliceSize] = newClient
	mySubscription.sliceSize++
	protocol.Debug("Subscribed client\r\n")
	return
}

//Removes a client from our clients slice, and compacts the remaining clients.
func (mySubscription *Subscription) RemoveClient(oldClient *Client) {
	mySubscription.sliceMutex.Lock()
	defer mySubscription.sliceMutex.Unlock()
	for index, client := range mySubscription.clients {
		if client == oldClient {
			copy(mySubscription.clients[index:], mySubscription.clients[index+1:])
			mySubscription.clients[mySubscription.sliceSize-1] = nil
			mySubscription.sliceSize--
			return
		}
	}
}
