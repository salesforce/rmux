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
	"bytes"
	"fmt"
	"github.com/forcedotcom/rmux/connection"
	"github.com/forcedotcom/rmux/protocol"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	//Response code for when a command (that operates on multiple keys) is used on a server that is multiplexing
	MULTIPLEX_OPERATION_UNSUPPORTED_RESPONSE = []byte("-ERR This command is not supported for multiplexing servers")
	//Response code for when a client can't connect to any target servers
	CONNECTION_DOWN_RESPONSE = []byte("-ERR Connection down")
)

//The main RedisMultiplexer
//Listens on a specified socket or port, and assigns out queries to any number of connection pools
//If more than one connection pool is given multi-key operations are blocked
type RedisMultiplexer struct {
	HashRing *connection.HashRing
	//hashmap of [connection endpoint] -> connectionPools
	ConnectionCluster []*connection.ConnectionPool
	//hashmap of [subscription names] -> subscriptions
	SubscriptionCluster map[string]*Subscription
	//The net.listener for our server
	Listener net.Listener
	//The amount of connections to store, in each of our connectionpools
	PoolSize int
	//The primary connection key to use.  If we're not operating on a key-based operation, it will go here
	PrimaryConnectionPool *connection.ConnectionPool
	//And overridable connect timeout.  Defaults to EXTERN_CONNECT_TIMEOUT
	EndpointConnectTimeout time.Duration
	//An overridable read timeout.  Defaults to EXTERN_READ_TIMEOUT
	EndpointReadTimeout time.Duration
	//An overridable write timeout.  Defaults to EXTERN_WRITE_TIMEOUT
	EndpointWriteTimeout time.Duration
	//An overridable read timeout.  Defaults to EXTERN_READ_TIMEOUT
	ClientReadTimeout time.Duration
	//An overridable write timeout.  Defaults to EXTERN_WRITE_TIMEOUT
	ClientWriteTimeout time.Duration

	//Whether or not the multiplexer is active.  Used to determine when a tear-down should be occuring
	active bool
	//The amount of active (outbound) connections that we have
	activeConnectionCount int
	//The amount of total (incoming) connections that we have
	connectionCount int32
	//whether or not we are multiplexing
	multiplexing bool
	infoResponse []byte
	infoMutex    sync.Mutex
}

//Sub-task that handles the cleanup when a server goes down
func (myRedisMultiplexer *RedisMultiplexer) initializeCleanup() {
	//Make a single-item channel for sigterm requests
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	// Block until we have a kill-request to pop off
	<-c
	//Flag ourselves as cleaning up
	myRedisMultiplexer.active = false
	//And close our listener
	myRedisMultiplexer.Listener.Close()
	//Give ourselves a bit to clean up
	time.Sleep(time.Millisecond * 150)
	os.Exit(1)
}

//Initializes a new redis multiplexer, listening on the given protocol/endpoint, with a set connectionPool size
//ex: "unix", "/tmp/myAwesomeSocket", 50
func NewRedisMultiplexer(listenProtocol, listenEndpoint string, poolSize int) (newRedisMultiplexer *RedisMultiplexer, err error) {
	newRedisMultiplexer = &RedisMultiplexer{}
	newRedisMultiplexer.Listener, err = net.Listen(listenProtocol, listenEndpoint)
	if err != nil {
		println("listen error", err.Error())
		return nil, err
	}
	newRedisMultiplexer.ConnectionCluster = make([]*connection.ConnectionPool, 0)
	newRedisMultiplexer.SubscriptionCluster = make(map[string]*Subscription)
	newRedisMultiplexer.PoolSize = poolSize
	newRedisMultiplexer.active = true
	newRedisMultiplexer.EndpointConnectTimeout = connection.EXTERN_CONNECT_TIMEOUT
	newRedisMultiplexer.EndpointReadTimeout = connection.EXTERN_READ_TIMEOUT
	newRedisMultiplexer.EndpointWriteTimeout = connection.EXTERN_WRITE_TIMEOUT
	newRedisMultiplexer.ClientReadTimeout = connection.EXTERN_READ_TIMEOUT
	newRedisMultiplexer.ClientWriteTimeout = connection.EXTERN_WRITE_TIMEOUT
	newRedisMultiplexer.infoMutex = sync.Mutex{}
	protocol.Debug("Redis Multiplexer Initialized\r\n")
	return
}

//Adds a connection to the redis multiplexer, for the given protocol and endpoint
func (myRedisMultiplexer *RedisMultiplexer) AddConnection(remoteProtocol, remoteEndpoint string) {
	connectionCluster := connection.NewConnectionPool(remoteProtocol, remoteEndpoint, myRedisMultiplexer.PoolSize)
	connectionCluster.ConnectTimeout = myRedisMultiplexer.EndpointConnectTimeout
	connectionCluster.ReadTimeout = myRedisMultiplexer.EndpointReadTimeout
	connectionCluster.WriteTimeout = myRedisMultiplexer.EndpointWriteTimeout
	myRedisMultiplexer.ConnectionCluster = append(myRedisMultiplexer.ConnectionCluster, connectionCluster)
	if len(myRedisMultiplexer.ConnectionCluster) == 1 {
		myRedisMultiplexer.PrimaryConnectionPool = connectionCluster
	} else {
		myRedisMultiplexer.multiplexing = true
	}
}

//Counts the number of active endpoints on the server
func (myRedisMultiplexer *RedisMultiplexer) countActiveConnections() (activeConnections int) {
	activeConnections = 0
	for _, connectionPool := range myRedisMultiplexer.ConnectionCluster {
		if connectionPool.CheckConnectionState() {
			activeConnections++
		}
	}
	return
}

//Refreshes all subscription's connections, incase one of our connectionPools has fallen offline, or come back online
func (myRedisMultiplexer *RedisMultiplexer) refreshSubscriptions() {
	for channelName, mySubscription := range myRedisMultiplexer.SubscriptionCluster {
		if len(mySubscription.clients) == 0 {
			mySubscription.RequestStop = true
			delete(myRedisMultiplexer.SubscriptionCluster, channelName)
			continue
		}
		connectionPool := myRedisMultiplexer.HashRing.GetConnectionPool(2, []byte(channelName))
		mySubscription.UpdateConnection(connectionPool.Endpoint, connectionPool)
	}
	return
}

//Checks the status of all connections, and calculates how many of them are currently up
func (myRedisMultiplexer *RedisMultiplexer) maintainConnectionStates() {
	var m runtime.MemStats
	for myRedisMultiplexer.active {
		myRedisMultiplexer.activeConnectionCount = myRedisMultiplexer.countActiveConnections()
		myRedisMultiplexer.refreshSubscriptions()
		protocol.Debug("We have %d connections\r\n", myRedisMultiplexer.connectionCount)
		runtime.ReadMemStats(&m)
		protocol.Debug("Memory profile: InUse(%d) Idle (%d) Released(%d)\n", m.HeapInuse, m.HeapIdle, m.HeapReleased)
		myRedisMultiplexer.generateMultiplexInfo()
		time.Sleep(100 * time.Millisecond)
	}
}

//Generates the Info response for a multiplexed server
func (myRedisMultiplexer *RedisMultiplexer) generateMultiplexInfo() {
	tmpSlice := fmt.Sprintf("rmux_version: %s\r\ngo_version: %s\r\nprocess_id: %d\r\nconnected_clients: %d\r\nactive_endpoints: %d\r\ntotal_endpoints: %d\r\nrole: master\r\n", "1.0", runtime.Version(), os.Getpid(), myRedisMultiplexer.connectionCount, myRedisMultiplexer.activeConnectionCount, len(myRedisMultiplexer.ConnectionCluster))
	myRedisMultiplexer.infoMutex.Lock()
	myRedisMultiplexer.infoResponse = []byte(fmt.Sprintf("$%d\r\n%s", len(tmpSlice), tmpSlice))
	myRedisMultiplexer.infoMutex.Unlock()
}

//Called when a rmux server is ready to begin accepting connections
func (myRedisMultiplexer *RedisMultiplexer) Start() (err error) {
	myRedisMultiplexer.HashRing, err = connection.NewHashRing(myRedisMultiplexer.ConnectionCluster)
	if err != nil {
		println(err)
		return
	}

	go myRedisMultiplexer.maintainConnectionStates()
	go myRedisMultiplexer.initializeCleanup()

	for myRedisMultiplexer.active {
		fd, err := myRedisMultiplexer.Listener.Accept()
		if err != nil {
			protocol.Debug("Start: Error received from listener.Accept", err.Error())
			continue
		}

		go myRedisMultiplexer.initializeClient(fd)
	}
	time.Sleep(100 * time.Millisecond)
	return
}

//Initializes a client's connection to our server.  Sets up our disconnect hooks and then passes the client off for request handling
func (myRedisMultiplexer *RedisMultiplexer) initializeClient(localConnection net.Conn) {
	defer func() {
		atomic.AddInt32(&myRedisMultiplexer.connectionCount, -1)
	}()
	atomic.AddInt32(&myRedisMultiplexer.connectionCount, 1)
	//Add the connection to our internal list
	myClient := NewClient(localConnection, myRedisMultiplexer.ClientReadTimeout, myRedisMultiplexer.ClientWriteTimeout)
	defer func() {
		r := recover()
		if r != nil {
			if val, ok := r.(string); ok {
				protocol.FlushLine([]byte(val), myClient.ReadWriter.Writer)
			}
		}
		myClient.ConnectionReadWriter.NetConnection.Close()
	}()
	myRedisMultiplexer.HandleClientRequests(myClient)
}

//Sends the pre-generated Info response for a multiplexed server
func (myRedisMultiplexer *RedisMultiplexer) sendMultiplexInfo(myClient *Client) (err error) {
	myRedisMultiplexer.infoMutex.Lock()
	err = protocol.FlushLine(myRedisMultiplexer.infoResponse, myClient.ReadWriter.Writer)
	myRedisMultiplexer.infoMutex.Unlock()
	return
}

//Handles requests for a client.
//Inspects all incoming commands, to find if they are key-driven or not.
//If they are, finds the appropriate connection pool, and passes the request off to it.
func (myRedisMultiplexer *RedisMultiplexer) HandleClientRequests(myClient *Client) {
	var err error
	var firstLine []byte
	var connectionPool *connection.ConnectionPool
	for myRedisMultiplexer.active && myClient.Active {
		firstLine, _, err = myClient.ReadWriter.ReadLine()
		protocol.Debug("I read %s\r\n", firstLine)
		if err != nil {
			if err == io.EOF {
				return
			}
			_, ok := err.(net.Error)
			if ok && err.(net.Error).Timeout() {
				//We had a read timeout.  Continue and try try again
				continue
			}
			panic("New Client Read Error: " + err.Error())
		}
		totalTime := time.Now()

		responded, err := myClient.ParseCommand(firstLine, myRedisMultiplexer.multiplexing)

		if err != nil {
			protocol.Debug("Received error %s from ParseCommand\r\n", err)
			return
		} else if responded {
			protocol.Debug("Apparently we already responded, continuing\r\n")
			continue
		}
		//If we have an INFO command, and multiplexing is enabled..
		if bytes.Equal(myClient.command[0:4], protocol.INFO_COMMAND) && myRedisMultiplexer.multiplexing {
			protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
			err = myRedisMultiplexer.sendMultiplexInfo(myClient)
			if err != nil {
				protocol.Debug("Receieved error %s from sendMultiplexInfo: %s\r\n", err)
				return
			}
			continue
		}

		//If we have a SUBSCRIBE command, hook it up...
		if bytes.Equal(myClient.command[0:4], protocol.SUBSCRIBE_COMMAND) {
			protocol.Debug("Received subscribe request on channel: %s\r\n", myClient.stringArgument)
			subscription := myRedisMultiplexer.GetSubscription(myClient.stringArgument)
			protocol.Debug("Got subscription\r\n")
			subscription.AddClient(myClient)
			err = myClient.SendSubscriptionResponse()
			if err != nil {
				protocol.Debug("Received error %s from SendSubscriptionResponse", err)
				return
			}
			protocol.Debug("Confirmed subscription\r\n")
			protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
			continue
		}

		startTime := time.Now()
		connectionPool = myRedisMultiplexer.HashRing.GetConnectionPool(myClient.argumentCount, myClient.firstArgumentSlice[0:myClient.argumentLength])
		protocol.Debug("Connection time: %s\r\n", time.Since(startTime))

		err = myClient.HandleRequest(connectionPool, firstLine)
		if err != nil {
			protocol.Debug("Error received from HandleRequest call: %s\r\n", err)
		}
		protocol.Debug("Total request time: %s\r\n", time.Since(totalTime))
	}
}

//Gets an active subscription for our client to connect to
func (myRedisMultiplexer *RedisMultiplexer) GetSubscription(channelName string) (mySubscription *Subscription) {
	myRedisMultiplexer.infoMutex.Lock()
	defer myRedisMultiplexer.infoMutex.Unlock()
	//if we need to make a new subscription, do so
	mySubscription, ok := myRedisMultiplexer.SubscriptionCluster[channelName]
	if !ok {
		mySubscription = NewSubscription(10, channelName)
		myRedisMultiplexer.SubscriptionCluster[channelName] = mySubscription
		go mySubscription.BroadcastMessages()
	}
	connectionPool := myRedisMultiplexer.HashRing.GetConnectionPool(2, []byte(channelName))
	mySubscription.UpdateConnection(connectionPool.Endpoint, connectionPool)
	return
}
