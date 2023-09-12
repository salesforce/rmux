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
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"rmux/connection"
	"rmux/graphite"
	"rmux/log"
	"rmux/protocol"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	//Response code for when a command (that operates on multiple keys) is used on a server that is multiplexing
	MULTIPLEX_OPERATION_UNSUPPORTED_RESPONSE = []byte("This command is not supported for multiplexing servers")
	//Response code for when a client can't connect to any target servers
	CONNECTION_DOWN_RESPONSE = []byte("Connection down")
	//Default diagnostic check interval
	EXTERN_DIAGNOSTIC_CHECK_INTERVAL = 1 * time.Second
)

var version string = "dev"

// The main RedisMultiplexer
// Listens on a specified socket or port, and assigns out queries to any number of connection pools
// If more than one connection pool is given multi-key operations are blocked
type RedisMultiplexer struct {
	HashRing *connection.HashRing
	//hashmap of [connection endpoint] -> connectionPools
	ConnectionCluster []*connection.ConnectionPool
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
	//An overridable reconnection interval. Defaults to EXTERN_RECONNECT_INTERVAL
	EndpointReconnectInterval time.Duration
	//An overridable diagnostic check interval.  Defaults to EXTERN_DIAGNOSTIC_CHECK_INTERVAL
	EndpointDiagnosticCheckInterval time.Duration
	//An overridable read timeout.  Defaults to EXTERN_READ_TIMEOUT
	ClientReadTimeout time.Duration
	//An overridable write timeout.  Defaults to EXTERN_WRITE_TIMEOUT
	ClientWriteTimeout time.Duration
	// The graphite statsd server to ping with metrics
	GraphiteServer *string
	//Whether or not the multiplexer is active.  Used to determine when a tear-down should be occuring
	active bool
	//The amount of active (outbound) connections that we have
	activeConnectionCount int
	//The amount of total (incoming) connections that we have
	connectionCount int32
	//whether or not we are multiplexing
	multiplexing bool
	// Cached 'info' command response for multiplexing servers
	infoResponse []byte
	// Read/Write mutex for above infoResponse slice
	infoMutex sync.RWMutex
	// Whether to failover to another connection pool if the target connection pool is down (in multiplexing mode)
	Failover bool
}

// Sub-task that handles the cleanup when a server goes down
func (this *RedisMultiplexer) initializeCleanup() {
	//Make a single-item channel for sigterm requests
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	// Block until we have a kill-request to pop off
	<-c
	//Flag ourselves as cleaning up
	this.active = false
	//And close our listener
	this.Listener.Close()
	//Give ourselves a bit to clean up
	time.Sleep(time.Millisecond * 150)
	os.Exit(0)
}

// Initializes a new redis multiplexer, listening on the given protocol/endpoint, with a set connectionPool size
// ex: "unix", "/tmp/myAwesomeSocket", 50
func NewRedisMultiplexer(listenProtocol, listenEndpoint string, poolSize int) (newRedisMultiplexer *RedisMultiplexer, err error) {
	newRedisMultiplexer = &RedisMultiplexer{}
	newRedisMultiplexer.Listener, err = net.Listen(listenProtocol, listenEndpoint)
	if err != nil {
		println("listen error", err.Error())
		return nil, err
	}
	newRedisMultiplexer.ConnectionCluster = make([]*connection.ConnectionPool, 0)
	newRedisMultiplexer.PoolSize = poolSize
	newRedisMultiplexer.active = true
	newRedisMultiplexer.EndpointConnectTimeout = connection.EXTERN_CONNECT_TIMEOUT
	newRedisMultiplexer.EndpointReadTimeout = connection.EXTERN_READ_TIMEOUT
	newRedisMultiplexer.EndpointWriteTimeout = connection.EXTERN_WRITE_TIMEOUT
	newRedisMultiplexer.EndpointReconnectInterval = connection.EXTERN_RECONNECT_INTERVAL
	newRedisMultiplexer.EndpointDiagnosticCheckInterval = EXTERN_DIAGNOSTIC_CHECK_INTERVAL
	newRedisMultiplexer.ClientReadTimeout = connection.EXTERN_READ_TIMEOUT
	newRedisMultiplexer.ClientWriteTimeout = connection.EXTERN_WRITE_TIMEOUT
	newRedisMultiplexer.infoMutex = sync.RWMutex{}
	//	Debug("Redis Multiplexer Initialized")
	return
}

// Adds a connection to the redis multiplexer, for the given protocol and endpoint
func (this *RedisMultiplexer) AddConnection(remoteProtocol, remoteEndpoint string) {
	connectionCluster := connection.NewConnectionPool(remoteProtocol, remoteEndpoint, this.PoolSize,
		this.EndpointConnectTimeout, this.EndpointReadTimeout, this.EndpointWriteTimeout,
		this.EndpointReconnectInterval)
	this.ConnectionCluster = append(this.ConnectionCluster, connectionCluster)
	if len(this.ConnectionCluster) == 1 {
		this.PrimaryConnectionPool = connectionCluster
	} else {
		this.multiplexing = true
	}
}

// Counts the number of active endpoints (connection pools) on the server
func (this *RedisMultiplexer) countActiveConnections() (activeConnections int) {
	activeConnections = 0
	for _, connectionPool := range this.ConnectionCluster {
		if connectionPool.CheckConnectionState() {
			activeConnections++
		}
	}

	if this.activeConnectionCount < activeConnections {
		log.Info("Connected diagnostics connection.")
	}
	return
}

// Checks the status of all connections, and calculates how many of them are currently up
// This only counts connection pools / diagnostic connections not real redis sessions
func (this *RedisMultiplexer) maintainConnectionStates() {
	var m runtime.MemStats
	for this.active {
		this.activeConnectionCount = this.countActiveConnections()
		//		// Debug("We have %d connections", this.connectionCount)
		runtime.ReadMemStats(&m)
		//		// Debug("Memory profile: InUse(%d) Idle (%d) Released(%d)", m.HeapInuse, m.HeapIdle, m.HeapReleased)
		this.generateMultiplexInfo()
		time.Sleep(this.EndpointDiagnosticCheckInterval)
	}
}

// Generates the Info response for a multiplexed server
func (this *RedisMultiplexer) generateMultiplexInfo() {
	tmpSlice := fmt.Sprintf("rmux_version: %s\r\ngo_version: %s\r\nprocess_id: %d\r\nconnected_clients: %d\r\nactive_endpoints: %d\r\ntotal_endpoints: %d\r\nrole: master\r\n", version, runtime.Version(), os.Getpid(), this.connectionCount, this.activeConnectionCount, len(this.ConnectionCluster))
	this.infoMutex.Lock()
	this.infoResponse = []byte(fmt.Sprintf("$%d\r\n%s", len(tmpSlice), tmpSlice))
	this.infoMutex.Unlock()
}

// Called when a rmux server is ready to begin accepting connections
func (this *RedisMultiplexer) Start() (err error) {
	this.HashRing, err = connection.NewHashRing(this.ConnectionCluster, this.Failover)
	if err != nil {
		return err
	}

	go this.maintainConnectionStates()
	go this.initializeCleanup()
	//if graphite.Enabled() {
	//	go this.GraphiteCheckin()
	//}

	for this.active {
		fd, err := this.Listener.Accept()
		if err != nil {
			//			Debug("Start: Error received from listener.Accept: %s", err.Error())
			continue
		}
		//		Debug("Accepted connection.")
		graphite.Increment("accepted")

		go this.initializeClient(fd)
	}
	time.Sleep(100 * time.Millisecond)
	return
}

// Initializes a client's connection to our server.  Sets up our disconnect hooks and then passes the client off for request handling
func (this *RedisMultiplexer) initializeClient(localConnection net.Conn) {
	defer func() {
		atomic.AddInt32(&this.connectionCount, -1)
	}()
	atomic.AddInt32(&this.connectionCount, 1)
	//Add the connection to our internal list
	myClient := NewClient(localConnection, this.ClientReadTimeout, this.ClientWriteTimeout,
		this.multiplexing, this.HashRing)

	defer func() {
		if r := recover(); r != nil {
			//			DebugPanic(r)
			if val, ok := r.(string); ok {
				// If we paniced, push that to the client before closing the connection
				protocol.WriteError([]byte(val), myClient.Writer, true)
			}
		}

		//		Debug("Closing client connection.")
		myClient.Connection.Close()
	}()

	this.HandleClientRequests(myClient)
}

// Sends the pre-generated Info response for a multiplexed server
func (this *RedisMultiplexer) sendMultiplexInfo(myClient *Client) (err error) {
	this.infoMutex.RLock()
	err = protocol.WriteLine(this.infoResponse, myClient.Writer, true)
	this.infoMutex.RUnlock()
	return
}

func (this *RedisMultiplexer) GraphiteCheckin() {
	for this.active {
		time.Sleep(time.Millisecond * 100)
		for _, pool := range this.ConnectionCluster {
			pool.ReportGraphite()
		}
	}
}

// Handles requests for a client.
// Inspects all incoming commands, to find if they are key-driven or not.
// If they are, finds the appropriate connection pool, and passes the request off to it.
func (this *RedisMultiplexer) HandleClientRequests(client *Client) {
	// Create background i/o thread
	go client.ReadLoop(this)

	defer func() {
		//		Debug("Client command handling loop closing")
		// If the multiplexer goes down, deactivate this client.
		client.Active = false
	}()

	for this.active && client.Active {
		select {
		case item := <-client.ReadChannel:
			if item.command != nil {
				this.HandleCommandChunk(client, item.command)
			}
			if item.err != nil {
				this.HandleError(client, item.err)
			}
		case <-time.After(time.Second * 1):
			// Allow heartbeat checks to happen once a second
		}
	}

	// TODO defer closing stuff?
}

// This looks a lot like HandleClientRequests above, but will break and flush to redis if there is nothing to read.
// Will allow it to handle a pipeline of commands without spinning indefinitely.
func (this *RedisMultiplexer) HandleCommandChunk(client *Client, command protocol.Command) {
	this.HandleCommand(client, command)

ChunkLoop:
	for this.active && client.Active {
		select {
		case item := <-client.ReadChannel:
			if item.command != nil {
				this.HandleCommand(client, item.command)
			}
			if item.err != nil {
				this.HandleError(client, item.err)
			}
		default:
			break ChunkLoop
		}
	}

	client.FlushRedisAndRespond()
}

func (this *RedisMultiplexer) HandleCommand(client *Client, command protocol.Command) {
	if this.multiplexing && bytes.Equal(command.GetCommand(), protocol.INFO_COMMAND) {
		this.sendMultiplexInfo(client)
		return
	}

	//	Debug("Writing out %q", command)
	immediateResponse, err := client.ParseCommand(command)

	if immediateResponse != nil {
		// Respond with anything we have queued
		if client.HasQueued() {
			client.FlushRedisAndRespond()
		}

		err = client.WriteLine(immediateResponse)
		if err != nil {
			//			Debug("Error received when writing an immediate response: %s", err)
		}

		return
	} else if err != nil {
		if err == ERR_QUIT {
			client.WriteLine(protocol.OK_RESPONSE)
			client.ReadChannel <- readItem{nil, err}
			return
		} else if recErr, ok := err.(*protocol.RecoverableError); ok {
			client.WriteError(recErr, false)
		} else {
			panic("Not sure how to handle this error: " + err.Error())
		}

		return
	}

	// Otherwise, the command is ready to buffer to the connection.
	client.Queue(command)

	// If we're multiplexing, just handle one command at a time
	if this.multiplexing && client.HasQueued() {
		client.FlushRedisAndRespond()
	}
}

func (this *RedisMultiplexer) HandleError(client *Client, err error) {
	if err == nil {
		return
	}

	if err == ERR_QUIT {
		client.Active = false
		return
	} else if recErr, ok := err.(*protocol.RecoverableError); ok {
		// Since we can recover, flush an error to the client
		log.Error("Error from server: %s", recErr)
		client.FlushError(recErr)
		return
	} else if err == io.EOF {
		// Stream EOF-ed. Deactivate this client and break out.
		client.FlushRedisAndRespond()
		client.Active = false
		return
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		// We had a read timeout. Let the client know that the connection is down
		graphite.Increment("nettimeout")
		client.FlushError(ERR_TIMEOUT)
		return
	} else {
		// This is something we've never seen before! Panic panic panic
		panic("New Client Read Error: " + err.Error())
	}
}

func (rm *RedisMultiplexer) SetAllTimeouts(t time.Duration) {
	rm.EndpointConnectTimeout = t
	rm.EndpointReadTimeout = t
	rm.EndpointWriteTimeout = t
	rm.ClientReadTimeout = t
	rm.ClientWriteTimeout = t
}
