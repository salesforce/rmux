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

package connection

import (
//	. "github.com/forcedotcom/rmux/log"
	"time"
	"sync/atomic"
	"github.com/forcedotcom/rmux/graphite"
	"strings"
)

const (
	//Default connect timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_CONNECT_TIMEOUT = time.Millisecond * 500
	//Default read timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_READ_TIMEOUT = time.Millisecond * 500
	//Default write timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_WRITE_TIMEOUT = time.Millisecond * 500
)

//A pool of connections to a single outbound redis server
type ConnectionPool struct {
	//The protocol to use for our connections (unix/tcp/udp)
	Protocol string
	//The endpoint to connect to
	Endpoint string
	//Represents whether or not the connection pool is currently up or down
	IsConnected bool
	//And overridable connect timeout.  Defaults to EXTERN_CONNECT_TIMEOUT
	ConnectTimeout time.Duration
	//An overridable read timeout.  Defaults to EXTERN_READ_TIMEOUT
	ReadTimeout time.Duration
	//An overridable write timeout.  Defaults to EXTERN_WRITE_TIMEOUT
	WriteTimeout time.Duration
	//channel of recycled connections, for re-use
	connectionPool chan *Connection
	// Number of active connections
	Count int32
}

//Initialize a new connection pool, for the given protocol/endpoint, with a given pool capacity
//ex: "unix", "/tmp/myAwesomeSocket", 5
func NewConnectionPool(Protocol, Endpoint string, poolCapacity int) (newConnectionPool *ConnectionPool) {
	newConnectionPool = &ConnectionPool{}
	newConnectionPool.Protocol = Protocol
	newConnectionPool.Endpoint = Endpoint
	newConnectionPool.connectionPool = make(chan *Connection, poolCapacity)
	newConnectionPool.ConnectTimeout = EXTERN_CONNECT_TIMEOUT
	newConnectionPool.ReadTimeout = EXTERN_READ_TIMEOUT
	newConnectionPool.WriteTimeout = EXTERN_WRITE_TIMEOUT
	newConnectionPool.Count = 0

	// Fill the pool with as many handlers as it asks for
	for i := 0; i < poolCapacity; i++ {
		newConnectionPool.connectionPool <- NewConnection(
			newConnectionPool.Protocol,
			newConnectionPool.Endpoint,
			newConnectionPool.ConnectTimeout,
			newConnectionPool.ReadTimeout,
			newConnectionPool.WriteTimeout,
		)
	}

	return
}

//Gets a connection from the connection pool
func (myConnectionPool *ConnectionPool) GetConnection() (myConnection *Connection) {
	select {
	case myConnection = <-myConnectionPool.connectionPool:
		atomic.AddInt32(&myConnectionPool.Count, 1)
		myConnection.ReconnectIfNecessary()
		return
	// TODO: Maybe a while/timeout/graphiteping loop?
	}
}

//Recycles a connection back into our connection pool
//If the pool is full, throws it away
func (myConnectionPool *ConnectionPool) RecycleRemoteConnection(remoteConnection *Connection) {
	myConnectionPool.connectionPool <- remoteConnection
	atomic.AddInt32(&myConnectionPool.Count, -1)
}

//Checks the state of connections in this connection pool
//If a remote server has severe lag, mysteriously goes away, or stops responding all-together, returns false
func (myConnectionPool *ConnectionPool) CheckConnectionState() bool {
	//get a connection from the channel
	myConnection := myConnectionPool.GetConnection()
	//If we failed to bind, or if our PING fails, abort
	if myConnection != nil && myConnection.CheckConnection() {
//		// protocol.Debug("The pool is up")
		myConnectionPool.IsConnected = true
		myConnectionPool.RecycleRemoteConnection(myConnection)
		return true
	} else {
//		Debug("The pool is down")

		myConnectionPool.IsConnected = false
		for {
			//drain all the connections
			select {
			case myConnection = <-myConnectionPool.connectionPool:
				continue
			default:
				return false
			}
		}
		return false
	}
}

func (cp *ConnectionPool) ReportGraphite() {
	endpoint := strings.Replace(cp.Endpoint, ".", "-", -1)
	endpoint = strings.Replace(cp.Endpoint, ":", "-", -1)

	graphite.Gauge("pools." + endpoint, int(cp.Count))
}
