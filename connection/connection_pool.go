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
	"rmux/graphite"
	"rmux/log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	//Default connect timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_CONNECT_TIMEOUT = time.Millisecond * 500
	//Default read timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_READ_TIMEOUT = time.Millisecond * 500
	//Default write timeout, for connection pools.  Can be adjusted on individual pools after initialization
	EXTERN_WRITE_TIMEOUT = time.Millisecond * 500
)

// A pool of connections to a single outbound redis server
type ConnectionPool struct {
	//The protocol to use for our connections (unix/tcp/udp)
	Protocol string
	//The endpoint to connect to
	Endpoint string
	//And overridable connect timeout.  Defaults to EXTERN_CONNECT_TIMEOUT
	ConnectTimeout time.Duration
	//An overridable read timeout.  Defaults to EXTERN_READ_TIMEOUT
	ReadTimeout time.Duration
	//An overridable write timeout.  Defaults to EXTERN_WRITE_TIMEOUT
	WriteTimeout time.Duration
	//channel of recycled connections, for re-use
	connectionPool chan *Connection
	// The connection used for diagnostics (like checking that the pool is up)
	diagnosticConnection     *Connection
	diagnosticConnectionLock sync.Mutex
	// Number of active connections
	Count         int32
	connectedLock sync.RWMutex
	// Whether or not the connction pool is up or down
	isConnected bool
}

// Initialize a new connection pool, for the given protocol/endpoint, with a given pool capacity
// ex: "unix", "/tmp/myAwesomeSocket", 5
func NewConnectionPool(Protocol, Endpoint string, poolCapacity int, connectTimeout time.Duration,
	readTimeout time.Duration, writeTimeout time.Duration) (newConnectionPool *ConnectionPool) {
	newConnectionPool = &ConnectionPool{}
	newConnectionPool.Protocol = Protocol
	newConnectionPool.Endpoint = Endpoint
	newConnectionPool.connectionPool = make(chan *Connection, poolCapacity)
	newConnectionPool.ConnectTimeout = connectTimeout
	newConnectionPool.ReadTimeout = readTimeout
	newConnectionPool.WriteTimeout = writeTimeout
	newConnectionPool.Count = 0

	// Fill the pool with as many handlers as it asks for
	for i := 0; i < poolCapacity; i++ {
		newConnectionPool.connectionPool <- newConnectionPool.CreateConnection()
	}

	newConnectionPool.diagnosticConnection = newConnectionPool.CreateConnection()

	return
}

// Gets a connection from the connection pool
func (cp *ConnectionPool) GetConnection() (connection *Connection, err error) {
	select {
	case connection = <-cp.connectionPool:
		atomic.AddInt32(&cp.Count, 1)

		if err := connection.ReconnectIfNecessary(); err != nil {
			// Recycle the holder, return an error
			cp.RecycleRemoteConnection(connection)
			log.Error("Received a nil connection in pool.GetConnection: %s", err)
			graphite.Increment("reconnect_error")
			return nil, err
		}

		return connection, nil
		// TODO: Maybe a while/timeout/graphiteping loop?
	}
}

// Creates a new Connection basead on the pool's configuration
func (cp *ConnectionPool) CreateConnection() *Connection {
	return NewConnection(
		cp.Protocol,
		cp.Endpoint,
		cp.ConnectTimeout,
		cp.ReadTimeout,
		cp.WriteTimeout,
	)
}

func (cp *ConnectionPool) getDiagnosticConnection() (connection *Connection, err error) {
	cp.diagnosticConnectionLock.Lock()

	if err := cp.diagnosticConnection.ReconnectIfNecessary(); err != nil {
		log.Error("The diagnostic connection is down for %s:%s : %s", cp.Protocol, cp.Endpoint, err)
		cp.diagnosticConnectionLock.Unlock()
		return nil, err
	}

	return cp.diagnosticConnection, nil
}

func (cp *ConnectionPool) releaseDiagnosticConnection() {
	cp.diagnosticConnectionLock.Unlock()
}

// Recycles a connection back into our connection pool
// If the pool is full, throws it away
func (myConnectionPool *ConnectionPool) RecycleRemoteConnection(remoteConnection *Connection) {
	myConnectionPool.connectionPool <- remoteConnection
	atomic.AddInt32(&myConnectionPool.Count, -1)
}

func (cp *ConnectionPool) SetIsConnected(isConnected bool) {
	cp.connectedLock.Lock()
	defer cp.connectedLock.Unlock()
	cp.isConnected = isConnected
}

func (cp *ConnectionPool) IsConnected() bool {
	cp.connectedLock.RLock()
	defer cp.connectedLock.RUnlock()
	return cp.isConnected
}

// Checks the state of connections in this connection pool
// If a remote server has severe lag, mysteriously goes away, or stops responding all-together, returns false
// This is only used for diagnostic connections!
func (cp *ConnectionPool) CheckConnectionState() (isUp bool) {
	isUp = true
	defer func() {
		cp.SetIsConnected(isUp)
	}()

	connection, err := cp.getDiagnosticConnection()
	if err != nil {
		isUp = false
		return
	}
	defer cp.releaseDiagnosticConnection()

	if !connection.CheckConnection() {
		connection.Disconnect()
		isUp = false
		return
	}

	return
}

func (cp *ConnectionPool) ReportGraphite() {
	endpoint := strings.Replace(cp.Endpoint, ".", "-", -1)
	endpoint = strings.Replace(cp.Endpoint, ":", "-", -1)

	graphite.Gauge("pools."+endpoint, int(cp.Count))
}
