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
	"testing"
	"time"
	"net"
	"github.com/forcedotcom/rmux/connection"
	"bytes"
)

func TestConnectTimeout(t *testing.T) {
	timeouts := []time.Duration {
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		1 * time.Millisecond,
	}

	rmux, err := NewRedisMultiplexer("unix", "/tmp/rmuxTestConnectTimeout.sock", 1)
	if err != nil {
		t.Errorf("Error from creating a new rmux instance: %s", err)
		return
	}
	defer rmux.Listener.Close()


	for _, timeout := range timeouts {
		rmux.EndpointConnectTimeout = timeout

		// Shamelessly stole this address from the net.DialTimeout test code
		rmux.AddConnection("tcp", "127.0.71.111:49151")

		start := time.Now()

		_, err = rmux.PrimaryConnectionPool.GetConnection()
		if err == nil {
			t.Errorf("Should have received a connect error from timing out")
			return
		} else if e, ok := err.(*net.OpError); !ok || !e.Timeout() {
			t.Errorf("Should have received a timeout error")
			return
		}

		diff := time.Now().Sub(start)
		if diff < timeout || diff > timeout + (10 * time.Millisecond) {
			t.Errorf("Should have timed out in the given interval between %s and %s, but instead timed out in %s",
				timeout, timeout + 10 * time.Millisecond, diff)
		}

		// Clear the pool
		rmux.ConnectionCluster = []*connection.ConnectionPool{}
	}
}

func TestReadTimeout(t *testing.T) {
	rmux, err := NewRedisMultiplexer("unix", "/tmp/rmuxTestConnectTimeout.sock", 1)
	if err != nil {
		t.Errorf("Error from creating a new rmux instance: %s", err)
		return
	}
	defer rmux.Listener.Close()

	mockRedis, err := net.Listen("tcp", "localhost:8090")
	if err != nil {
		t.Errorf("Error from listening on socket: %s", err)
		return
	}
	defer mockRedis.Close()

	rmux.SetAllTimeouts(10 * time.Millisecond)
	rmux.AddConnection("tcp", "localhost:8090")

	conn, err := rmux.PrimaryConnectionPool.GetConnection()
	if err != nil {
		t.Errorf("Error when getting connection: %s", err)
		return
	}

	conn.Writer.Write([]byte("+PING\r\n"))
	if err := conn.Writer.Flush(); err != nil {
		t.Errorf("Error when writing to redis: %s", err)
	}

	b := make([]byte, 2048)
	_, err = conn.Reader.Read(b)
	if err == nil {
		t.Errorf("Should have errored when reading from server that isn't responding")
		return
	} else if e, ok := err.(*net.OpError); !ok || !e.Timeout() {
		t.Errorf("Should have received a timeout error from read. Got %s", err)
		return
	}
}

func TestRmuxTimeoutConnectionTeardown(t *testing.T) {
	rmux, err := NewRedisMultiplexer("unix", "/tmp/rmuxTimeoutConnectionTeardown.sock", 1)
	if err != nil {
		t.Fatalf("Error creating new rmux instance: %s", err)
	}
	defer rmux.Listener.Close()

	redisSock := "/tmp/rmuxTimeoutConnectionTeardown-redis-1.sock"
	unixAddr, err := net.ResolveUnixAddr("unix", redisSock)
	if err != nil {
		t.Errorf("Error resolving socket: %s", err)
	}
	mockRedis, err := net.ListenUnix("unix", unixAddr)
	if err != nil {
		t.Errorf("Error listening no socket: %s", err)
	}
	defer mockRedis.Close()

	rmux.SetAllTimeouts(2 * time.Millisecond)
	rmux.AddConnection("unix", redisSock)

	conn, err := rmux.PrimaryConnectionPool.GetConnection()
	if err != nil {
		t.Errorf("Error when getting connection from pool: %s", err)
	}

	mockRedis.SetDeadline(time.Now().Add(10 * time.Millisecond))
	mockConn, err := mockRedis.Accept()
	if err != nil {
		t.Errorf("Error when accepting connection: %s", err)
	}

	// Sends a ping, which should time out and disconnect the connection.
	if conn.CheckConnection() {
		t.Errorf("connection.CheckConnection should have returned false")
	}
	if conn.IsConnected() {
		t.Errorf("connection.CheckConnection should have flagged the connection as not connected")
	}

	// Read and respond from the server intentionally late...
	redisReadBuffer := make([]byte, 1024)
	nRead, err := mockConn.Read(redisReadBuffer)
	if err != nil {
		t.Errorf("Error reading: %s", err)
	} else if !bytes.Equal(redisReadBuffer[:nRead], []byte("PING\r\n")) {
		t.Errorf("Did not receive expected ping from client. Received %q", redisReadBuffer[:nRead])
	}
	_, err = mockConn.Write([]byte("+PONG\r\n"))
	if err != nil {
		t.Errorf("Error writing PONG back to client: %s", err)
	}

	// Recycle the connection
	rmux.PrimaryConnectionPool.RecycleRemoteConnection(conn)

	// Get that same handler back since we're using a pool size of 1
	conn, err = rmux.PrimaryConnectionPool.GetConnection()
	if err != nil {
		t.Errorf("Error getting a connection: %s", err)
	} else if !conn.IsConnected() {
		t.Errorf("Got a disconnected client")
	}

	// Make sure there's nothing to read... The read timeout is 2 milliseconds, so this won't block
	clientReadBuffer := make([]byte, 1024)
	n, err := conn.Reader.Read(clientReadBuffer)
	if err == nil {
		t.Errorf("Read data when we should not have. Read: %q", clientReadBuffer[:n])
	}

	// And this time successfully respond to a ping
	go func() {
		mockRedis.SetDeadline(time.Now().Add(100 * time.Millisecond))
		conn, err := mockRedis.Accept()
		if err != nil {
			t.Errorf("Error when accepting connection: %s", err)
		}

		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := conn.Read(redisReadBuffer)
		if err != nil {
			t.Errorf("Error reading in redis: %s", err)
		} else if !bytes.Equal([]byte("PING\r\n"), redisReadBuffer[:n]) {
			t.Errorf("Expected +PING, got %q", redisReadBuffer[:n])
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			t.Errorf("Error writing to client from redis: %s", err)
		}
	}()

	if !conn.CheckConnection() {
		t.Errorf("Should have been able to establish that the connection is valid")
	}
}
