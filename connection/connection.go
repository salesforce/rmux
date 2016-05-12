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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	. "github.com/SalesforceEng/rmux/log"
	"github.com/SalesforceEng/rmux/protocol"
	. "github.com/SalesforceEng/rmux/writer"
	"net"
	"time"
	"github.com/SalesforceEng/rmux/graphite"
)

//An outbound connection to a redis server
//Maintains its own underlying TimedNetReadWriter, and keeps track of its DatabaseId for select() changes
type Connection struct {
	connection net.Conn
	//The database that we are currently connected to
	DatabaseId int
	// The reader from the redis server
	Reader *bufio.Reader
	// The writer to the redis server
	Writer *FlexibleWriter

	protocol string
	endpoint string
	connectTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
}

//Initializes a new connection, of the given protocol and endpoint, with the given connection timeout
//ex: "unix", "/tmp/myAwesomeSocket", 50*time.Millisecond
func NewConnection(Protocol, Endpoint string, ConnectTimeout, ReadTimeout, WriteTimeout time.Duration) *Connection {
	c := &Connection{}
	c.protocol = Protocol
	c.endpoint = Endpoint
	c.connectTimeout = ConnectTimeout
	c.readTimeout = ReadTimeout
	c.writeTimeout = WriteTimeout
	return c
}

func (c *Connection) Disconnect() {
	if c.connection != nil {
		c.connection.Close()
		Info("Disconnected a connection")
		graphite.Increment("disconnect")
	}
	c.connection = nil
	c.DatabaseId = 0
	c.Reader = nil
	c.Writer = nil
}

func (c *Connection) ReconnectIfNecessary() (err error) {
	if c.IsConnected() {
		return nil
	}

	// If it's not connected, manually disconnect the connection for sanity's sake
	c.Disconnect()

	c.connection, err = net.DialTimeout(c.protocol, c.endpoint, c.connectTimeout)
	if err != nil {
		Error("NewConnection: Error received from dial: %s", err)
		c.connection = nil
		return err
	}

	netReadWriter := protocol.NewTimedNetReadWriter(c.connection, c.readTimeout, c.writeTimeout)
	c.DatabaseId = 0
	c.Writer = NewFlexibleWriter(netReadWriter)
	c.Reader = bufio.NewReader(netReadWriter)

	return nil
}

//Selects the given database, for the connection
//If an error is returned, or if an invalid response is returned from the select, then this will return an error
//If not, the connections internal database will be updated accordingly
func (this *Connection) SelectDatabase(DatabaseId int) (err error) {
	if this.connection == nil {
		Error("SelectDatabase: Selecting on invalid connection")
		return errors.New("Selecting database on an invalid connection")
	}

	err = protocol.WriteLine([]byte(fmt.Sprintf("select %d", DatabaseId)), this.Writer, true)
	if err != nil {
		Error("SelectDatabase: Error received from protocol.FlushLine: %s", err)
		return err
	}

	if line, isPrefix, err := this.Reader.ReadLine(); err != nil || isPrefix || !bytes.Equal(line, protocol.OK_RESPONSE) {
		Error("SelectDatabase: Error while attempting to select database. Err:%q Response:%q isPrefix:%q", err, line, isPrefix)
		this.Disconnect()
		return errors.New("Invalid select response")
	}

	this.DatabaseId = DatabaseId
	return
}

//Checks if the current connection is up or not
//If we do not get a response, or if we do not get a PONG reply, or if there is any error, returns false
func (myConnection *Connection) CheckConnection() bool {
	if myConnection.connection == nil {
		return false
	}

	//start := time.Now()
	//defer func() {
	//	graphite.Timing("check_connection", time.Now().Sub(start))
	//}()

	startWrite := time.Now()
	err := protocol.WriteLine(protocol.SHORT_PING_COMMAND, myConnection.Writer, true)
	if err != nil {
		Error("CheckConnection: Could not write PING Err:%s Timing:%s", err, time.Now().Sub(startWrite))
		myConnection.Disconnect()
		return false
	}

	startRead := time.Now()
	line, isPrefix, err := myConnection.Reader.ReadLine()

	if err == nil && !isPrefix && bytes.Equal(line, protocol.PONG_RESPONSE) {
		return true
	} else {
		if err != nil {
			Error("CheckConnection: Could not read PING. Error: %s Timing:%s", err, time.Now().Sub(startRead))
		} else if isPrefix {
			Error("CheckConnection: ReadLine returned prefix: %q", line)
		} else {
			Error("CheckConnection: Expected PONG response. Got: %q", line)
		}
		myConnection.Disconnect()
		return false
	}
}

func (c *Connection) IsConnected() bool {
	if c.connection == nil {
		return false
	}

	// Adds a hundredth a milli...
	c.connection.SetReadDeadline(time.Now().Add(time.Microsecond * 10))
	var b [4]byte
	n, err := c.connection.Read(b[:])

	if err != nil {
		if err, ok := err.(net.Error); ok {
			if err.Timeout() {
				return true
			}
		}

		Info("There was an error when checking the connection (%s), will reconnect the connection", err)
		return false
	}

	if n != 0 {
		Warn("Got %d bytes back when we expected 0.", n)
	}

	return true
}
