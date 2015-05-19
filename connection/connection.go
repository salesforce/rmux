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
	"bytes"
	"errors"
	"fmt"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"time"
)

//An outbound connection to a redis server
//Maintains its own underlying TimedNetReadWriter, and keeps track of its DatabaseId for select() changes
type Connection struct {
	//The underlying ReadWriter for this connection
	*bufio.ReadWriter
	//The database that we are currently connected to
	DatabaseId int
	//The connection wrapper for our net connection
	ConnectionReadWriter *protocol.TimedNetReadWriter
}

//Initializes a new connection, of the given protocol and endpoint, with the given connection timeout
//ex: "unix", "/tmp/myAwesomeSocket", 50*time.Millisecond
func NewConnection(Protocol, Endpoint string, ConnectTimeout, ReadTimeout, WriteTimeout time.Duration) (newConnection *Connection) {
	remoteConnection, err := net.DialTimeout(Protocol, Endpoint, ConnectTimeout)
	if err != nil {
		protocol.Debug("NewConnection: Error received from dial: %s", err)
		return nil
	}
	newConnection = &Connection{}
	newConnection.ConnectionReadWriter = protocol.NewTimedNetReadWriter(remoteConnection, ReadTimeout, WriteTimeout)
	newConnection.ReadWriter = bufio.NewReadWriter(bufio.NewReader(newConnection.ConnectionReadWriter), bufio.NewWriter(newConnection.ConnectionReadWriter))
	newConnection.DatabaseId = 0
	return
}

//Selects the given database, for the connection
//If an error is returned, or if an invalid response is returned from the select, then this will return an error
//If not, the connections internal database will be updated accordingly
func (myConnection *Connection) SelectDatabase(DatabaseId int) (err error) {
	err = protocol.WriteLine([]byte(fmt.Sprintf("select %d", DatabaseId)), myConnection.ReadWriter.Writer, true)
	if err != nil {
		protocol.Debug("SelectDatabase: Error received from protocol.FlushLine: %s", err)
		return
	}
	buf, _, err := myConnection.ReadWriter.ReadLine()
	if err != nil {
		protocol.Debug("SelectDatabase: Error received from ReadLine: %s", err)
		return
	}

	if !bytes.Equal(buf, protocol.OK_RESPONSE) {
		protocol.Debug("SelectDatabase: Invalid response for select: %s", buf)
		err = errors.New("Invalid select response")
	}

	myConnection.DatabaseId = DatabaseId
	return
}

//Checks if the current connection is up or not
//If we do not get a response, or if we do not get a PONG reply, or if there is any error, returns false
func (myConnection *Connection) CheckConnection() bool {
	err := protocol.WriteLine(protocol.SHORT_PING_COMMAND, myConnection.Writer, true)
	if err != nil {
		protocol.Debug("CheckConnection: Error received from FlushLine: %s", err)
		return false
	}

	buf, _, err := myConnection.ReadWriter.ReadLine()
	if err != nil {
		protocol.Debug("CheckConnection: Error received from ReadLine: %s", err)
		return false
	}

	if err == nil && bytes.Equal(buf, protocol.PONG_RESPONSE) {
		return true
	} else {
		return false
	}
}
