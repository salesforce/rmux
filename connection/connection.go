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
	. "github.com/forcedotcom/rmux/log"
	"github.com/forcedotcom/rmux/protocol"
	. "github.com/forcedotcom/rmux/writer"
	"net"
	"time"
)

//An outbound connection to a redis server
//Maintains its own underlying TimedNetReadWriter, and keeps track of its DatabaseId for select() changes
type Connection struct {
	connection net.Conn
	//The underlying ReadWriter for this connection
	Writer *FlexibleWriter
	//The database that we are currently connected to
	DatabaseId int
	//The connection wrapper for our net connection
	//	ConnectionReadWriter *protocol.TimedNetReadWriter
	Reader *bufio.Reader
}

//Initializes a new connection, of the given protocol and endpoint, with the given connection timeout
//ex: "unix", "/tmp/myAwesomeSocket", 50*time.Millisecond
func NewConnection(Protocol, Endpoint string, ConnectTimeout, ReadTimeout, WriteTimeout time.Duration) (newConnection *Connection) {
	remoteConnection, err := net.DialTimeout(Protocol, Endpoint, ConnectTimeout)
	if err != nil {
		Debug("NewConnection: Error received from dial: %s", err)
		return nil
	}
	newConnection = &Connection{}
	netReadWriter := protocol.NewTimedNetReadWriter(remoteConnection, ReadTimeout, WriteTimeout)
	newConnection.Writer = NewFlexibleWriter(netReadWriter)
	newConnection.DatabaseId = 0
	newConnection.Reader = bufio.NewReader(netReadWriter)
	return
}

//Selects the given database, for the connection
//If an error is returned, or if an invalid response is returned from the select, then this will return an error
//If not, the connections internal database will be updated accordingly
func (this *Connection) SelectDatabase(DatabaseId int) (err error) {
	err = protocol.WriteLine([]byte(fmt.Sprintf("select %d", DatabaseId)), this.Writer, true)
	if err != nil {
		Debug("SelectDatabase: Error received from protocol.FlushLine: %s", err)
		return err
	}

	if line, isPrefix, err := this.Reader.ReadLine(); err != nil || isPrefix || !bytes.Equal(line, protocol.OK_RESPONSE) {
		Debug("Could not successfully select db: err:%s isPrefix:%t readLine:%q", err, isPrefix, line)
		err = errors.New("Invalid select response")
		if this.connection != nil {
			this.connection.Close()
		}
		return err
	}

	this.DatabaseId = DatabaseId
	return
}

//Checks if the current connection is up or not
//If we do not get a response, or if we do not get a PONG reply, or if there is any error, returns false
func (myConnection *Connection) CheckConnection() bool {
	err := protocol.WriteLine(protocol.SHORT_PING_COMMAND, myConnection.Writer, true)
	if err != nil {
		Debug("CheckConnection: Error received from FlushLine: %s", err)
		return false
	}

	line, isPrefix, err := myConnection.Reader.ReadLine()
	if err != nil || isPrefix {
		Debug("CheckConnection: Could not ping: %q isPrefix: %t", err, isPrefix)
	}

	if err == nil && bytes.Equal(line, protocol.PONG_RESPONSE) {
		return true
	} else {
		return false
	}
}
