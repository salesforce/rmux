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
	"fmt"
	"github.com/forcedotcom/rmux/connection"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"time"
)

//Represents a redis client that is connected to our rmux server
type Client struct {
	//The underlying ReadWriter for this connection
	*bufio.ReadWriter
	//The connection wrapper for our net connection
	ConnectionReadWriter *protocol.TimedNetReadWriter
	//The current command that we're processing
	command []byte
	//The string representation of the current command that we're processing
	stringCommand string
	//The first argument for the current command that we're processing
	firstArgument []byte
	//The first argument for the current command that we're processing
	stringArgument string
	//The number of arguments supplied for the current command that we're processing'
	argumentCount int
	//The Database that our client thinks we're connected to
	DatabaseId int
	//Whether or not this client connection is active or not
	//Upon QUIT command, this gets toggled off
	Active bool
	//The current active connection
	ActiveConnection *connection.Connection
	//The current active subscription
	Subscriptions map[string]bool
}

var (
	//Error for unsupported (deemed unsafe for multiplexing) commands
	ERR_COMMAND_UNSUPPORTED = []byte("-ERR This command is not supported")
	//Error for when we receive bad arguments (for multiplexing) accompanying a command
	ERR_BAD_ARGUMENTS = []byte("-ERR Wrong number of arguments supplied for this command")
)

//Initializes a new client, for the given established net connection, with the specified read/write timeouts
func NewClient(connection net.Conn, readTimeout, writeTimeout time.Duration) (newClient *Client) {
	newClient = &Client{}
	newClient.ConnectionReadWriter = protocol.NewTimedNetReadWriter(connection, readTimeout, writeTimeout)
	newClient.ReadWriter = bufio.NewReadWriter(bufio.NewReader(newClient.ConnectionReadWriter), bufio.NewWriter(newClient.ConnectionReadWriter))
	newClient.Active = true
	newClient.Subscriptions = make(map[string]bool)
	return
}

//Parses the current command, starting with firstLine.
//isMultiplexing is supplied to let the client know if single-server-only commands should be supported or not
func (myClient *Client) ParseCommand(firstLine []byte, isMultiplexing bool) (responded bool, err error) {
	if bytes.Equal(firstLine, protocol.SHORT_PING_COMMAND) {
		err = protocol.FlushLine(protocol.PONG_RESPONSE, myClient.ReadWriter.Writer)
		return true, err
	}

	myClient.command, myClient.firstArgument, err = protocol.GetCommand(myClient.ReadWriter.Reader)
	if err != nil {
		protocol.Debug("Received error from GetCommand: %s\r\n", err)
		return false, err
	}
	protocol.Debug("Received %s %s\r\n", myClient.command, myClient.firstArgument)
	myClient.stringCommand = string(myClient.command)
	myClient.stringArgument = string(myClient.firstArgument)

	//PINGs and QUITs should auto-return
	if bytes.Equal(myClient.command, protocol.PING_COMMAND) {
		protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		err = protocol.FlushLine(protocol.PONG_RESPONSE, myClient.ReadWriter.Writer)
		return true, err
	} else if bytes.Equal(myClient.command, protocol.QUIT_COMMAND) {
		//Disable ourselves, if this is a QUIT command.  The server managing this client is responsible for checking this flag for cleanup
		myClient.Active = false
		protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		err = protocol.FlushLine(protocol.OK_RESPONSE, myClient.ReadWriter.Writer)
		return true, err
	}

	myClient.argumentCount, err = protocol.ParseInt(firstLine[1:])
	if err != nil {
		return
	}

	//block all unsafe commands
	if protocol.UNSAFE_FUNCTIONS[myClient.stringCommand] {
		protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		err = protocol.FlushLine(ERR_COMMAND_UNSUPPORTED, myClient.ReadWriter.Writer)
		return true, err
		//block all pubsub commands
	} else if len(myClient.Subscriptions) > 0 && !protocol.PUBSUB_FUNCTIONS[myClient.stringCommand] {
		protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		err = protocol.FlushLine(ERR_COMMAND_UNSUPPORTED, myClient.ReadWriter.Writer)
		return true, err
	} else if isMultiplexing {
		if protocol.SINGLE_DB_FUNCTIONS[myClient.stringCommand] {
			protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
			err = protocol.FlushLine(ERR_COMMAND_UNSUPPORTED, myClient.ReadWriter.Writer)
			return true, err
			//delete commands can only work on one key at a time, if we're multiplexing
		} else if bytes.Equal(myClient.command, protocol.DEL_COMMAND) && myClient.argumentCount != 2 {
			protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
			err = protocol.FlushLine(ERR_BAD_ARGUMENTS, myClient.ReadWriter.Writer)
			return true, err
			//subscribe commands can only work on one key at a time, if we're multiplexing
		} else if bytes.Equal(myClient.command, protocol.SUBSCRIBE_COMMAND) && myClient.argumentCount != 2 {
			protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
			err = protocol.FlushLine(ERR_BAD_ARGUMENTS, myClient.ReadWriter.Writer)
			return true, err
		}
	}

	//If we have a select command, fake it and return
	if bytes.Equal(myClient.command, protocol.SELECT_COMMAND) {
		protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		//If we have an accompanying argument for our select..
		if len(myClient.firstArgument) > 0 {
			//Find what database we want to be selecting
			myClient.DatabaseId, err = protocol.ParseInt(myClient.firstArgument)
			if err == nil {
				//We've stored the DB into our client, that's enough.  Strip out the remaining message and return
				err = protocol.FlushLine(protocol.OK_RESPONSE, myClient.ReadWriter.Writer)
			} else {
				err = protocol.FlushLine(ERR_BAD_ARGUMENTS, myClient.ReadWriter.Writer)
			}
		} else {
			err = protocol.FlushLine(ERR_BAD_ARGUMENTS, myClient.ReadWriter.Writer)
		}
		return true, err
	} else if bytes.Equal(myClient.command, protocol.SUBSCRIBE_COMMAND) {
		myClient.Subscriptions[myClient.stringArgument] = true
	} else if bytes.Equal(myClient.command, protocol.UNSUBSCRIBE_COMMAND) {
		delete(myClient.Subscriptions, myClient.stringArgument)
	}
	return false, err
}

//Sends a subscription success response, to the current client
func (myClient *Client) SendSubscriptionResponse() (err error) {
	tmpSlice := []byte(fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d", len(myClient.stringArgument), myClient.stringArgument, len(myClient.Subscriptions)))
	err = protocol.FlushLine(tmpSlice, myClient.ReadWriter.Writer)
	return
}

//Handles sending a single client request out across connectionPool, and copying the response back into our local buffer
func (myClient *Client) HandleRequest(connectionPool *connection.ConnectionPool, firstLine []byte) (err error) {
	myClient.ActiveConnection = connectionPool.GetConnection()
	//If we don't have a connection, something went horribly wrong
	if myClient.ActiveConnection == nil {
		protocol.Debug("Failed to retrieve an active connection from the provided connection pool\r\n")
		err = protocol.FlushLine(CONNECTION_DOWN_RESPONSE, myClient.ReadWriter.Writer)
		if err != nil {
			return
		}
		err = protocol.IgnoreMultiBulkMessage(firstLine, myClient.ReadWriter.Reader)
		if err != nil {
			return
		}
		return
	}

	//If we aren't on the right DB, flip
	if myClient.DatabaseId != myClient.ActiveConnection.DatabaseId {
		err = myClient.ActiveConnection.SelectDatabase(myClient.DatabaseId)
		if err != nil {
			protocol.Debug("Error received while attempting to select database across remote connection: %s\r\n", err)
			return
		}
	}

	startTime := time.Now()
	err = protocol.CopyMultiBulkMessage(firstLine, myClient.ActiveConnection.ReadWriter.Writer, myClient.ReadWriter.Reader)
	if err != nil {
		protocol.Debug("Error received when attempting to copy client request accross to remote server: %s\r\n", err)
	}
	protocol.Debug("Write to server time: %s\r\n", time.Since(startTime))

	startTime = time.Now()

	err = protocol.CopyServerResponse(myClient.ActiveConnection.ReadWriter.Reader, myClient.ReadWriter.Writer)
	if err != nil {
		protocol.Debug("Error received when attempting to copy remote connection response back to local client: %s\r\n", err)
	}

	protocol.Debug("Read from server time: %s\r\n", time.Since(startTime))
	connectionPool.RecycleRemoteConnection(myClient.ActiveConnection)
	return
}
