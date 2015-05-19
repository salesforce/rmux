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
	"errors"
	"github.com/forcedotcom/rmux/connection"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"time"
)

//Represents a redis client that is connected to our rmux server
type Client struct {
	//The underlying ReadWriter for this connection
	*bufio.ReadWriter
	//Whether or not this client needs to consider multiplexing
	Multiplexing bool
	//The connection wrapper for our net connection
	ConnectionReadWriter *protocol.TimedNetReadWriter
	//The Database that our client thinks we're connected to
	DatabaseId int
	//Whether or not this client connection is active or not
	//Upon QUIT command, this gets toggled off
	Active bool
}

var (
	ERR_NOTHING_TO_READ = errors.New("Nothing to read")
	ERR_QUIT            = errors.New("Client asked to quit")
)

//Initializes a new client, for the given established net connection, with the specified read/write timeouts
func NewClient(connection net.Conn, readTimeout, writeTimeout time.Duration, isMuliplexing bool) (newClient *Client) {
	newClient = &Client{}
	newClient.ConnectionReadWriter = protocol.NewTimedNetReadWriter(connection, readTimeout, writeTimeout)
	newClient.ReadWriter = bufio.NewReadWriter(bufio.NewReader(newClient.ConnectionReadWriter), bufio.NewWriter(newClient.ConnectionReadWriter))
	newClient.Active = true
	newClient.Multiplexing = isMuliplexing
	return
}

//Parses the given command
func (this *Client) ParseCommand(command protocol.Command) ([]byte, error) {
	//block all unsafe commands
	if !protocol.IsSupportedFunction(command.GetCommand(), this.Multiplexing, command.GetArgCount() > 2) {
		return nil, protocol.ERR_COMMAND_UNSUPPORTED
	}

	if bytes.Equal(command.GetCommand(), protocol.PING_COMMAND) {
		return protocol.PONG_RESPONSE, nil
	}

	if bytes.Equal(command.GetCommand(), protocol.QUIT_COMMAND) {
		return nil, ERR_QUIT
	}

	if bytes.Equal(command.GetCommand(), protocol.SELECT_COMMAND) {
		databaseId, err := protocol.ParseInt(command.GetFirstArg())
		if err != nil {
			return nil, protocol.ERR_BAD_ARGUMENTS
		}

		this.DatabaseId = databaseId
		return protocol.OK_RESPONSE, nil
	}

	return nil, nil
}

//Handles sending a single client request out across connectionPool, and copying the response back into our local buffer
func (this *Client) HandleRequest(activeConnection *connection.Connection, command protocol.Command, flush bool) (err error) {
	//If we aren't on the right DB, flip
	if this.DatabaseId != activeConnection.DatabaseId {
		err = activeConnection.SelectDatabase(this.DatabaseId)
		if err != nil {
			protocol.Debug("Error received while attempting to select database across remote connection: %s", err)
			return
		}
	}

	err = protocol.WriteCommand(command, activeConnection.Writer, flush)
	if err != nil {
		protocol.Debug("Error received when attempting to copy client request accross to remote server: %s", err)
		// TODO: Should return an error code to the client?
	}

	err = protocol.CopyServerResponse(activeConnection.Reader, this.Writer, flush)
	if err != nil {
		protocol.Debug("Error received when attempting to copy remote connection response back to local client: %s", err)
		return
	}

	return
}

// Reads commands on the input buffer. If something goes wrong reading, will return all successfully read until the error.
// Discards the rest on the input buffer in the case of an error condition.
// TODO Testing
func (this *Client) ReadBufferedCommands() (buffered []protocol.Command, err error) {
	buffered = []protocol.Command{}

	for this.Reader.Buffered() > 0 {
		var command protocol.Command

		// When multiplexing don't read the entire input buffer.
		if this.Multiplexing && len(buffered) > 0 {
			return
		}

		protocol.Debug("Doop")
		command, err = this.ReadBufferedCommand()
		if err != nil {
			// Discard the rest on the read buffer.
			protocol.Debug("Error reading buffered command: %s", err.Error())
			this.DiscardReaderBuffered()
			return
		}

		buffered = append(buffered, command)
	}

	return
}

func (this *Client) WriteError(err error, flush bool) error {
	return protocol.WriteError([]byte(err.Error()), this.Writer, flush)
}

func (this *Client) FlushError(err error) error {
	return this.WriteError(err, true)
}

func (this *Client) ReadBufferedCommand() (command protocol.Command, err error) {
	if this.Reader.Buffered() == 0 {
		return nil, ERR_NOTHING_TO_READ
	}

	command, err = protocol.ReadCommand(this.Reader)
	if err != nil {
		return nil, err
	}

	return command, nil
}

func (this *Client) DiscardReaderBuffered() {
	// At the time of this writing Reader.Discard only exists in golang unstable.
	// TODO: Benchmark using a sort of 'refuse pile' to read the full chunk at once
	buffered := this.Reader.Buffered()
	for i := 0; i < buffered; i++ {
		this.Reader.ReadByte()
	}
	return
}

func (this *Client) FlushLine(line []byte) (err error) {
	return protocol.WriteLine(line, this.Writer, true)
}

// Performs the query against the redis server and responds to the connected client with the response from redis.
func (this *Client) FlushRedisAndRespond(redisConn *connection.Connection) error {
	if redisConn == nil {
		return nil
	}

	// Flush any pending requests
	if redisConn.Writer.Buffered() > 0 {
		if err := redisConn.Writer.Flush(); err != nil {
			return err
		}
	}

	// Read the response
	for redisConn.Reader.Buffered() > 0 {
		if _, err := this.Writer.ReadFrom(redisConn.Reader); err != nil {
			return err
		}
	}

	return nil
}
