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
	"io"
)

//Represents a redis client that is connected to our rmux server
type Client struct {
	//The underlying ReadWriter for this connection
	*bufio.ReadWriter
	//Whether or not this client needs to consider multiplexing
	Multiplexing bool
	//The connection wrapper for our net connection
	ConnectionReadWriter io.ReadWriter
	Connection net.Conn
	//The Database that our client thinks we're connected to
	DatabaseId int
	//Whether or not this client connection is active or not
	//Upon QUIT command, this gets toggled off
	Active bool
	ReadChannel chan []protocol.Command
	ErrorChannel chan error
}

var (
	ERR_NOTHING_TO_READ = errors.New("Nothing to read")
	ERR_QUIT            = errors.New("Client asked to quit")
)

var client_refuse_pile = make([]byte, 4096)

//Initializes a new client, for the given established net connection, with the specified read/write timeouts
func NewClient(connection net.Conn, readTimeout, writeTimeout time.Duration, isMuliplexing bool) (newClient *Client) {
	newClient = &Client{}
	newClient.Connection = connection
	newClient.ReadWriter = bufio.NewReadWriter(bufio.NewReader(connection), bufio.NewWriter(connection))
	newClient.ConnectionReadWriter = newClient.ReadWriter
	newClient.Active = true
	newClient.Multiplexing = isMuliplexing
	newClient.ReadChannel = make(chan []protocol.Command, 2048) // TODO: Something sane, these things don't grow automatically... or configurable
	newClient.ErrorChannel = make(chan error)
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

	for first := true; first || this.HasAvailable(); first = false { // fake do while loop
		var command protocol.Command

		// When multiplexing don't read the entire input buffer.
		if this.Multiplexing && !first {
			return buffered, nil
		}

		command, err = this.ReadBufferedCommand()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				//We had a read timeout. Return what we have.
				return buffered, nil
			} else if err == io.EOF {
				protocol.Debug("EOF!")
				return nil, err
			} else {
				// Discard the rest on the read buffer.
				protocol.Debug("Error reading buffered command: %s", err.Error())
				this.DiscardReaderBuffered()
				return nil, err
			}
		}

		buffered = append(buffered, command)
	}

	return buffered, nil
}

func (this *Client) HasAvailable() bool {
	return this.Reader.Buffered() > 0
}

func (this *Client) WriteError(err error, flush bool) error {
	return protocol.WriteError([]byte(err.Error()), this.Writer, flush)
}

func (this *Client) FlushError(err error) error {
	return this.WriteError(err, true)
}

//Reads a buffered command.
func (this *Client) ReadBufferedCommand() (command protocol.Command, err error) {
	command, err = protocol.ReadCommand(this.Reader)
	if err != nil {
		protocol.Debug("Error in ReadBufferedCommand: %s", err.Error())
		return nil, err
	}

	return command, nil
}

func (this *Client) DiscardReaderBuffered() {
	// At the time of this writing Reader.Discard only exists in golang unstable.
	for this.HasAvailable() {
		this.Reader.Read(client_refuse_pile)
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

	if redisConn.Writer.Buffered() == 0 {
		return nil
	}

	// Flush any pending requests
	if err := redisConn.Writer.Flush(); err != nil {
		return err
	}

	// Read the response TODO ensure as many commands as sent were responded to
	if _, err := this.Writer.ReadFrom(redisConn.Reader); err != nil {
		return err
	}

	return nil
}

func (this *Client) HasBufferedOutput() bool {
	return this.Writer.Buffered() > 0
}

// Read loop for this client - moves commands and channels to the worker loop
func (this *Client) ReadLoop() {
	// TODO: This needs to stop when the client dies is deactivated.
	for this.Active {
		// Read any commands
		commands, err := this.ReadBufferedCommands()
		if err != nil {
			this.ErrorChannel<- err
		} else if len(commands) > 0 {
			this.ReadChannel<- commands
		}
	}
}
