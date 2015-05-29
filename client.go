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
	"io"
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
	ConnectionReadWriter io.ReadWriter
	Connection           net.Conn
	//The Database that our client thinks we're connected to
	DatabaseId int
	//Whether or not this client connection is active or not
	//Upon QUIT command, this gets toggled off
	Active       bool
	ReadChannel  chan protocol.Command
	ErrorChannel chan error
	HashRing     *connection.HashRing
	queued       []protocol.Command
	Scanner *bufio.Scanner
}

var (
	ERR_NOTHING_TO_READ = errors.New("Nothing to read")
	ERR_QUIT            = errors.New("Client asked to quit")
	ERR_CONNECTION_DOWN = errors.New(string(CONNECTION_DOWN_RESPONSE))
)

var client_refuse_pile = make([]byte, 4096)

//Initializes a new client, for the given established net connection, with the specified read/write timeouts
func NewClient(connection net.Conn, readTimeout, writeTimeout time.Duration, isMuliplexing bool, hashRing *connection.HashRing) (newClient *Client) {
	newClient = &Client{}
	newClient.Connection = connection
	newClient.ReadWriter = bufio.NewReadWriter(bufio.NewReader(connection), bufio.NewWriter(connection))
	newClient.ConnectionReadWriter = newClient.ReadWriter
	//	newClient.ConnectionReadWriter = protocol.NewTimedNetReadWriter(connection, readTimeout, writeTimeout)
	//	newClient.ConnectionReadWriter = protocol.NewTimedNetReadWriter(connection, 300, 300)
	newClient.Active = true
	newClient.Multiplexing = isMuliplexing
	newClient.ReadChannel = make(chan protocol.Command, 2048) // TODO: Something sane or configurable, these things don't grow automatically
	newClient.ErrorChannel = make(chan error)
	newClient.queued = make([]protocol.Command, 0, 4)
	newClient.HashRing = hashRing
	newClient.DatabaseId = 0
	newClient.Scanner = protocol.NewRespScanner(connection)
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
				return nil, err
			} else {
				// Discard the rest on the read buffer.
				protocol.Debug("Error reading buffered command: %s", err.Error())
				this.DiscardReaderBytes()
				return nil, err
			}
		}

		buffered = append(buffered, command)
	}

	return buffered, nil
}

func (this *Client) HasAvailable() bool {
	// TODO Buffered doesn't really work here
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

func (this *Client) DiscardReaderBytes() {
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
func (this *Client) FlushRedisAndRespond() error {
	if !this.HasQueued() {
		return nil
	}

	start := time.Now()

	var connectionPool *connection.ConnectionPool
	if !this.Multiplexing {
		connectionPool = this.HashRing.DefaultConnectionPool
	} else {
//		connectionPool = this.HashRing.GetConnectionPool()
		// TODO - kind of complicated, can only do one command at a time
	}

	connStart := time.Now()
	redisConn := connectionPool.GetConnection()
	connEnd := time.Since(connStart)
	defer connectionPool.RecycleRemoteConnection(redisConn)

	if redisConn == nil {
		protocol.Debug("Failed to retrieve an active connection from the provided connection pool")
		this.ErrorChannel <- ERR_CONNECTION_DOWN
		return nil
	}

	if redisConn.DatabaseId != this.DatabaseId {
		if err := redisConn.SelectDatabase(this.DatabaseId); err != nil {
			protocol.Debug("Error while attempting to select database: %s", err)
		}
	}

	writeStart := time.Now()
	numCommands := len(this.queued)
	protocol.Debug("Writing %d commands to the redis server", numCommands)
	for _, command := range this.queued {
		redisConn.Write(command.GetBuffer())
	}
	this.resetQueued()
	redisConn.Writer.Flush()
	writeEnd := time.Since(writeStart)

	copyStart := time.Now()
	if err := protocol.CopyServerResponses(redisConn.Scanner, this.Writer, numCommands); err != nil {
		return err
	}
	copyEnd := time.Since(copyStart)

	this.Writer.Flush()

	protocol.Debug("all %s getConn %s write %s copyResponse %s", time.Since(start), connEnd, writeEnd, copyEnd)

	return nil
}

func (this *Client) HasBufferedOutput() bool {
	return this.Writer.Buffered() > 0
}

// Read loop for this client - moves commands and channels to the worker loop
func (this *Client) ReadLoop() {
	for this.Active && this.Scanner.Scan() {
		bytes := this.Scanner.Bytes()
		command, err := protocol.ParseCommand(bytes)
		if err != nil {
			this.ErrorChannel <- err
		} else {
			this.ReadChannel <- command
		}
	}

	if err := this.Scanner.Err(); err != nil {
		this.ErrorChannel <- err
	} else {
		this.ErrorChannel <- io.EOF
	}
}

func (this *Client) resetQueued() {
	// We make a new one instead of using this.queued=this.queued[:0] so that the command arrays are eligible for GC
	this.queued = make([]protocol.Command, 0, 4)
}

func (this *Client) HasQueued() bool {
	return len(this.queued) > 0
}

func (this *Client) Queue(command protocol.Command) {
	this.queued = append(this.queued, command)
}
