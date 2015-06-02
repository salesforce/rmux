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
	. "github.com/forcedotcom/rmux/writer"
	"io"
	"net"
	"time"
)

type readItem struct {
	command protocol.Command
	err     error
}

//Represents a redis client that is connected to our rmux server
type Client struct {
	//The underlying ReadWriter for this connection
	Writer *FlexibleWriter
	//Whether or not this client needs to consider multiplexing
	Multiplexing bool
	Connection   net.Conn
	//The Database that our client thinks we're connected to
	DatabaseId int
	//Whether or not this client connection is active or not
	//Upon QUIT command, this gets toggled off
	Active      bool
	ReadChannel chan readItem
	HashRing    *connection.HashRing
	queued      []protocol.Command
	Scanner     *bufio.Scanner
}

var (
	ERR_QUIT            = errors.New("Client asked to quit")
	ERR_CONNECTION_DOWN = errors.New(string(CONNECTION_DOWN_RESPONSE))
)

//Initializes a new client, for the given established net connection, with the specified read/write timeouts
func NewClient(connection net.Conn, readTimeout, writeTimeout time.Duration, isMuliplexing bool, hashRing *connection.HashRing) (newClient *Client) {
	newClient = &Client{}
	newClient.Connection = connection
	newClient.Writer = NewFlexibleWriter(connection)
	newClient.Active = true
	newClient.Multiplexing = isMuliplexing
	newClient.ReadChannel = make(chan readItem, 10000)
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

func (this *Client) WriteError(err error, flush bool) error {
	return protocol.WriteError([]byte(err.Error()), this.Writer, flush)
}

func (this *Client) FlushError(err error) error {
	return this.WriteError(err, true)
}

func (this *Client) WriteLine(line []byte) (err error) {
	return protocol.WriteLine(line, this.Writer, false)
}

func (this *Client) FlushLine(line []byte) (err error) {
	return protocol.WriteLine(line, this.Writer, true)
}

// Performs the query against the redis server and responds to the connected client with the response from redis.
func (this *Client) FlushRedisAndRespond() error {
	if !this.HasQueued() {
		this.Writer.Flush()
		return nil
	}

	start := time.Now()

	var connectionPool *connection.ConnectionPool
	if !this.Multiplexing {
		connectionPool = this.HashRing.DefaultConnectionPool
	} else {
		if len(this.queued) != 1 {
			panic("Should not have multiple commands to flush when multiplexing")
		}
		connectionPool = this.HashRing.GetConnectionPool(this.queued[0])
	}

	connStart := time.Now()
	redisConn := connectionPool.GetConnection()
	connEnd := time.Since(connStart)
	defer connectionPool.RecycleRemoteConnection(redisConn)

	if redisConn == nil {
		protocol.Debug("Failed to retrieve an active connection from the provided connection pool")
		this.ReadChannel <- readItem{nil, ERR_CONNECTION_DOWN}
		return nil
	}

	if redisConn.DatabaseId != this.DatabaseId {
		if err := redisConn.SelectDatabase(this.DatabaseId); err != nil {
			protocol.Debug("Error while attempting to select database: %s", err)
			return err
		}
	}

	writeStart := time.Now()
	numCommands := len(this.queued)
	protocol.Debug("Writing %d commands to the redis server", numCommands)
	for _, command := range this.queued {
		redisConn.Writer.Write(command.GetBuffer())
	}
	this.resetQueued()
	for redisConn.Writer.Buffered() > 0 {
		redisConn.Writer.Flush()
	}
	writeEnd := time.Since(writeStart)

	copyStart := time.Now()
	if err := protocol.CopyServerResponses(redisConn.Reader, this.Writer, numCommands); err != nil {
		protocol.Debug("Error copying server responses: %s", err)
		this.ReadChannel <- readItem{nil, err}
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
func (this *Client) ReadLoop(rmux *RedisMultiplexer) {
	for rmux.active && this.Active && this.Scanner.Scan() {
		bytes := this.Scanner.Bytes()
		command, err := protocol.ParseCommand(bytes)
		this.ReadChannel <- readItem{command, err}
	}

	if err := this.Scanner.Err(); err != nil {
		this.ReadChannel <- readItem{nil, err}
	} else {
		this.ReadChannel <- readItem{nil, io.EOF}
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
