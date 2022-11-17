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
	"fmt"
	"net"
	"rmux/writer"
	"testing"
	"time"
)

func verifySelectDatabaseSuccess(test *testing.T, database int) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer listenSock.Close()
	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	testConnection.ReconnectIfNecessary()

	//read buffer does't matter
	readBuf := bufio.NewReader(bytes.NewBufferString("+OK\r\n"))
	//write buffer will be used for verification
	w := new(bytes.Buffer)
	w.Reset()
	testConnection.Reader = readBuf
	testConnection.Writer = writer.NewFlexibleWriter(w)

	// Do the select
	if err := testConnection.SelectDatabase(database); err != nil {
		test.Fatalf("Error when selecting database: %s", err)
	}

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if !bytes.Equal(expectedWrite, w.Bytes()) {
		test.Fatalf("Select statement was not written to output buffer got:%q expected:%q", w.Bytes(), expectedWrite)
	}

	if err != nil {
		test.Fatal("Database select failed")
	}
}

func verifySelectDatabaseError(test *testing.T, database int) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()
	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	testConnection.ReconnectIfNecessary()
	//read buffer does't matter
	readBuf := bufio.NewReader(bytes.NewBufferString("+NOPE\r\n"))
	//write buffer will be used for verification
	w := new(bytes.Buffer)
	w.Reset()
	testConnection.Reader = readBuf
	testConnection.Writer = writer.NewFlexibleWriter(w)
	err = testConnection.SelectDatabase(database)

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if !bytes.Equal(expectedWrite, w.Bytes()) {
		test.Fatal("Select statement was not written to output buffer", w.Bytes(), expectedWrite)
	}

	if err == nil {
		test.Fatal("Database select did not fail, even though bad response code was given")
	}
}

func verifySelectDatabaseTimeout(test *testing.T, database int) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer listenSock.Close()

	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	if err := testConnection.ReconnectIfNecessary(); err != nil {
		test.Fatalf("Could not connect to testSocket %s: %s", testSocket, err)
	}

	//write buffer will be used for verification
	w := new(bytes.Buffer)
	//Make a small buffer, just to confirm occasional flushes
	testConnection.Writer = writer.NewFlexibleWriter(w)
	err = testConnection.SelectDatabase(database)

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if !bytes.Equal(expectedWrite, w.Bytes()) {
		test.Fatalf("Select statement was not written to output buffer Got(%q) Expected(%q)", w.Bytes(), expectedWrite)
	}

	if err == nil {
		test.Fatal("Database select did not fail, even though there was no response")
	}
}

func TestSelectDatabase(test *testing.T) {
	verifySelectDatabaseSuccess(test, 0)
	verifySelectDatabaseSuccess(test, 1)
	verifySelectDatabaseSuccess(test, 123)

	verifySelectDatabaseError(test, 0)
	verifySelectDatabaseError(test, 1)
	verifySelectDatabaseError(test, 123)

	verifySelectDatabaseTimeout(test, 0)
	verifySelectDatabaseTimeout(test, 1)
	verifySelectDatabaseTimeout(test, 123)
}

func TestNewUnixConnection(test *testing.T) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer listenSock.Close()

	connection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	connection.ReconnectIfNecessary()
	if connection == nil || connection.connection == nil {
		test.Fatal("Connection initialization returned nil, binding to unix endpoint failed")
	}

	connection = NewConnection("unix", "/tmp/thisdoesnotexist", 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	connection.ReconnectIfNecessary()
	if connection != nil && connection.connection != nil {
		test.Fatal("Connection initialization success, binding to fake unix endpoint succeeded????")
	}
}

func TestNewTcpConnection(test *testing.T) {
	testEndpoint := "localhost:8886"
	listenSock, err := net.Listen("tcp", testEndpoint)
	if err != nil {
		test.Fatalf("Error listening on tcp sock %s. Error: %s", testEndpoint, err)
	}
	defer listenSock.Close()

	connection := NewConnection("tcp", testEndpoint, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	connection.ReconnectIfNecessary()
	if connection == nil || connection.connection == nil {
		test.Fatal("Connection initialization returned nil, binding to tcp endpoint failed")
	}

	//reserved sock should have nothing on it
	connection = NewConnection("tcp", "localhost:49151", 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, "", "")
	connection.ReconnectIfNecessary()
	if connection != nil && connection.connection != nil {
		test.Fatal("Connection initialization success, binding to fake tcp endpoint succeeded????")
	}
}

func TestCheckConnection(test *testing.T) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()

	connection := NewConnection("unix", testSocket, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, "", "")
	connection.ReconnectIfNecessary()
	if connection == nil {
		test.Fatal("Connection initialization returned nil, binding to unix endpoint failed")
	}

	fd, err := listenSock.Accept()
	if err != nil {
		test.Fatal("Failed to accept connection")
	}

	// Buffering responses, one valid, one not
	if _, err := fd.Write([]byte("+PONG\r\nPONG")); err != nil {
		test.Fatalf("Failed to write to buffer: %s", err)
	}

	if !connection.CheckConnection() {
		test.Fatal("Valid connection's check connection failed")
	}

	if connection.CheckConnection() {
		test.Fatal("Invalid connection's check connection succeeded")
	}

	if connection.CheckConnection() {
		test.Fatal("Timing-out connection's check connection succeeded")
	}
}
