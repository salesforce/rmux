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
	"fmt"
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"testing"
	"time"
)

func verifySelectDatabaseSuccess(test *testing.T, database int) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()
	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	//read buffer does't matter
	readBuf := bufio.NewReader(bytes.NewBufferString("+OK\r\n"))
	//write buffer will be used for verification
	w := new(bytes.Buffer)
	w.Reset()
	testConnection.Scanner = protocol.NewRespScanner(readBuf)
	err = testConnection.SelectDatabase(database)

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if bytes.Equal(expectedWrite, w.Bytes()) {
		test.Log("Select statement was properly written to output buffer")
	} else {
		test.Fatal("Select statement was not written to output buffer", w.Bytes(), expectedWrite)
	}

	if err == nil {
		test.Log("Database select did not fail")
	} else {
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
	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	//read buffer does't matter
	readBuf := bufio.NewReader(bytes.NewBufferString("+NOPE\r\n"))
	//write buffer will be used for verification
	w := new(bytes.Buffer)
	w.Reset()
	testConnection.Scanner = protocol.NewRespScanner(readBuf)
	err = testConnection.SelectDatabase(database)

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if bytes.Equal(expectedWrite, w.Bytes()) {
		test.Log("Select statement was properly written to output buffer")
	} else {
		test.Fatal("Select statement was not written to output buffer", w.Bytes(), expectedWrite)
	}

	if err == nil {
		test.Fatal("Database select did not fail, even though bad response code was given")
	} else {
		test.Log("Database select failed")
	}
}

func verifySelectDatabaseTimeout(test *testing.T, database int) {
	testSocket := "/tmp/rmuxConnectionTest"
	listenSock, err := net.Listen("unix", testSocket)
	if err != nil {
		test.Fatal("Failed to listen on test socket ", testSocket)
	}
	defer func() {
		listenSock.Close()
	}()
	testConnection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)

	//write buffer will be used for verification
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	buf := bufio.NewWriterSize(w, 38)
	testConnection.Writer = buf
	err = testConnection.SelectDatabase(database)

	expectedWrite := []byte(fmt.Sprintf("select %d\r\n", database))
	if bytes.Equal(expectedWrite, w.Bytes()) {
		test.Log("Select statement was properly written to output buffer")
	} else {
		test.Fatal("Select statement was not written to output buffer", w.Bytes(), expectedWrite)
	}

	if err == nil {
		test.Fatal("Database select did not fail, even though there was no response")
	} else {
		test.Log("Database select timed out successfully")
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
	defer func() {
		listenSock.Close()
	}()
	connection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	if connection == nil {
		test.Fatal("Connection initialization returned nil, binding to unix endpoint failed")
	} else {
		test.Log("Connection initialization success, binding to unix endpoint succeeded")
	}

	connection = NewConnection("unix", "/tmp/thisdoesnotexist", 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	if connection == nil {
		test.Log("Connection initialization returned nil, binding to fake unix endpoint failed")
	} else {
		test.Fatal("Connection initialization success, binding to fake unix endpoint succeeded????")
	}
}

func TestNewTcpConnection(test *testing.T) {
	testEndpoint := "localhost:6379"
	listenSock, err := net.Listen("tcp", testEndpoint)
	if err != nil {
		test.Log("Failed to listen on test socket ", testEndpoint, "maybe we're testing against a real readis connection")
	}
	defer func() {
		listenSock.Close()
	}()
	connection := NewConnection("tcp", testEndpoint, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	if connection == nil {
		test.Fatal("Connection initialization returned nil, binding to tcp endpoint failed")
	} else {
		test.Log("Connection initialization success, binding to tcp endpoint succeeded")
	}

	//reserved sock should have nothing on it
	connection = NewConnection("tcp", "localhost:49151", 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	if connection == nil {
		test.Log("Connection initialization returned nil, binding to fake tcp endpoint failed")
	} else {
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

	connection := NewConnection("unix", testSocket, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)

	fd, err := listenSock.Accept()
	if err != nil {
		test.Fatal("Failed to accept connection")
	} else {
		localBuffer := bufio.NewReadWriter(bufio.NewReader(fd), bufio.NewWriter(fd))
		test.Log("Shoving a +PONG response in the buffer for testing")
		protocol.WriteLine(protocol.PONG_RESPONSE, localBuffer.Writer, true)
		test.Log("Shoving a PONG response (no plus sign, so invalid) in the buffer for failure testing")
		protocol.WriteLine([]byte{'P', 'O', 'N', 'G'}, localBuffer.Writer, true)
	}

	if connection == nil {
		test.Fatal("Connection initialization returned nil, binding to unix endpoint failed")
	} else {
		test.Log("Connection initialization success, binding to unix endpoint succeeded")
	}

	if connection.CheckConnection() {
		test.Log("Valid connection's check connection succeeded")
	} else {
		test.Fatal("Valid connection's checkheck connection failed")
	}

	if connection.CheckConnection() {
		test.Fatal("Invalid connection's checkheck connection succeeded")
	} else {
		test.Log("Invalid connection's checkheck connection failed")
	}

	if connection.CheckConnection() {
		test.Fatal("Timeing-out connection's checkheck connection succeeded")
	} else {
		test.Log("Timeing-out connection's checkheck connection failed")
	}
}
