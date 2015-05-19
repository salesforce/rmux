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
	"github.com/forcedotcom/rmux/protocol"
	"net"
	"testing"
	"time"
)

func TestReadCommand(t *testing.T) {
	testData := []struct {
		input    string
		command  string
		argCount int
		arg1     string
	}{
		{"ping", "ping", 0, ""},
		{"PING", "ping", 0, ""},
		{"+ping", "ping", 0, ""},
		{"select 1", "select", 1, "1"},
		{"*1\r\n$4\r\nping\r\n", "ping", 0, ""},
		{"*2\r\n$6\r\nselect\r\n$1\r\n1\r\n", "select", 1, "1"},
		{"*2\r\n$6\r\nselect\r\n$1\r\na\r\n", "select", 1, "a"},
		{"*5\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$4\r\nkey4\r\n", "del", 4, "key1"},
	}

	listenSock, err := net.Listen("unix", "/tmp/rmuxTest1.sock")
	if err != nil {
		t.Fatal("Cannot listen on /tmp/rmuxTest1.sock: ", err)
	}
	defer listenSock.Close()
	testConnection, err := net.DialTimeout("unix", "/tmp/rmuxTest1.sock", 1*time.Second)
	if err != nil {
		t.Fatal("Could not dial in to our local rmux sock")
	}
	defer testConnection.Close()
	client := NewClient(testConnection, 1*time.Millisecond, 1*time.Millisecond, true)

	for _, data := range testData {
		input := []byte(data.input)

		client.ReadWriter.Writer = bufio.NewWriterSize(bytes.NewBuffer([]byte("")), 38)
		client.ReadWriter.Reader = bufio.NewReader(bytes.NewBuffer(input))

		command, err := protocol.ReadCommand(client.Reader)

		if err != nil {
			t.Errorf("Error when reading input %q: %s", data.input, err)
			continue
		}

		if bytes.Compare(command.GetCommand(), []byte(data.command)) != 0 {
			t.Errorf("Should have parsed command %s, got %q", data.command, command.GetCommand())
		}

		if command.GetArgCount() != data.argCount {
			t.Errorf("Should have parsed %d arguments, got %d", data.argCount, command.GetArgCount())
		}

		if bytes.Compare(command.GetFirstArg(), []byte(data.arg1)) != 0 {
			t.Errorf("Should have first arg %q, got %q", data.arg1, command.GetFirstArg())
		}
	}
}

func TestParseCommand(test *testing.T) {
	testCases := []struct {
		input             []byte
		immediateResponse []byte
		err               error
	}{
		//should accept inline format
		{protocol.SHORT_PING_COMMAND, protocol.PONG_RESPONSE, nil},
		//should accept simple format
		{protocol.PING_COMMAND, protocol.PONG_RESPONSE, nil},
		//should accept multibulk format
		{[]byte("*1\r\n$4\r\nping\r\n"), protocol.PONG_RESPONSE, nil},
		//quit in proper format should respond appropriately
		{[]byte("*1\r\n$4\r\nquit\r\n"), nil, ERR_QUIT},
		//select without database should err
		{[]byte("*1\r\n$6\r\nselect\r\n"), nil, protocol.ERR_BAD_ARGUMENTS},
		//select in proper format should respond appropriately
		{[]byte("*2\r\n$6\r\nselect\r\n$1\r\n1\r\n"), protocol.OK_RESPONSE, nil},
		//select in a bad format should err
		{[]byte("*2\r\n$6\r\nselect\r\n$1\r\na\r\n"), nil, protocol.ERR_BAD_ARGUMENTS},
		//random command on our blacklist should respond appropriately
		{[]byte("*1\r\n$4\r\nauth\r\n"), nil, protocol.ERR_COMMAND_UNSUPPORTED},
		//random command on our pubsub list should respond appropriately
		{[]byte("*1\r\n$6\r\npubsub\r\n"), nil, protocol.ERR_COMMAND_UNSUPPORTED},
		//multi should fail
		{[]byte("*1\r\n$5\r\nmulti\r\n"), nil, protocol.ERR_COMMAND_UNSUPPORTED},
	}

	listenSock, err := net.Listen("unix", "/tmp/rmuxTest1.sock")
	if err != nil {
		test.Fatalf("Cannot listen on /tmp/rmuxTest1.sock: %s", err)
	}
	defer listenSock.Close()

	testConnection, err := net.DialTimeout("unix", "/tmp/rmuxTest1.sock", 1*time.Second)
	if err != nil {
		test.Fatal("Could not dial in to our local rmux sock")
	}
	defer testConnection.Close()

	client := NewClient(testConnection, 1*time.Millisecond, 1*time.Millisecond, true)

	for _, testCase := range testCases {
		w := new(bytes.Buffer)
		//Make a small buffer, just to confirm flushes
		client.ReadWriter.Writer = bufio.NewWriterSize(w, 38)
		client.ReadWriter.Reader = bufio.NewReader(bytes.NewBuffer(testCase.input))

		readCommand, err := protocol.ReadCommand(client.Reader)
		if err != nil {
			test.Errorf("Errored while reading the command %q: %s", string(testCase.input), err)
			return
		}

		immediateResponse, err := client.ParseCommand(readCommand)

		if bytes.Compare(testCase.immediateResponse, immediateResponse) != 0 {
			test.Errorf("ParseCommand(%q) should have returned immediate response %q, but returned %q", string(testCase.input), testCase.immediateResponse, immediateResponse)
		}

		if testCase.err != err {
			test.Errorf("ParseCommand(%q) should have returned err %q, but returned %q", string(testCase.input), testCase.err, err)
		}
	}
}
