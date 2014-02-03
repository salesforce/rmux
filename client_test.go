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

func checkParseCommandResponse(test *testing.T, myClient *Client, command, response []byte, withMulti, shouldErr bool) {
	w := new(bytes.Buffer)
	//Make a small buffer, just to confirm flushes
	myClient.ReadWriter.Writer = bufio.NewWriterSize(w, 38)

	//Split our command into whatever's first, and everything else'
	parts := bytes.SplitN(command, []byte{'\r', '\n'}, 2)
	if len(parts) == 2 {
		myClient.ReadWriter.Reader = bufio.NewReader(bytes.NewBuffer(parts[1]))
		myClient.ReadWriter.Reader.Peek(1)
	}
	responded, err := myClient.ParseCommand(parts[0], withMulti)
	written := w.Bytes()

	if len(response) > 0 {
		if responded {
			test.Log("Expected a response, and got one")
		} else {
			test.Fatal("Expected a response, and did not get one on ", string(command))
		}
	} else {
		if responded {
			test.Fatal("Did not expect a response, but got one")
		} else {
			test.Log("Did not expect a response, and did not get one")
		}
	}

	if shouldErr {
		if err == nil {
			test.Fatal("ParseCommand should have err'd, but did not")
		} else {
			test.Log("ParseCommand was expected to not err, and did not")
		}
	} else {
		if err == nil {
			test.Log("ParseCommand was expected to not err, and did not")
		} else {
			test.Fatal("ParseCommand was expected to not err, but did")
		}
	}

	if response != nil {
		if len(written) == len(response)+2 && bytes.Equal(written[0:len(response)], response) {
			test.Log("Found appropriate response")
		} else {
			test.Fatal("Did not find appropriate response")
		}
	}
}

func TestParseCommand(test *testing.T) {
	listenSock, err := net.Listen("unix", "/tmp/rmuxTest1.sock")
	if err != nil {
		test.Fatal("Cannot listen on /tmp/rmuxTest1.sock: ", err)
	}
	defer func() {
		listenSock.Close()
	}()
	testConnection, err := net.DialTimeout("unix", "/tmp/rmuxTest1.sock", 1*time.Second)
	if err != nil {
		test.Fatal("Could not dial in to our local rmux sock")
	}
	defer func() {
		testConnection.Close()
	}()

	client := NewClient(testConnection, 1*time.Millisecond, 1*time.Millisecond)

	//short ping should respond with a ping response
	checkParseCommandResponse(test, client, protocol.SHORT_PING_COMMAND, protocol.PONG_RESPONSE, false, false)
	//non-short ping should err, if it's not in multibulk format
	checkParseCommandResponse(test, client, protocol.PING_COMMAND, nil, false, true)
	//ping in proper format should respond appropriately
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '4', '\r', '\n', 'p', 'i', 'n', 'g', '\r', '\n'}, protocol.PONG_RESPONSE, false, false)
	//quit in proper format should respond appropriately
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '4', '\r', '\n', 'q', 'u', 'i', 't', '\r', '\n'}, protocol.OK_RESPONSE, false, false)
	//select without database should err
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '6', '\r', '\n', 's', 'e', 'l', 'e', 'c', 't', '\r', '\n'}, ERR_BAD_ARGUMENTS, false, false)
	//select in proper format should respond appropriately
	checkParseCommandResponse(test, client, []byte{'*', '2', '\r', '\n', '$', '6', '\r', '\n', 's', 'e', 'l', 'e', 'c', 't', '\r', '\n', '$', '1', '\r', '\n', '1', '\r', '\n'}, protocol.OK_RESPONSE, false, false)
	//select in a bad format should err
	checkParseCommandResponse(test, client, []byte{'*', '2', '\r', '\n', '$', '6', '\r', '\n', 's', 'e', 'l', 'e', 'c', 't', '\r', '\n', '$', '1', '\r', '\n', 'a', '\r', '\n'}, ERR_BAD_ARGUMENTS, false, false)
	//random command on our blacklist should respond appropriately
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '4', '\r', '\n', 'a', 'u', 't', 'h', '\r', '\n'}, ERR_COMMAND_UNSUPPORTED, false, false)
	//random command on our pubsub list should respond appropriately
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '6', '\r', '\n', 'p', 'u', 'b', 's', 'u', 'b', '\r', '\n'}, ERR_COMMAND_UNSUPPORTED, false, false)
	//multi should fail with multiplexing on
	checkParseCommandResponse(test, client, []byte{'*', '1', '\r', '\n', '$', '5', '\r', '\n', 'm', 'u', 'l', 't', 'i', '\r', '\n'}, ERR_COMMAND_UNSUPPORTED, true, false)
}
