// +build integration

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

package rmux

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
	"strconv"
	"io"
)

type tRmux struct {
	t *testing.T
	s *RedisMultiplexer
}

var (
	redisSock string = "/tmp/redis-test.sock"
	rmuxSock  string = "/tmp/rmux-test.sock"
)

func StartRmux(t *testing.T, servers int) (r *tRmux) {
	r = &tRmux{}
	r.t = t

	// start rmux
	rs, err := NewRedisMultiplexer("unix", rmuxSock, 2)
	if err != nil {
		r.t.Fatalf("Error when creating new redis multiplexer: %s", err)
	}

	rs.SetAllTimeouts(2000 * time.Millisecond)

	r.s = rs

	for i := 0; i < servers; i++ {
		r.s.AddConnection("unix", redisSock)
	}

	if r.s.countActiveConnections() != servers {
		r.t.Errorf("Not as many connections are active as expected.")
	}

	go r.s.Start()

	return
}

func (r *tRmux) Cleanup() {
	r.s.active = false
	r.s.Listener.Close()
}

func TestStartRmux(t *testing.T) {
	r := StartRmux(t, 1)
	defer r.Cleanup()
}

func flushRedis(t *testing.T) {
	s, err := net.Dial("unix", redisSock)
	if err != nil {
		t.Fatalf("Error when dialing to redis for flush: %s", err)
	}
	defer s.Close()

	s.Write([]byte("flushall\r\n"))

	b := make([]byte, 2048)
	n, err := s.Read(b)
	if err != nil {
		t.Fatalf("Failed to read on flushall: %s", err)
	}

	if !bytes.Equal(b[:n], []byte("+OK\r\n")) {
		t.Fatalf("Expected OK response, got %q", b[:n])
	}
}

func checkResponse(t *testing.T, in string, expected string) {
	var err error

	flushRedis(t)

	r := StartRmux(t, 1)
	defer r.Cleanup()

	sock, err := net.Dial("unix", rmuxSock)
	if err != nil {
		t.Fatalf("Error dialing rmux socket: %s", err)
	}
	defer sock.Close()

	_, err = sock.Write([]byte(in))
	if err != nil {
		t.Fatalf("Error writing command: %s", err)
	}

	b := new(bytes.Buffer)
	for b.Len() < len(expected) {
		buf := make([]byte, 8*1024)
		sock.SetDeadline(time.Now().Add(1000 * time.Millisecond))
		n, err := sock.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Error reading from sock: %s %d", err)
		}

		b.Write(buf[:n])
	}

	if read := b.Next(len(expected)); bytes.Compare(read, []byte(expected)) != 0 {
		t.Errorf("Did not read the expected response.\r\nGot %q\r\n", read)
	}
}

func checkMuxResponse(t *testing.T, in string, expected string) {
	var err error

	flushRedis(t)

	r := StartRmux(t, 2)
	defer r.Cleanup()

	sock, err := net.Dial("unix", rmuxSock)
	if err != nil {
		t.Fatalf("Error dialing rmux socket: %s", err)
	}
	defer sock.Close()

	_, err = sock.Write([]byte(in))
	if err != nil {
		t.Fatalf("Error writing command: %s", err)
	}

	b := new(bytes.Buffer)
	for b.Len() < len(expected) {
		buf := make([]byte, 8*1024)
		sock.SetDeadline(time.Now().Add(1000 * time.Millisecond))
		n, err := sock.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Error reading from sock: %s %d", err)
		}

		b.Write(buf[:n])
	}

	if read := b.Next(len(expected)); bytes.Compare(read, []byte(expected)) != 0 {
		t.Errorf("Did not read the expected response.\r\nGot %q\r\n", read)
	}
}

// given a simple command, construct a multi-bulk command
func makeCommand(str string) string {
	splits := strings.Split(str, " ")

	cmd := "*" + strconv.Itoa(len(splits)) + "\r\n"

	for _, s := range splits {
		cmd = cmd + "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
	}

	return cmd
}

func TestResponse(t *testing.T) {
	cmd := "+PING\r\n"
	expected := "+PONG\r\n"
	checkResponse(t, cmd, expected)
}

func TestLargeResponse(t *testing.T) {
	cmd := "*3\r\n$4\r\nEVAL\r\n$47\r\nreturn cjson.encode(string.rep('a', 65 * 1024))\r\n$1\r\n0\r\n"
	expected := "$66562\r\n\"" + strings.Repeat("a", 66560) + "\"\r\n"
	checkResponse(t, cmd, expected)
}

func TestPipelineResponse(t *testing.T) {
	cmd := makeCommand("get key1") + makeCommand("set key1 test") + makeCommand("get key1")
	expected := "$-1\r\n+OK\r\n$4\r\ntest\r\n"
	checkResponse(t, cmd, expected)
}

func TestMuxPipelineResponse(t *testing.T) {
	cmd := makeCommand("get key1") + makeCommand("set key1 test") + makeCommand("get key1")
	expected := "$-1\r\n+OK\r\n$4\r\ntest\r\n"
	checkMuxResponse(t, cmd, expected)
}

func TestLargeResponseWithValidation(t *testing.T) {
	script := "local str = \"\"\r\nfor i=1,4000 do\r\nstr = str .. i .. \" \"\r\nend\r\nreturn str\r\n"
	cmd := "*3\r\n$4\r\nEVAL\r\n$" + strconv.Itoa(len(script)) + "\r\n" + script + "\r\n$1\r\n0\r\n"

	// construct the expected string
	expected := ""
	for i := 1; i <= 4000; i++ {
		expected = expected + strconv.Itoa(i) + " "
	}
	expectedResp := "$" + strconv.Itoa(len(expected)) + "\r\n" + expected + "\r\n"

	checkResponse(t, cmd, expectedResp)
}
