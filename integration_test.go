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
	rs, err := NewRedisMultiplexer("unix", rmuxSock, 20)
	if err != nil {
		r.t.Fatalf("Error when creating new redis multiplexer: %s", err)
	}

	r.s = rs

	for i := 0; i < servers; i++ {
		r.s.AddConnection("unix", redisSock)
	}

	for _, conn := range r.s.ConnectionCluster {
		if !conn.CheckConnectionState() {
			r.t.Errorf("Could not connect to a cluster...")
			return
		}
	}

	go r.s.Start()

	return
}

func (r *tRmux) Cleanup() {
	r.s.active = false
	r.s.Listener.Close()
}

func TestStartRmux(t *testing.T) {
	r := StartRmux(t)
	defer r.Cleanup()
}

func checkResponse(t *testing.T, in string, expected string) {
	var err error

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

	buf := make([]byte, 8*1024)
	b := new(bytes.Buffer)
	sock.SetDeadline(time.Now().Add(1000 * time.Millisecond))
	for read := 0; read < len(expected); {
		n, err := sock.Read(buf)
		if err != nil {
			t.Fatalf("Error reading from sock: %s", err)
		}

		b.Write(buf[:n])
		read += int(n)
	}

	if read := b.Next(len(expected)); bytes.Compare(read, []byte(expected)) != 0 {
		t.Errorf("Did not read the expected response of length 66560.\r\nGot %q\r\n", read)
	}
}

func checkMuxResponse(t *testing.T, in string, expected string) {
	var err error

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

	buf := make([]byte, 8*1024)
	b := new(bytes.Buffer)
	sock.SetDeadline(time.Now().Add(1000 * time.Millisecond))
	for read := 0; read < len(expected); {
		n, err := sock.Read(buf)
		if err != nil {
			t.Fatalf("Error reading from sock: %s", err)
		}

		b.Write(buf[:n])
		read += int(n)
	}

	if read := b.Next(len(expected)); bytes.Compare(read, []byte(expected)) != 0 {
		t.Errorf("Did not read the expected response of length 66560.\r\nGot %q\r\n", read)
	}
}

// given a simple command, construct a multi-bulk command
func makeCommand(str string) string {
	splits := strings.Split(str, " ")

	cmd := "*" + string(len(splits)) + "\r\n"

	for _, s := range splits {
		cmd = cmd + "$" + string(len(s)) + "\r\n"
	}

	return cmd
}

func TestLargeResponse(t *testing.T) {
	cmd := "*3\r\n$4\r\nEVAL\r\n$47\r\nreturn cjson.encode(string.rep('a', 65 * 1024))\r\n$1\r\n0\r\n"
	expected := "$66562\r\n\"" + strings.Repeat("a", 66560) + "\"\r\n"
	checkResponse(t, cmd, expected)
}

func TestPipelineResponse(t *testing.T) {
	cmd := makeCommand("get key1") + makeCommand("set key1 test") + makeCommand("get key1")
	expected := "$-1\r\n:1\r\n$4\r\ntest\r\n"
	checkResponse(t, cmd, expected)
}

func TestMuxPipelineResponse(t *testing.T) {
	cmd := makeCommand("get key1") + makeCommand("set key1 test") + makeCommand("get key1")
	expected := "$-1\r\n:1\r\n$4\r\ntest\r\n"
	checkMuxResponse(t, cmd, expected)
}
