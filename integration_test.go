// +build integration

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

func StartRmux(t *testing.T) (r *tRmux) {
	r = &tRmux{}
	r.t = t

	// start rmux
	rs, err := NewRedisMultiplexer("unix", rmuxSock, 20)
	if err != nil {
		r.t.Fatalf("Error when creating new redis multiplexer: %s", err)
	}

	r.s = rs

	r.s.AddConnection("unix", redisSock)

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

	r := StartRmux(t)
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

func TestLargeResponse(t *testing.T) {
	cmd := "*3\r\n$4\r\nEVAL\r\n$47\r\nreturn cjson.encode(string.rep('a', 65 * 1024))\r\n$1\r\n0\r\n"
	expected := "$66562\r\n\"" + strings.Repeat("a", 66560) + "\"\r\n"
	checkResponse(t, cmd, expected)
}
