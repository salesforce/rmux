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

package protocol

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

type ProtocolTester struct {
	*testing.T
}

func (test *ProtocolTester) compareInt(int1, int2 int) {
	if int1 != int2 {
		test.Errorf("Did not receive correct int values %d %d", int1, int2)
	}
}

func (test *ProtocolTester) verifyParseIntError(fakeInt []byte) {
	_, err := ParseInt(fakeInt)
	if err == nil {
		test.Errorf("ParseInt did not error on %q", fakeInt)
	}
}

func (test *ProtocolTester) verifyParseIntResponse(fakeInt []byte, expected int) {
	value, err := ParseInt(fakeInt)
	if err != nil {
		test.Fatalf("ParseInt fataled on %q", fakeInt)
	}

	test.compareInt(value, expected)
}

func TestParseInt(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyParseIntError([]byte("invalid int"))
	tester.verifyParseIntError([]byte("01b"))
	tester.verifyParseIntError([]byte("0b1"))
	tester.verifyParseIntError([]byte("b1"))

	tester.verifyParseIntResponse([]byte("-1"), -1)
	tester.verifyParseIntResponse([]byte("12345"), 12345)
	tester.verifyParseIntResponse([]byte("01"), 1)
	tester.verifyParseIntResponse([]byte("10"), 10)
}

// Verifies that the given bad command errors
func (test *ProtocolTester) verifyGetCommandError(badCommand string) {
	buf := bufio.NewReader(bytes.NewBufferString(badCommand))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	_, err := ReadCommand(buf)
	if err == nil {
		test.Errorf("GetCommand did not err on %q", badCommand)
	}
}

func (test *ProtocolTester) compareString(str1, str2 string) {
	if str1 != str2 {
		test.Errorf("Did not receive correct string values %s %s", str1, str2)
	}
}

func (test *ProtocolTester) verifyGetCommandResponse(validMessage, expectedCommand string, expectedArgument string) {
	buf := bufio.NewReader(bytes.NewBufferString(validMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	command, err := ReadCommand(buf)

	if err != nil {
		test.Errorf("Received unexpected error from ReadCommand(%q): %s", validMessage, err)
		return
	}

	test.compareString(string(command.GetCommand()), expectedCommand)
	test.compareString(string(command.GetFirstArg()), expectedArgument)
}

func TestGetCommand(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyGetCommandError("$4\r\ninf")
	tester.verifyGetCommandError("$4\r\ninfo")
	tester.verifyGetCommandError("$4\r\ninfo\r")
	tester.verifyGetCommandError("$a\r\ninfo")

	tester.verifyGetCommandResponse("$3\r\nget\r\n$1a", "get", "")
	tester.verifyGetCommandResponse("$3\r\nget\r\n$a", "get", "")
	tester.verifyGetCommandResponse("$3\r\nget\r\n$1\r\naa", "get", "")
	tester.verifyGetCommandResponse("$4\r\niNfo\r\n", "info", "")

	tester.verifyGetCommandResponse("info", "info", "")
	tester.verifyGetCommandResponse("*1\r\n$4\r\niNfo\r\n", "info", "")
	tester.verifyGetCommandResponse("*2\r\n$3\r\nget\r\n$1\r\na\r\n", "get", "a")
}

func TestWriteLine(test *testing.T) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	buf := bufio.NewWriterSize(w, 38)
	//buffer of length 10 (8 plus \r\n)
	ten_bytes := []byte("0123456789")
	WriteLine(ten_bytes, buf, false)
	written := w.Bytes()
	if len(written) != 0 {
		test.Fatal("Buffer flushed prematurely")
	}
	WriteLine(ten_bytes, buf, false)
	written = w.Bytes()
	if len(written) != 0 {
		test.Fatal("Buffer flushed prematurely")
	}
	WriteLine(ten_bytes, buf, false)
	written = w.Bytes()
	if len(written) != 0 {
		test.Fatal("Buffer flushed prematurely")
	}
	WriteLine([]byte{'1'}, buf, false)
	written = w.Bytes()
	if len(written) != 38 {
		test.Fatal("Buffer did not flush", len(written))
	}
}

func TestFlushLine(test *testing.T) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	buf := bufio.NewWriterSize(w, 38)
	//buffer of length 10 (8 plus \r\n)
	ten_bytes := []byte("0123456789")
	WriteLine(ten_bytes, buf, false)
	written := w.Bytes()
	if len(written) != 0 {
		test.Fatal("Buffer flushed prematurely")
	}
	WriteLine(ten_bytes, buf, true)

	written = w.Bytes()
	if len(written) != 24 {
		test.Fatal("Buffer did not flush")
	}
}

func (test *ProtocolTester) verifyGoodCopyServerResponse(goodMessage, extraMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(strings.Join([]string{goodMessage, extraMessage}, "")))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := CopyServerResponses(buf, writer, 1)
	if err != nil {
		test.Fatalf("CopyServerResponse fataled on %q", goodMessage)
	}
	if buf.Buffered() != len(extraMessage) {
		test.Fatalf("CopyServerResponse did not leave the right stuff on the buffer %q", goodMessage)
	}

	if !bytes.Equal(w.Bytes(), []byte(goodMessage)) {
		test.Fatalf("Our buffer is missing data? %q %q", w.Bytes(), []byte(goodMessage))
	}
}

func BenchmarkGetCommand(bench *testing.B) {
	bench.ResetTimer()
	bench.StopTimer()
	for i := 0; i < bench.N; i++ {
		buf := bufio.NewReader(bytes.NewBufferString("$3\r\nget\r\n$3\r\nabc\r\n"))
		//If this looks hacky, that's because it is
		//bufio.NewReader doesn't call fill() upon init, so we have to force it
		buf.Peek(1)
		bench.StartTimer()
		ReadCommand(buf)
		bench.StopTimer()
	}
}

func BenchmarkGoodParseInt(bench *testing.B) {
	for i := 0; i < bench.N; i++ {
		ParseInt([]byte("12345"))
	}
}

func BenchmarkBadParseInt(bench *testing.B) {
	for i := 0; i < bench.N; i++ {
		ParseInt([]byte("a1"))
	}
}

var testDataAllRedisCommands = []struct {
	Command        string
	SupportsMux    bool
	SupportsNonMux bool
}{
	{"append", true, true},
	{"auth", false, false},
	{"bgrewriteaof", false, false},
	{"bgsave", false, false},
	{"bitcount", true, true},
	{"bitop", false, true}, // has a different format than other commands
	{"bitpos", true, true},
	{"blpop", false, true},      // key [key ...] timeout
	{"brpop", false, true},      // key [key ...] timeout
	{"brpoplpush", false, true}, // source destination timeout - source and destination are keys
	{"client", false, false},    // dangerous
	{"cluster", false, false},   // dangerous
	{"command", false, false},   // shouldn't need it
	{"config", false, false},    // dangerous
	{"dbsize", false, false},    // considered dangerous
	{"debug", false, false},     // dangerous
	{"decr", true, true},
	{"decrby", true, true},
	{"del", true, true},
	{"discard", false, false}, // dont support transactions
	{"dump", true, true},
	{"echo", true, true},
	{"eval", false, true}, // can operate on several keys
	{"evalsha", false, true},
	{"exec", false, false},
	{"exists", true, true},
	{"expireat", true, true},
	{"flushall", false, true},
	{"flushdb", false, true},
	{"get", true, true},
	{"getbit", true, true},
	{"getrange", true, true},
	{"getset", true, true},
	{"hdel", true, true},
	{"hexists", true, true},
	{"hget", true, true},
	{"hgetall", true, true},
	{"hincrby", true, true},
	{"hincrbyfloat", true, true},
	{"hkeys", true, true},
	{"hlen", true, true},
	{"hmget", true, true},
	{"hmset", true, true},
	{"hsetnx", true, true},
	{"hstrlen", true, true},
	{"hvals", true, true},
	{"incr", true, true},
	{"incrby", true, true},
	{"incrbyfloat", true, true},
	{"info", true, true},
	{"keys", false, true},      // can glob many keys, not supported over mux
	{"lastsave", false, false}, // system related information
	{"lindex", true, true},
	{"linsert", true, true},
	{"llen", true, true},
	{"lpop", true, true},
	{"lpush", true, true},
	{"lpushx", true, true},
	{"lrange", true, true},
	{"lrem", true, true},
	{"lset", true, true},
	{"ltrim", true, true},
	{"mget", false, true},
	{"migrate", false, false}, // system related operation - dangerous
	{"monitor", false, false}, // system related operation - dangerous
	{"move", false, false},    // moves between dbs, let's not support
	{"mset", false, true},     // should operate on multiple keys
	{"multi", false, false},   // transaction related
	{"object", false, false},  // to inspect internals
	{"persist", true, true},
	{"pexpire", true, true},
	{"pexpireat", true, true},
	{"pfadd", true, true},
	{"pfcount", true, true},
	{"pfmerge", false, true},
	{"ping", true, true},
	{"psetex", true, true},
	{"psubscribe", false, false},
	{"pubsub", false, false},
	{"pttl", true, true},
	{"publish", true, true},
	{"punsubscribe", false, false},
	{"quit", true, true},
	{"randomkey", false, true},
	{"rename", false, true},
	{"renamenx", false, true},
	{"restore", true, true},
	{"role", false, false}, // returns role in replication
	{"rpop", true, true},
	{"rpoplpush", false, true},
	{"rpush", true, true},
	{"rpushx", true, true},
	{"sadd", true, true},
	{"save", false, false},
	{"scard", true, true},
	{"script", true, true},
	{"sdiff", false, true},
	{"sdiffstore", false, true},
	{"select", true, true},
	{"set", true, true},
	{"setbit", true, true},
	{"setex", true, true},
	{"setnx", true, true},
	{"setrange", true, true},
	{"shutdown", false, false}, // system related operation - dangerous
	{"sinter", false, true},
	{"sinterstore", false, true},
	{"sismember", true, true},
	{"slaveof", false, false}, // system related operation - dangerous
	{"slowlog", false, false}, // system related operation - dangerous
	{"smembers", true, true},
	{"smove", false, true},
	{"sort", true, true},
	{"spop", true, true},
	{"srandmember", true, true},
	{"srem", true, true},
	{"strlen", true, true},
	{"subscribe", false, false},
	{"sunion", false, true},
	{"sunionstore", false, true},
	{"sync", false, false}, // used for replication
	{"time", true, true},
	{"ttl", true, true},
	{"type", true, true},
	{"unsubscribe", false, false},
	{"unwatch", false, false}, // transaction related
	{"watch", false, false},   // transaction related
	{"zadd", true, true},
	{"zcard", true, true},
	{"zcount", true, true},
	{"zincrby", true, true},
	{"zinterstore", false, true},
	{"zlexcount", true, true},
	{"zrange", true, true},
	{"zrangebylex", true, true},
	{"zrevrangebylex", true, true},
	{"zrangebyscore", true, true},
	{"zrank", true, true},
	{"zrem", true, true},
	{"zremrangebylex", true, true},
	{"zremrangebyrank", true, true},
	{"zremrangebyscore", true, true},
	{"zrevremrange", true, true},
	{"zrevrangebyscore", true, true},
	{"zrevrank", true, true},
	{"zscore", true, true},
	{"zunionscore", false, true},
	{"scan", false, true},
	{"sscan", true, true},
	{"hscan", true, true},
	{"zscan", true, true},
}

func TestIsSupportedFunction_NotMultipleKeys(test *testing.T) {
	for _, command := range testDataAllRedisCommands {
		bcommand := []byte(command.Command)

		if IsSupportedFunction(bcommand, true, false) != command.SupportsMux {
			if command.SupportsMux {
				test.Errorf("Should be supported in multiplexing mode but is not: %s", command.Command)
			} else {
				test.Errorf("Should not be supported in multiplexing mode but is: %s", command.Command)
			}
		}

		if IsSupportedFunction(bcommand, false, false) != command.SupportsNonMux {
			if command.SupportsNonMux {
				test.Errorf("Should be supported in non-multiplexing mode but is not: %s", command.Command)
			} else {
				test.Errorf("Should not be supported in non-multiplexing mode but is: %s", command.Command)
			}
		}
	}
}

func TestIsSupportedFunction_MultipleKeys(test *testing.T) {
	unsupportedWithMultipleKeys := map[string]bool{
		// given multiple keys, the following should not be supported in mux mode
		"bitop":       true,
		"blpop":       true,
		"brpop":       true,
		"del":         true,
		"mget":        true,
		"pfcount":     true,
		"pfmerge":     true,
		"sdiff":       true,
		"sdiffstore":  true,
		"sinter":      true,
		"sinterstore": true,
		"sunion":      true,
		"sunionstore": true,
		"watch":       true,
		"zinterstore": true,
		"zunionstore": true,
	}

	for _, command := range testDataAllRedisCommands {
		bcommand := []byte(command.Command)

		isSupported := IsSupportedFunction(bcommand, true, true)

		if unsupportedWithMultipleKeys[command.Command] {
			if isSupported {
				test.Errorf("Should not be supported with multiple args in multiplexing mode but is: %s", command.Command)
			}
		} else {
			if isSupported != command.SupportsMux {
				test.Errorf("Should be supported if has multiple args in multiplexing mode but is not: %s", command.Command)
			}
		}
	}
}

func BenchmarkIsSupportedFunction(b *testing.B) {
	slice := []byte("sismember")

	for i := 0; i < b.N; i++ {
		IsSupportedFunction(slice, true, true)
	}
}
