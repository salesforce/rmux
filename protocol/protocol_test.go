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
	if int1 == int2 {
		test.Logf("Received correct int values %d %d", int1, int2)
	} else {
		test.Errorf("Did not receive correct int values %d %d", int1, int2)
	}
}

func (test *ProtocolTester) verifyParseIntError(fakeInt []byte) {
	_, err := ParseInt(fakeInt)
	if err == nil {
		test.Fatalf("ParseInt did not fatal on %q", fakeInt)
	} else {
		test.Logf("ParseInt fataled on %q", fakeInt)
	}
}

func (test *ProtocolTester) verifyParseIntResponse(fakeInt []byte, expected int) {
	value, err := ParseInt(fakeInt)
	if err == nil {
		test.Logf("ParseInt did not fatal %q", fakeInt)
	} else {
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
	tester.verifyParseIntError([]byte("-1"))

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
	var commandBuffer []byte
	_, _, err := GetCommand(buf, commandBuffer, commandBuffer)
	if err == nil {
		test.Fatalf("GetCommand did not err on %q", badCommand)
	} else {
		test.Logf("GetCommand erred on %q", badCommand)
	}
}

func (test *ProtocolTester) compareString(str1, str2 string) {
	if str1 == str2 {
		test.Logf("Received correct string values %s %s", str1, str2)
	} else {
		test.Errorf("Did not receive correct string values %s %s", str1, str2)
	}
}

func (test *ProtocolTester) verifyGetCommandResponse(validMessage, expectedCommand string, expectedArgument string) {
	buf := bufio.NewReader(bytes.NewBufferString(validMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	var command, argument [20]byte
	commandLength, argumentLength, _ := GetCommand(buf, command[:], argument[:])
	test.compareString(string(command[:commandLength]), expectedCommand)
	test.compareString(string(argument[:argumentLength]), expectedArgument)
}

func TestGetCommand(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyGetCommandError("info")
	tester.verifyGetCommandError("$4\r\ninf")
	tester.verifyGetCommandError("$4\r\ninfo")
	tester.verifyGetCommandError("$4\r\ninfo\r")
	tester.verifyGetCommandError("$a\r\ninfo")

	tester.verifyGetCommandError("$3\r\nget\r\n$1a")
	tester.verifyGetCommandError("$3\r\nget\r\n$a")
	tester.verifyGetCommandError("$3\r\nget\r\n$1\r\naa")

	tester.verifyGetCommandResponse("$4\r\niNfo\r\n", "info", "")
	tester.verifyGetCommandResponse("$3\r\nget\r\n$1\r\na\r\n", "get", "a")
}

func TestWriteLine(test *testing.T) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	buf := bufio.NewWriterSize(w, 38)
	//buffer of length 10 (8 plus \r\n)
	ten_bytes := []byte("0123456789")
	writeLine(ten_bytes, buf)
	written := w.Bytes()
	if len(written) == 0 {
		test.Logf("Verified nothing was flushed after 12/38 chars, remaining: %d", buf.Available())
	} else {
		test.Fatal("Buffer flushed prematurely")
	}
	writeLine(ten_bytes, buf)
	written = w.Bytes()
	if len(written) == 0 {
		test.Logf("Verified nothing was flushed after 24/38 chars, remaining: %d", buf.Available())
	} else {
		test.Fatal("Buffer flushed prematurely")
	}
	writeLine(ten_bytes, buf)
	written = w.Bytes()
	if len(written) == 0 {
		test.Logf("Verified nothing was flushed after 36/38 chars, remaining: %d", buf.Available())
	} else {
		test.Fatal("Buffer flushed prematurely")
	}
	writeLine([]byte{'1'}, buf)
	written = w.Bytes()
	if len(written) == 38 {
		test.Log("Verified existing buffer flushed once full")
	} else {
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
	writeLine(ten_bytes, buf)
	written := w.Bytes()
	if len(written) == 0 {
		test.Logf("Verified nothing was flushed after 12/38 chars, remaining: %d", buf.Available())
	} else {
		test.Fatal("Buffer flushed prematurely")
	}
	FlushLine(ten_bytes, buf)

	written = w.Bytes()
	if len(written) == 24 {
		test.Log("Verified flush_line was obeyed")
	} else {
		test.Fatal("Buffer did not flush")
	}
}

func (test *ProtocolTester) verifyIgnoreBulkMessageError(badLine, badMessage string) {
	buf := bufio.NewReader(bytes.NewBufferString(badMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := ignoreBulkMessage([]byte(badLine), buf)
	if err == nil {
		test.Fatalf("ignoreBulkMessage did not fatal on %q", badMessage)
	} else {
		test.Logf("ignoreBulkMessage fataled on %q", badMessage)
	}
}

func (test *ProtocolTester) verifyGoodIgnoreBulkMessage(goodLine, goodMessage, extraMessage string) {
	buf := bufio.NewReader(bytes.NewBufferString(strings.Join([]string{goodMessage, extraMessage}, "")))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := ignoreBulkMessage([]byte(goodLine), buf)
	if err == nil {
		test.Logf("ignoreBulkMessage did not fatal on %q", goodMessage)
	} else {
		test.Fatalf("ignoreBulkMessage fataled on $q", goodMessage)
	}
	if buf.Buffered() == len(extraMessage) {
		test.Logf("ignoreBulkMessage left the right stuff on the buffer %q", goodMessage)
	} else {
		test.Fatalf("ignoreBulkMessage did not leave the right stuff on the buffer %q", goodMessage)
	}
}

func TestIgnoreBulkMuessage(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyIgnoreBulkMessageError("$a", "abc123")
	tester.verifyIgnoreBulkMessageError("abc", "abc123")
	tester.verifyIgnoreBulkMessageError("$3", "12\r\n")
	tester.verifyIgnoreBulkMessageError("$3", "1234\r\n")

	tester.verifyGoodIgnoreBulkMessage("$3", "123\r\n", "")
	tester.verifyGoodIgnoreBulkMessage("$3", "123\r\n", "abc")
	tester.verifyGoodIgnoreBulkMessage("$0", "\r\n", "leftover stuff")
	//newlines are perfectly valid in the middle of payloads, this is why the bulk format exists
	tester.verifyGoodIgnoreBulkMessage("$4", "1\r\n2\r\n", "and even more")
}

func (test *ProtocolTester) verifyIgnoreMultiBulkMessageError(badLine, badMessage string) {
	buf := bufio.NewReader(bytes.NewBufferString(badMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := IgnoreMultiBulkMessage([]byte(badLine), buf)
	if err == nil {
		test.Fatalf("ignoreMultiBulkMessage did not fatal on %q", badMessage)
	} else {
		test.Logf("ignoreMultiBulkMessage fataled on %q", badMessage)
	}
}

func (test *ProtocolTester) verifyGoodIgnoreMultiBulkMessage(goodLine, goodMessage, extraMessage string) {
	buf := bufio.NewReader(bytes.NewBufferString(strings.Join([]string{goodMessage, extraMessage}, "")))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := IgnoreMultiBulkMessage([]byte(goodLine), buf)
	if err == nil {
		test.Logf("ignoreMultiBulkMessage did not fatal on %q", goodMessage)
	} else {
		test.Fatalf("ignoreMultiBulkMessage fataled on %q", goodMessage)
	}
	if buf.Buffered() == len(extraMessage) {
		test.Logf("ignoreMultiBulkMessage left the right stuff on the buffer %q", goodMessage)
	} else {
		test.Fatalf("ignoreMultiBulkMessage did not leave the right stuff on the buffer %q", goodMessage)
	}
}

func TestIgnoreMultiBulkMuessage(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyIgnoreMultiBulkMessageError("$a", "abc123")
	tester.verifyIgnoreMultiBulkMessageError("abc", "abc123")
	tester.verifyIgnoreMultiBulkMessageError("$3", "123\r\n")
	//too much data
	tester.verifyIgnoreMultiBulkMessageError("*1", "$1\r\nab\r\n")
	//not enough data
	tester.verifyIgnoreMultiBulkMessageError("*1", "$1\r\nab\r\n")
	//not enough bulk messages (specifying 3, only 2 exist)
	tester.verifyIgnoreMultiBulkMessageError("*3", "$1\r\na\r\n$1\r\na\r\n")

	//Error case
	tester.verifyGoodIgnoreMultiBulkMessage("*1", "$-1\r\n", "")
	tester.verifyGoodIgnoreMultiBulkMessage("*1", "$1\r\na\r\n", "")
	//verify buffer remains
	tester.verifyGoodIgnoreMultiBulkMessage("*2", "$3\r\nabc\r\n$2\r\nab\r\n", "extra stuff")
	//error case in the middle of good responses (hmget can do this)
	tester.verifyGoodIgnoreMultiBulkMessage("*3", "$3\r\nabc\r\n$-1\r\n$2\r\nab\r\n", "more extra stuff")
}

func (test *ProtocolTester) verifyCopyBulkMessageError(badLine, badMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(badMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := copyBulkMessage([]byte(badLine), writer, buf)
	if err == nil {
		test.Fatalf("copyBulkMessage did not fatal on %q", badMessage)
	} else {
		test.Logf("copyBulkMessage fataled on %q", badMessage)
	}
}

func (test *ProtocolTester) verifyGoodCopyBulkMessage(goodLine, goodMessage, extraMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(strings.Join([]string{goodMessage, extraMessage}, "")))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := copyBulkMessage([]byte(goodLine), writer, buf)
	writer.Flush()
	if err == nil {
		test.Logf("ignoreMultiBulkMessage did not fatal on %q", goodMessage)
	} else {
		test.Fatalf("ignoreMultiBulkMessage fataled on %q", goodMessage)
	}
	if buf.Buffered() == len(extraMessage) {
		test.Logf("ignoreMultiBulkMessage left the right stuff on the buffer %q", goodMessage)
	} else {
		test.Fatalf("ignoreMultiBulkMessage did not leave the right stuff on the buffer %q", goodMessage)
	}

	fullMessage := strings.Join([]string{goodLine, goodMessage}, "\r\n")
	if bytes.Equal(w.Bytes(), []byte(fullMessage)) {
		test.Log("The right stuff got copied into our writer")
	} else {
		test.Fatalf("Our buffer is missing data? %q %q", w.Bytes(), []byte(fullMessage))
	}
}

func TestCopyBulkMuessage(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyCopyBulkMessageError("$a", "abc123")
	tester.verifyCopyBulkMessageError("abc", "abc123")
	tester.verifyCopyBulkMessageError("$3", "12\r\n")
	tester.verifyCopyBulkMessageError("$3", "1234\r\n")

	tester.verifyGoodCopyBulkMessage("$3", "123\r\n", "")
	tester.verifyGoodCopyBulkMessage("$3", "123\r\n", "abc")
	tester.verifyGoodCopyBulkMessage("$0", "\r\n", "leftover stuff")
//	newlines are perfectly valid in the middle of payloads, this is why the bulk format exists
	tester.verifyGoodCopyBulkMessage("$4", "1\r\n2\r\n", "and even more")
}

func (test *ProtocolTester) verifyCopyMultiBulkMessageError(badLine, badMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(badMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := CopyMultiBulkMessage([]byte(badLine), writer, buf)
	if err == nil {
		test.Fatalf("CopyMultiBulkMessage did not fatal on %q", badMessage)
	} else {
		test.Logf("CopyMultiBulkMessage fataled on %q", badMessage)
	}
}

func (test *ProtocolTester) verifyGoodCopyMultiBulkMessage(goodLine, goodMessage, extraMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(strings.Join([]string{goodMessage, extraMessage}, "")))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := CopyMultiBulkMessage([]byte(goodLine), writer, buf)
	if err == nil {
		test.Logf("CopyMultiBulkMessage did not fatal on %q", goodMessage)
	} else {
		test.Fatalf("CopyMultiBulkMessage fataled on %q", goodMessage)
	}
	if buf.Buffered() == len(extraMessage) {
		test.Logf("CopyMultiBulkMessage left the right stuff on the buffer %q", goodMessage)
	} else {
		test.Fatalf("CopyMultiBulkMessage did not leave the right stuff on the buffer %q", goodMessage)
	}

	fullMessage := strings.Join([]string{goodLine, goodMessage}, "\r\n")
	if bytes.Equal(w.Bytes(), []byte(fullMessage)) {
		test.Log("The right stuff got copied into our writer")
	} else {
		test.Fatalf("Our buffer is missing data? %q %q", w.Bytes(), []byte(fullMessage))
	}
}

func TestCopyMultiBulkMuessage(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyCopyMultiBulkMessageError("$a", "abc123")
	tester.verifyCopyMultiBulkMessageError("abc", "abc123")
	tester.verifyCopyMultiBulkMessageError("$3", "123\r\n")
	//too much data
	tester.verifyCopyMultiBulkMessageError("*1", "$1\r\nab\r\n")
	//not enough data
	tester.verifyCopyMultiBulkMessageError("*1", "$1\r\nab\r\n")
	//not enough bulk messages (specifying 3, only 2 exist)
	tester.verifyCopyMultiBulkMessageError("*3", "$1\r\na\r\n$1\r\na\r\n")

	//Error case
	tester.verifyGoodCopyMultiBulkMessage("*1", "$-1\r\n", "")
	tester.verifyGoodCopyMultiBulkMessage("*1", "$1\r\na\r\n", "")
	//verify buffer remains
	tester.verifyGoodCopyMultiBulkMessage("*2", "$3\r\nabc\r\n$2\r\nab\r\n", "extra stuff")
	//error case in the middle of good responses (hmget can do this)
	tester.verifyGoodCopyMultiBulkMessage("*3", "$3\r\nabc\r\n$-1\r\n$2\r\nab\r\n", "more extra stuff")
}

func (test *ProtocolTester) verifyCopyServerResponseError(badMessage string) {
	w := new(bytes.Buffer)
	w.Reset()
	//Make a small buffer, just to confirm occasional flushes
	writer := bufio.NewWriterSize(w, 100)

	buf := bufio.NewReader(bytes.NewBufferString(badMessage))
	//If this looks hacky, that's because it is
	//bufio.NewReader doesn't call fill() upon init, so we have to force it
	buf.Peek(1)
	err := CopyServerResponse(buf, writer)
	if err == nil {
		test.Fatalf("CopyServerResponse did not fatal on %q", badMessage)
	} else {
		test.Logf("CopyServerResponse fataled on %q", badMessage)
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
	err := CopyServerResponse(buf, writer)
	if err == nil {
		test.Logf("CopyServerResponse did not fatal on %q", goodMessage)
	} else {
		test.Fatalf("CopyServerResponse fataled on %q", goodMessage)
	}
	if buf.Buffered() == len(extraMessage) {
		test.Logf("CopyServerResponse left the right stuff on the buffer %q", goodMessage)
	} else {
		test.Fatalf("CopyServerResponse did not leave the right stuff on the buffer %q", goodMessage)
	}

	if bytes.Equal(w.Bytes(), []byte(goodMessage)) {
		test.Log("The right stuff got copied into our writer")
	} else {
		test.Fatalf("Our buffer is missing data? %q %q", w.Bytes(), []byte(goodMessage))
	}
}

func TestCopyServerResponse(test *testing.T) {
	tester := &ProtocolTester{test}
	tester.verifyCopyServerResponseError("$a\r\n")
	tester.verifyCopyServerResponseError("*a\r\n")
	tester.verifyCopyServerResponseError("$1\r\nab")
	tester.verifyCopyServerResponseError("$3\r\nab\r\n")
	tester.verifyCopyServerResponseError("*1a\r\n$1\r\nab\r\n")
	//too much data
	tester.verifyCopyServerResponseError("*1\r\n$1\r\nab\r\n")
	//not enough data
	tester.verifyCopyServerResponseError("*1\r\n$1\r\nab\r\n")
	//not enough bulk messages (specifying 3, only 2 exist)
	tester.verifyCopyServerResponseError("*3\r\n$1\r\na\r\n$1\r\na\r\n")

	//Error case
	tester.verifyGoodCopyServerResponse("*1\r\n$-1\r\n", "")
	tester.verifyGoodCopyServerResponse("*1\r\n$1\r\na\r\n", "")
	//verify buffer remains
	tester.verifyGoodCopyServerResponse("*2\r\n$3\r\nabc\r\n$2\r\nab\r\n", "extra stuff")
	//error case in the middle of good responses (hmget can do this)
	tester.verifyGoodCopyServerResponse("*3\r\n$3\r\nabc\r\n$-1\r\n$2\r\nab\r\n", "more extra stuff")
	tester.verifyGoodCopyServerResponse("*-1\r\n", "extra stuff")
	tester.verifyGoodCopyServerResponse("$1\r\n1\r\n", "extra stuff")
	tester.verifyGoodCopyServerResponse(":5\r\n", "extra stuff")
	tester.verifyGoodCopyServerResponse(":-5\r\n", "extra stuff")
	tester.verifyGoodCopyServerResponse("+OK\r\n", "extra stuff")
}

func BenchmarkGetCommand(bench *testing.B) {
	bench.ResetTimer()
	bench.StopTimer()
	var ignored []byte
	for i := 0; i < bench.N; i++ {
		buf := bufio.NewReader(bytes.NewBufferString("$3\r\nget\r\n$3\r\nabc\r\n"))
		//If this looks hacky, that's because it is
		//bufio.NewReader doesn't call fill() upon init, so we have to force it
		buf.Peek(1)
		bench.StartTimer()
		GetCommand(buf, ignored, ignored)
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

func BenchmarkIgnoreBulkMessage(bench *testing.B) {
	bench.ResetTimer()
	bench.StopTimer()
	for i := 0; i < bench.N; i++ {
		buf := bufio.NewReader(bytes.NewBufferString("abc\r\n"))
		firstLine := []byte{'$', '3'}
		//If this looks hacky, that's because it is
		//bufio.NewReader doesn't call fill() upon init, so we have to force it
		buf.Peek(1)
		bench.StartTimer()
		ignoreBulkMessage(firstLine, buf)
		bench.StopTimer()
	}
}

func BenchmarkIgnoreMultiBulkMessage(bench *testing.B) {
	bench.ResetTimer()
	bench.StopTimer()
	for i := 0; i < bench.N; i++ {
		buf := bufio.NewReader(bytes.NewBufferString("$3\r\nabc\r\n$-1\r\n$2\r\nab\r\n"))
		firstLine := []byte{'*', '3'}
		//If this looks hacky, that's because it is
		//bufio.NewReader doesn't call fill() upon init, so we have to force it
		buf.Peek(1)
		bench.StartTimer()
		IgnoreMultiBulkMessage(firstLine, buf)
		bench.StopTimer()
	}
}
