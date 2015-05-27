package protocol

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// Common functions and structs useful for tests in this package

func getReader(str string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(str))
}

type commandTestData struct {
	command  string
	arg1     string
	buffer   string
	argCount int
}

type commandTester struct {
	*testing.T
}

func (this *commandTester) checkCommandOutput(expects commandTestData, command Command, err error, input string) {
	if err != nil {
		this.Fatalf("Error parsing %q: %s", input, err)
	}

	if bytes.Compare([]byte(expects.buffer), command.GetBuffer()) != 0 {
		this.Errorf("Expected buffer to contain the full message.\r\nExpected:\r\n%q\r\nGot:\r\n%q", expects.buffer, command.GetBuffer())
	}

	if bytes.Compare([]byte(expects.command), command.GetCommand()) != 0 {
		this.Errorf("Expected parsed command to match. Expected %q, got %q", expects.command, command.GetCommand())
	}

	if bytes.Compare([]byte(expects.arg1), command.GetFirstArg()) != 0 {
		this.Errorf("Expected parsed arg1 to match. Expected %q, got %q", expects.arg1, command.GetFirstArg())
	}

	if expects.argCount != command.GetArgCount() {
		this.Errorf("GetArgCount() did not match expectations.\r\nExpected: %d\r\nGot: %d", expects.argCount, command.GetArgCount())
	}
}
