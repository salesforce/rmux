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
