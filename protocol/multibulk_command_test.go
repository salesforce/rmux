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
	"testing"
)

var multibulkTestData = map[string]commandTestData{
	"*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n": commandTestData{
		"keys",
		"*",
		"*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n",
		1,
	},

	"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n": commandTestData{
		"keys",
		"*",
		"*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n",
		1,
	},

	"*1\r\n$4\r\nquit\r\n": commandTestData{
		"quit",
		"",
		"*1\r\n$4\r\nquit\r\n",
		0,
	},

	"*1\r\n$4\r\nQUIT\r\n": commandTestData{
		"quit",
		"",
		"*1\r\n$4\r\nquit\r\n",
		0,
	},

	// Larger than the internal buffer (64), will have to reallocate internally
	"*2\r\n$40\r\n1234567890123456789012345678901234567890\r\n$30\r\n123456789012345678901234567890\r\n": commandTestData{
		"1234567890123456789012345678901234567890",
		"123456789012345678901234567890",
		"*2\r\n$40\r\n1234567890123456789012345678901234567890\r\n$30\r\n123456789012345678901234567890\r\n",
		1,
	},

	"*5\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$4\r\nkey4\r\n": commandTestData{
		"del",
		"key1",
		"*5\r\n$3\r\ndel\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$4\r\nkey4\r\n",
		4,
	},

	// Handle Null Bulk Strings (http://redis.io/topics/protocol)
	"*2\r\n$3\r\ndel\r\n$-1\r\n": commandTestData{
		"del",
		string(NIL_STRING),
		"*2\r\n$3\r\ndel\r\n$-1\r\n",
		1,
	},
}

func TestMultibulkCommand(test *testing.T) {
	tester := commandTester{test}

	for input, expected := range multibulkTestData {
		command, err := ReadMultibulkCommand(getReader(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
