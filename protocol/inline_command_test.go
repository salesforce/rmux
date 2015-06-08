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
	"testing"
)

var inlineTestData = map[string]commandTestData{
	"PING\r\n": commandTestData{
		"ping",
		"",
		"ping\r\n",
		0,
	},

	"BGREWRITEAOF\r\n": commandTestData{
		"bgrewriteaof",
		"",
		"bgrewriteaof\r\n",
		0,
	},

	"command\r\n": commandTestData{
		"command",
		"",
		"command\r\n",
		0,
	},

	"dbsize\r\n": commandTestData{
		"dbsize",
		"",
		"dbsize\r\n",
		0,
	},

	"discard\r\n": commandTestData{
		"discard",
		"",
		"discard\r\n",
		0,
	},

	// Test the expansion of the internal buffer slice
	"someverylongcommandthatprobablydoesntexist\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"someverylongcommandthatprobablydoesntexist\r\n",
		0,
	},

	"SOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"someverylongcommandthatprobablydoesntexist\r\n",
		0,
	},

	"keys *\r\n": commandTestData{
		"keys",
		"*",
		"keys *\r\n",
		1,
	},

	"KEYS *\r\n": commandTestData{
		"keys",
		"*",
		"keys *\r\n",
		1,
	},

	"quit\r\n": commandTestData{
		"quit",
		"",
		"quit\r\n",
		0,
	},

	"QUIT\r\n": commandTestData{
		"quit",
		"",
		"quit\r\n",
		0,
	},

	// Larger than the internal buffer (64), will have to reallocate internally
	"1234567890123456789012345678901234567890 123456789012345678901234567890\r\n": commandTestData{
		"1234567890123456789012345678901234567890",
		"123456789012345678901234567890",
		"1234567890123456789012345678901234567890 123456789012345678901234567890\r\n",
		1,
	},

	"del key1 key2 key3 key4\r\n": commandTestData{
		"del",
		"key1",
		"del key1 key2 key3 key4\r\n",
		4,
	},

	"del  key1  key2  key3    key4   \r\n": commandTestData{
		"del",
		"key1",
		"del  key1  key2  key3    key4   \r\n",
		4,
	},
}

func TestInlineCommand(test *testing.T) {
	tester := commandTester{test}

	for input, expected := range inlineTestData {
		command, err := ParseInlineCommand([]byte(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
