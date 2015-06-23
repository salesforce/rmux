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

var stringTestData = map[string]commandTestData{
	"$4\r\nPING\r\n": commandTestData{
		"ping",
		"",
		"$4\r\nping\r\n",
		0,
	},

	"$12\r\nBGREWRITEAOF\r\n": commandTestData{
		"bgrewriteaof",
		"",
		"$12\r\nbgrewriteaof\r\n",
		0,
	},

	"$7\r\ncommand\r\n": commandTestData{
		"command",
		"",
		"$7\r\ncommand\r\n",
		0,
	},

	"$6\r\ndbsize\r\n": commandTestData{
		"dbsize",
		"",
		"$6\r\ndbsize\r\n",
		0,
	},

	"$7\r\ndiscard\r\n": commandTestData{
		"discard",
		"",
		"$7\r\ndiscard\r\n",
		0,
	},

	// Test the expansion of the internal buffer slice
	"$42\r\nsomeverylongcommandthatprobablydoesntexist\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"$42\r\nsomeverylongcommandthatprobablydoesntexist\r\n",
		0,
	},

	"$42\r\nSOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"$42\r\nsomeverylongcommandthatprobablydoesntexist\r\n",
		0,
	},

	"$-1\r\n": commandTestData{
		"",
		"",
		"$-1\r\n",
		0,
	},
}

func TestStringCommand(test *testing.T) {
	tester := commandTester{test}

	for input, expected := range stringTestData {
		command, err := ParseStringCommand([]byte(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
