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
		command, err := ReadInlineCommand(getReader(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
