package protocol

import (
	"testing"
)

var simpleTestData = map[string]commandTestData{
	"+PING\r\n": commandTestData{
		"ping",
		"",
		"+ping\r\n",
		0,
	},

	"+BGREWRITEAOF\r\n": commandTestData{
		"bgrewriteaof",
		"",
		"+bgrewriteaof\r\n",
		0,
	},

	"+command\r\n": commandTestData{
		"command",
		"",
		"+command\r\n",
		0,
	},

	"+dbsize\r\n": commandTestData{
		"dbsize",
		"",
		"+dbsize\r\n",
		0,
	},

	"+discard\r\n": commandTestData{
		"discard",
		"",
		"+discard\r\n",
		0,
	},

	// Test the expansion of the internal buffer slice
	"+someverylongcommandthatprobablydoesntexist\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"+someverylongcommandthatprobablydoesntexist\r\n",
		0,
	},

	"+SOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST\r\n": commandTestData{
		"someverylongcommandthatprobablydoesntexist",
		"",
		"+someverylongcommandthatprobablydoesntexist\r\n",
		0,
	},
}

func TestSimpleCommand(test *testing.T) {
	tester := commandTester{test}

	for input, expected := range simpleTestData {
		command, err := ParseSimpleCommand([]byte(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
