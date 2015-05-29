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
}

func TestStringCommand(test *testing.T) {
	tester := commandTester{test}

	for input, expected := range stringTestData {
		command, err := ParseStringCommand([]byte(input))

		tester.checkCommandOutput(expected, command, err, input)
	}
}
