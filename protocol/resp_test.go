package protocol

import (
	"testing"
	"bytes"
)

func TestReadRSimpleString(t *testing.T) {
	testData := []struct {
		input string
		value []byte
	} {
		{"+Test\r\n",[]byte("Test")},
		{"+This is a simple string test\r\n",[]byte("This is a simple string test")},
	}

	for _, expected := range testData {
		reader := getReader(expected.input)
		val, err := ReadRSimpleString(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
		}

		if bytes.Compare(val.Value, expected.value) != 0 {
			t.Errorf("Value should have parsed to '%s', but was '%s'", expected.value, val.Value)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}
	}
}

func TestReadRBulkString(t *testing.T) {
	testData := []struct {
		input string
		value string
	} {
		{"$4\r\nPING\r\n", "PING"},
		{"$12\r\nBGREWRITEAOF\r\n", "BGREWRITEAOF"},
		{"$7\r\ncommand\r\n", "command"},
		{"$6\r\ndbsize\r\n", "dbsize"},
		{"$7\r\ndiscard\r\n", "discard"},
		{"$42\r\nsomeverylongcommandthatprobablydoesntexist\r\n", "someverylongcommandthatprobablydoesntexist"},
		{"$42\r\nSOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST\r\n", "SOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST"},
	}

	for _, expected := range testData {
		reader := getReader(expected.input)

		val, err := ReadRBulkString(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
		}

		if bytes.Compare(val.Value, []byte(expected.value)) != 0 {
			t.Errorf("Value should have parsed to '%s', but was '%s'", expected.value, val.Value)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}
	}
}

func TestReadInlineString(t *testing.T) {
	testData := []struct {
		input string
		command string
		firstArg string
	} {
		{"PING\r\n", "PING", ""},
		{"BGREWRITEAOF\r\n", "BGREWRITEAOF", ""},
		{"command\r\n", "command", ""},
		{"dbsize\r\n", "dbsize", ""},
		{"discard\r\n", "discard", ""},
		{"keys *\r\n", "keys", "*"},
		{"someverylongcommandthatprobablydoesntexist asdf\r\n", "someverylongcommandthatprobablydoesntexist", "asdf"},
		{"SOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST\r\n", "SOMEVERYLONGCOMMANDTHATPROBABLYDOESNTEXIST", ""},
	}

	for _, expected := range testData {
		reader := getReader(expected.input)

		val, err := ReadRInlineString(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
		}

		if bytes.Compare(val.Command, []byte(expected.command)) != 0 {
			t.Errorf("Command should have parsed to '%s', but was '%s'", expected.command, val.Command)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}

		if bytes.Compare(val.FirstArg, []byte(val.FirstArg)) != 0 {
			t.Errorf("FirstArgw as not parsed correctly. Expected %q, got %q", expected.firstArg, val.FirstArg)
		}
	}
}

func TestReadRError(t *testing.T) {
	testData := []struct {
		input string
		value string
	} {
		{"-SOME ERROR\r\n", "SOME ERROR"},
	}

	for _, expected := range testData {
		reader := getReader(expected.input)

		val, err := ReadRError(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
		}

		if bytes.Compare(val.Value, []byte(expected.value)) != 0 {
			t.Errorf("Command should have parsed to '%s', but was '%s'", expected.value, val.Value)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}
	}
}

func TestReadRInteger(t *testing.T) {
	testData := []struct {
		input string
		value int
	} {
		{":1\r\n", 1},
		{":9001\r\n", 9001},
		{":9223372036854775807\r\n", 1<<63 - 1},
	}

	for _, expected := range testData {
		reader := getReader(expected.input)

		val, err := ReadRInteger(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
		}

		if val.Value != expected.value {
			t.Errorf("Value should have parsed to %d, but was %d", expected.value, val.Value)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}
	}
}

func TestReadRArray(t *testing.T) {
	testData := []struct {
		input string
		arg1 string
		arg2 string
		count int
	} {

		{"*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n", "keys", "*", 1},
		{"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n", "KEYS", "*", 1},
		{"*1\r\n$4\r\nquit\r\n", "quit", "", 0},
		{"*1\r\n$4\r\nQUIT\r\n", "QUIT", "", 0},
		{
			"*2\r\n$40\r\n1234567890123456789012345678901234567890\r\n$30\r\n123456789012345678901234567890\r\n",
			"1234567890123456789012345678901234567890",
			"123456789012345678901234567890",
			1,
		},
		{
			"*5\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$4\r\nkey4\r\n",
			"del",
			"key1",
			4,
		},
		// Handle Null Bulk Strings (http://redis.io/topics/protocol)
		{"*2\r\n$3\r\ndel\r\n$-1\r\n", "del", string(NIL_STRING), 1},
	}

	for _, expected := range testData {
		t.Logf("Testing %q", expected.input)
		reader := getReader(expected.input)

		val, err := ReadRArray(reader)
		if err != nil {
			t.Errorf("Got error parsing %q: %s", expected.input, err)
			continue
		}

		if val.Count != expected.count {
			t.Errorf("Count should have parsed to %d, but was %d", expected.count, val.Count)
		}

		if bytes.Compare(val.Buffer, []byte(expected.input)) != 0 {
			t.Errorf("Input buffer was not stored correctly. Expected %q, got %q", expected.input, val.Buffer)
		}

		if bytes.Compare(val.FirstValue, []byte(expected.arg1)) != 0 {
			t.Errorf("First arg was not stored correctly. Expected %q, got %q", expected.arg1, val.FirstValue)
		}

		if bytes.Compare(val.SecondValue, []byte(expected.arg2)) != 0 {
			t.Errorf("Second arg was not stored correctly. Expected %q, got %q", expected.arg2, val.SecondValue)
		}
	}
}
