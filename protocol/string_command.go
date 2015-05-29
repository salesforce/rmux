package protocol

import (
	"bytes"
)

type StringCommand struct {
	Buffer []byte
	Command []byte
}

func ParseStringCommand(b []byte) (*StringCommand, error) {
	c := &StringCommand{}
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	if c.Buffer[0] != '$' {
		return nil, ERROR_COMMAND_PARSE
	}

	newlinePos := bytes.Index(c.Buffer, REDIS_NEWLINE)
	if newlinePos < 0 {
		return nil, ERROR_COMMAND_PARSE
	}

	strLen, err := ParseInt(c.Buffer[1:newlinePos])
	if err != nil {
		return nil, err
	} else if strLen < 0 {
		return c, err
	}

	c.Command = c.Buffer[newlinePos + 2:newlinePos + 2 + strLen]
	for i := 0; i < len(c.Command); i++ {
		// lowercase it
		if char := c.Command[i]; char >= 'A' && char <= 'Z' {
			c.Command[i] = c.Command[i] + 0x20
		}
	}

	return c, nil
}

// Satisfy Command Interface
func (this *StringCommand) GetCommand() []byte {
	return this.Command
}

func (this *StringCommand) GetBuffer() []byte {
	return this.Buffer
}

func (this *StringCommand) GetFirstArg() []byte {
	return nil
}

func (this *StringCommand) GetArgCount() int {
	return 0
}
