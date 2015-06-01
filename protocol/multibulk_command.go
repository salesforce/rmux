package protocol

import (
	"bytes"
)

var NIL_STRING []byte = nil

type MultibulkCommand struct {
	Buffer  []byte
	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func ParseMultibulkCommand(b []byte) (*MultibulkCommand, error) {
	c := &MultibulkCommand{}
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	if c.Buffer[0] != '*' {
		return nil, ERROR_COMMAND_PARSE
	}

	newlinePos := bytes.Index(c.Buffer, REDIS_NEWLINE)
	if newlinePos < 0 {
		return nil, ERROR_COMMAND_PARSE
	}

	count, err := ParseInt(c.Buffer[1:newlinePos])
	if err != nil {
		return nil, err
	}

	if count > 0 {
		c.ArgCount = count - 1
	}

	cBuf := c.Buffer[newlinePos+2:]
	for i := 0; i < 2 && i < count; i++ {
		if cBuf[0] != '$' {
			return nil, ERROR_COMMAND_PARSE
		}

		newlinePos := bytes.Index(cBuf, REDIS_NEWLINE)
		if newlinePos < 0 {
			return nil, ERROR_COMMAND_PARSE
		}

		count, err := ParseInt(cBuf[1:newlinePos])
		if err != nil {
			return nil, err
		} else if count < 0 {
			cBuf = cBuf[newlinePos+2:]
			continue
		}

		if i == 0 {
			c.Command = cBuf[newlinePos+2 : newlinePos+2+count]
		} else {
			c.FirstArg = cBuf[newlinePos+2 : newlinePos+2+count]
		}

		cBuf = cBuf[newlinePos+2+count+2:]
	}

	for i := 0; i < len(c.Command); i++ {
		if char := c.Command[i]; char >= 'A' && char <= 'Z' {
			c.Command[i] = c.Command[i] + 0x20
		}
	}

	return c, nil
}

// Satisfy Command Interface
func (this *MultibulkCommand) GetCommand() []byte {
	return this.Command
}

func (this *MultibulkCommand) GetBuffer() []byte {
	return this.Buffer
}

func (this *MultibulkCommand) GetFirstArg() []byte {
	return this.FirstArg
}

func (this *MultibulkCommand) GetArgCount() int {
	return this.ArgCount
}
