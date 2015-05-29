package protocol

import (
	"bytes"
)

type InlineCommand struct {
	Buffer []byte
	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func NewInlineCommand() *InlineCommand {
	c := &InlineCommand{}
	c.Buffer = nil
	c.Command = nil
	c.FirstArg = nil
	return c
}

func ParseInlineCommand(b []byte) (*InlineCommand, error) {
	c := NewInlineCommand()

	// Copy the bytes, probably not going to have access to that dataspace later
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	parts := bytes.Split(c.Buffer[:len(c.Buffer) - 2], []byte(" "))

	for i, part := range parts {
		if i == 0 {
			c.Command = part

			for i := 0; i < len(c.Command); i++ {
				if c.Command[i] >= 'A' && c.Command[i] <= 'Z' {
					c.Command[i] = c.Command[i] + 0x20
				}
			}

			continue
		}

		if len(part) == 0 {
			continue
		}

		if c.FirstArg == nil {
			c.FirstArg = part
		}

		c.ArgCount++
	}

	return c, nil
}

// Satisfy Command Interface
func (this *InlineCommand) GetCommand() []byte {
	return this.Command
}

func (this *InlineCommand) GetBuffer() []byte {
	return this.Buffer
}

func (this *InlineCommand) GetFirstArg() []byte {
	return this.FirstArg
}

func (this *InlineCommand) GetArgCount() int {
	return this.ArgCount
}
