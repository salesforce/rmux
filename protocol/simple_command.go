package protocol

import (
	"fmt"
)

type SimpleCommand struct {
	Buffer  []byte
	Command []byte
}

func ParseSimpleCommand(b []byte) (*SimpleCommand, error) {
	c := &SimpleCommand{}
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	if c.Buffer[0] != '+' {
		return nil, fmt.Errorf("Expected '+', got '%c'", c.Buffer[0])
	}

	c.Command = c.Buffer[1 : len(c.Buffer)-2]
	for i := 0; i < len(c.Command); i++ {
		// lowercase it
		if char := c.Command[i]; char >= 'A' && char <= 'Z' {
			c.Command[i] = c.Command[i] + 0x20
		}
	}

	return c, nil
}

// Satisfy Command Interface
func (this *SimpleCommand) GetCommand() []byte {
	return this.Command
}

func (this *SimpleCommand) GetBuffer() []byte {
	return this.Buffer
}

func (this *SimpleCommand) GetFirstArg() []byte {
	return nil
}

func (this *SimpleCommand) GetArgCount() int {
	return 0
}
