package protocol

import (
	"bufio"
	"fmt"
)

type SimpleCommand struct {
	Command []byte
	Buffer  []byte
	// Every simple command *should* fit within 20 bytes. The slice above will resize properly in append() if not though.
	buffer [20]byte
}

func NewSimpleCommand() *SimpleCommand {
	sc := &SimpleCommand{}
	sc.Buffer = sc.buffer[0:0]
	return sc
}

func ReadSimpleCommand(reader *bufio.Reader) (*SimpleCommand, error) {
	sc := NewSimpleCommand()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '+' {
		return nil, fmt.Errorf("Expected '+', got '%c'", firstByte)
	}
	sc.Buffer = append(sc.Buffer, firstByte)

	read, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	sc.Buffer = append(sc.Buffer, read...)
	sc.Command = sc.Buffer[1:]
	for i := 0; i < len(sc.Command); i++ {
		// lowercase it
		if char := sc.Command[i]; char >= 'A' && char <= 'Z' {
			sc.Command[i] = sc.Command[i] + 0x20
		}
	}

	sc.Buffer = append(sc.Buffer, "\r\n"...)

	return sc, nil
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
