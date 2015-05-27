package protocol

import (
	"bufio"
	"fmt"
)

type StringCommand struct {
	Buffer []byte
	// Every string command *should* fit within 20 bytes. The slice above will resize properly in append() if not though.
	buffer [20]byte
}

func NewStringCommand() *StringCommand {
	sc := &StringCommand{}
	sc.Buffer = sc.buffer[0:0]
	return sc
}

func ReadStringCommand(reader *bufio.Reader) (*StringCommand, error) {
	sc := NewStringCommand()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '$' {
		return nil, fmt.Errorf("Expected '$', got '%c'", firstByte)
	}
	sc.Buffer = append(sc.Buffer, firstByte)

	lenStr, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	sc.Buffer = append(sc.Buffer, lenStr...)
	sc.Buffer = append(sc.Buffer, "\r\n"...)

	len, err := ParseInt(lenStr)
	if err != nil {
		return nil, err
	}

	// Also read the newline
	strSlice := make([]byte, len+2)
	_, err = reader.Read(strSlice)
	if err != nil {
		return nil, err
	}

	sc.Buffer = append(sc.Buffer, strSlice...)

	return sc, nil
}

// Satisfy Command Interface
func (this *StringCommand) GetCommand() []byte {
	return nil
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
