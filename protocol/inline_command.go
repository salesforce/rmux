package protocol

import (
	"bufio"
	"bytes"
)

type InlineCommand struct {
	Buffer []byte
	buffer [64]byte

	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func NewInlineCommand() *InlineCommand {
	bs := &InlineCommand{}
	bs.Buffer = bs.buffer[0:0]
	bs.Command = nil
	bs.FirstArg = nil
	return bs
}

func ReadInlineCommand(r *bufio.Reader) (*InlineCommand, error) {
	command := NewInlineCommand()

	// TODO: Handle the isPrefix return value as an error condition... but how?
	fullBuffer, _, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	command.Buffer = append(command.Buffer, fullBuffer...)
	command.Buffer = append(command.Buffer, "\r\n"...)

	// Defer lowercasing the resulting command name
	defer func() {
		for i := 0; i < len(command.Command); i++ {
			if command.Command[i] >= 'A' && command.Command[i] <= 'Z' {
				command.Command[i] = command.Command[i] + 0x20
			}
		}
	}()

	command.ArgCount = -1

	bufSlice := command.Buffer
	for i := 0; len(bufSlice) > 0; i++ {
		var part []byte
		command.ArgCount++

		spacePos := bytes.IndexByte(bufSlice, ' ')
		if spacePos == -1 {
			// Couldn't find it! The part is the buffer up to the newline.
			part = bufSlice[:len(bufSlice)-2]
			// Although, if the part is empty, we don't have a real arg - need to decrement the arg counter
			if len(part) == 0 {
				command.ArgCount--
			}
			bufSlice = bufSlice[0:0]
		} else {
			part = bufSlice[:spacePos]

			// Skip the spacePos var past the space
			for bufSlice[spacePos] == ' ' {
				spacePos++
			}
			bufSlice = bufSlice[spacePos:]
		}

		if i == 0 {
			command.Command = part
		} else if i == 1 {
			command.FirstArg = part
		}
	}
	return command, nil
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
