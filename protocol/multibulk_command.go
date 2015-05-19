package protocol

import (
	"bufio"
	"fmt"
	"strconv"
)

var NIL_STRING []byte = nil

type MultibulkCommand struct {
	Buffer []byte
	// Internal buffer that the slice above is initialized against to avoid allocation past the struct allocation in most cases.
	buffer [64]byte

	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func NewMultibulkCommand() *MultibulkCommand {
	bs := &MultibulkCommand{}
	bs.Buffer = bs.buffer[0:0]
	return bs
}

func ReadMultibulkCommand(r *bufio.Reader) (*MultibulkCommand, error) {
	command := NewMultibulkCommand()

	command.readByte(r, '*')

	// Read the number of parts
	partCount, err := command.readLength(r)
	if err != nil {
		return nil, err
	}
	command.ArgCount = partCount - 1

	// Defer lowercasing the command
	defer func() {
		for i := 0; i < len(command.Command); i++ {
			// lowercase it
			if char := command.Command[i]; char >= 'A' && char <= 'Z' {
				command.Command[i] = command.Command[i] + 0x20
			}
		}
	}()

	for i := 0; i < partCount; i++ {
		part, err := command.readString(r)
		if err != nil {
			return nil, err
		}

		if i > 1 {
			continue
		}

		// Determine start/end points of the part in the buffer. command.Buffer should contain everything read so far...
		partLen := len(part)
		partEndPos := len(command.Buffer) - 2 // without \r\n
		partStartPos := partEndPos - partLen

		defer func(startPos, endPos, i int) {
			part := command.Buffer[startPos:endPos]
			if i == 0 {
				command.Command = part
			} else if i == 1 {
				command.FirstArg = part
			}
		}(partStartPos, partEndPos, i)
	}

	return command, nil
}

// Reads a byte, checks that it is what is expected, and appends to the internal buffer
func (this *MultibulkCommand) readByte(reader *bufio.Reader, expected byte) error {
	if read, err := reader.ReadByte(); err != nil {
		return err
	} else if read != expected {
		return fmt.Errorf("Expected %q, got %q", expected, read)
	} else {
		this.Buffer = append(this.Buffer, read)
		return nil
	}
}

// Reads a length variable. This can be either the size of an array or length of a string.
func (this *MultibulkCommand) readLength(reader *bufio.Reader) (intval int, err error) {
	read, _, err := reader.ReadLine()
	if err != nil {
		return intval, err
	}

	// parse the int, return
	this.Buffer = append(this.Buffer, read...)
	this.Buffer = append(this.Buffer, "\r\n"...)

	ParseInt(read)
	intval, err = strconv.Atoi(string(read))

	return intval, err
}

func (this *MultibulkCommand) readString(reader *bufio.Reader) (stringValue []byte, err error) {
	// TODO: Ability to read integers, which start with colon (:)
	// First read the length of the string
	if err := this.readByte(reader, '$'); err != nil {
		return nil, err
	}

	length, err := this.readLength(reader)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return NIL_STRING, nil
	}

	bytes, err := this.readBytes(reader, length)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (this *MultibulkCommand) readBytes(reader *bufio.Reader, length int) (bytes []byte, err error) {
	if length < 0 {
		return []byte{}, nil
	}

	slice := make([]byte, length)
	count, err := reader.Read(slice)
	if err != nil {
		return nil, err
	} else if count < length {
		return nil, fmt.Errorf("Read less bytes than expected. (%d / %d)", count, length)
	}

	this.Buffer = append(this.Buffer, slice...)

	if err := this.readByte(reader, '\r'); err != nil {
		return nil, err
	}
	if err := this.readByte(reader, '\n'); err != nil {
		return nil, err
	}

	return slice, nil
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
