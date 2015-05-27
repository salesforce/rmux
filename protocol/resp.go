package protocol

import (
	"bufio"
	"bytes"
	"fmt"
)

// resp.go: REdis Serialization Protocol
// Responsible for parsing redis data from a reader

// ================== Base =================
type RespData interface {
	GetBuffer() []byte
}

type respData struct {
	Buffer []byte
	buffer [16]byte
}

type respCommand struct {
	respData
	Command  []byte
	FirstArg []byte
	ArgCount int
}

func (this respData) init() {
	this.Buffer = this.buffer[:0]
}

// =============== Simple String ==============
type RSimpleString struct {
	respData
	Value []byte
}

func NewRSimpleString() *RSimpleString {
	ss := &RSimpleString{}
	ss.init()
	return ss
}

func ReadRSimpleString(reader *bufio.Reader) (*RSimpleString, error) {
	ss := NewRSimpleString()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '+' {
		return nil, fmt.Errorf("Expected '+', got '%c'", firstByte)
	}
	ss.Buffer = append(ss.Buffer, firstByte)

	read, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	ss.Buffer = append(ss.Buffer, read...)
	ss.Buffer = append(ss.Buffer, "\r\n"...)
	ss.Value = ss.Buffer[1 : len(ss.Buffer)-2]

	return ss, nil
}

func (this RSimpleString) GetBuffer() []byte {
	return this.Buffer
}

// =============== Bulk String ==============
type RBulkString struct {
	respData
	Value []byte
}

func NewRBulkString() *RBulkString {
	ss := &RBulkString{}
	ss.init()
	return ss
}

func ReadRBulkString(reader *bufio.Reader) (*RBulkString, error) {
	bs := NewRBulkString()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '$' {
		return nil, fmt.Errorf("Expected '$', got '%c'", firstByte)
	}
	bs.Buffer = append(bs.Buffer, firstByte)

	lenStr, isPrefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	} else if isPrefix {
		return nil, ERROR_INVALID_COMMAND_FORMAT
	}
	length, err := ParseInt(lenStr)
	if err != nil {
		return nil, err
	}

	bs.Buffer = append(bs.Buffer, lenStr...)
	bs.Buffer = append(bs.Buffer, "\r\n"...)

	if length == -1 {
		// 'nil' string
		return bs, nil
	}

	// Also read the newline
	strSlice := make([]byte, length+2)
	_, err = reader.Read(strSlice)
	if err != nil {
		return nil, err
	}

	bs.Buffer = append(bs.Buffer, strSlice...)
	bs.Value = bs.Buffer[len(bs.Buffer)-length-2 : len(bs.Buffer)-2]

	return bs, nil
}

func (this RBulkString) GetBuffer() []byte {
	return this.Buffer
}

// =============== Errors ==============
type RError struct {
	respData
	Value []byte
}

func NewRError() *RError {
	obj := &RError{}
	obj.init()
	return obj
}

func ReadRError(reader *bufio.Reader) (*RError, error) {
	re := NewRError()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '-' {
		return nil, fmt.Errorf("Expected '-', got '%c'", firstByte)
	}
	re.Buffer = append(re.Buffer, firstByte)

	read, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	re.Buffer = append(re.Buffer, read...)
	re.Buffer = append(re.Buffer, "\r\n"...)
	re.Value = re.Buffer[1 : len(re.Buffer)-2]

	return re, nil
}

func (this RError) GetBuffer() []byte {
	return this.Buffer
}

// =============== Integer ==============
type RInteger struct {
	respData
	Value int
}

func NewRInteger() *RInteger {
	obj := &RInteger{}
	obj.init()
	return obj
}

func ReadRInteger(reader *bufio.Reader) (*RInteger, error) {
	ri := NewRInteger()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != ':' {
		return nil, fmt.Errorf("Expected ':', got '%c'", firstByte)
	}
	ri.Buffer = append(ri.Buffer, firstByte)

	read, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	ri.Buffer = append(ri.Buffer, read...)
	ri.Buffer = append(ri.Buffer, "\r\n"...)

	value, err := ParseInt(ri.Buffer[1 : len(ri.Buffer)-2])
	if err != nil {
		return nil, err
	}
	ri.Value = value

	return ri, nil
}

func (this RInteger) GetBuffer() []byte {
	return this.Buffer
}

// =============== Inline String ==============
// Inline strings are always commands. You will never see them not be commands.
type RInlineString struct {
	respCommand
}

func NewRInlineString() *RInlineString {
	obj := &RInlineString{}
	obj.init()
	return obj
}

func ReadRInlineString(reader *bufio.Reader) (*RInlineString, error) {
	is := NewRInlineString()

	// TODO: Handle the isPrefix return value as an error condition... but how?
	fullBuffer, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	is.Buffer = append(is.Buffer, fullBuffer...)
	is.Buffer = append(is.Buffer, "\r\n"...)
	is.ArgCount = -1

	bufSlice := is.Buffer
	for i := 0; len(bufSlice) > 0; i++ {
		var part []byte
		is.ArgCount++

		spacePos := bytes.IndexByte(bufSlice, ' ')
		if spacePos == -1 {
			// Couldn't find it! The part is the buffer up to the newline.
			part = bufSlice[:len(bufSlice)-2]
			// Although, if the part is empty, we don't have a real arg - need to decrement the arg counter
			if len(part) == 0 {
				is.ArgCount--
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
			is.Command = part
		} else if i == 1 {
			is.FirstArg = part
		}
	}
	return is, nil
}

func (this RInlineString) GetBuffer() []byte {
	return this.Buffer
}

// =============== Array ==============
type RArray struct {
	respData
	FirstValue  []byte
	SecondValue []byte
	Count       int
}

func NewRArray() *RArray {
	obj := &RArray{}
	obj.init()
	return obj
}

func ReadRArray(reader *bufio.Reader) (data *RArray, err error) {
	ra := NewRArray()

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	} else if firstByte != '*' {
		return nil, fmt.Errorf("Expected '*', got '%c'", firstByte)
	}
	ra.Buffer = append(ra.Buffer, firstByte)

	// Read the number of parts
	partCountStr, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	ra.Buffer = append(ra.Buffer, partCountStr...)
	ra.Buffer = append(ra.Buffer, "\r\n"...)
	partCount, err := ParseInt(partCountStr)
	if err != nil {
		return nil, err
	}
	ra.Count = partCount - 1

	for i := 0; i < partCount; i++ {
		part, err := ReadResp(reader)
		if err != nil {
			return nil, err
		}

		ra.Buffer = append(ra.Buffer, part.GetBuffer()...)

		if i > 1 {
			continue
		}

		respBulkStr, isBulkString := part.(*RBulkString)
		if isBulkString {
			if i == 0 {
				ra.FirstValue = respBulkStr.Value
			} else if i == 1 {
				ra.SecondValue = respBulkStr.Value
			}
		}
	}

	return ra, nil
}

func (this RArray) GetBuffer() []byte {
	return this.Buffer
}

// ============== Unified parser =============
func ReadResp(reader *bufio.Reader) (data RespData, err error) {
	peeked, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}

	peek := peeked[0]
	Debug("Peeked: %c", peek)
	switch {
	case peek == '+':
		data, err = ReadRSimpleString(reader)
	case peek == '*':
		data, err = ReadRArray(reader)
	case peek == '$':
		data, err = ReadRBulkString(reader)
	case peek == ':':
		data, err = ReadRInteger(reader)
	case peek == '-':
		data, err = ReadRError(reader)
	case (peek >= 'a' && peek <= 'z') || (peek >= 'A' && peek <= 'Z'):
		data, err = ReadRInlineString(reader)
	default:
		data, err = nil, ERROR_INVALID_COMMAND_FORMAT
	}

	return data, err
}
