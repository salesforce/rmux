//Copyright (c) 2013, Salesforce.com, Inc.
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
//	Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//	Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//	Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"time"
)

const (
	//This is set to match bufio's default buffer size, so taht we can safely read&ignore large chunks of data when necessary
	BUFFER_SIZE = 4096
)

var (
	//The refuse heap is a dumping ground for ignored data
	REFUSE_HEAP = [BUFFER_SIZE]byte{}

	//Used when we are trying to parse the size of a bulk or multibulk message, and do not receive a valid number
	ERROR_INVALID_INT = errors.New("Did not receive valid int value")
	//Used when we inspect a packet, and it is using the deprecated messaging format
	ERROR_MULTIBULK_FORMAT_REQUIRED = errors.New("Multibulk format is required")
	//Used when we expect a redis bulk-format payload, and do not receive one
	ERROR_BAD_BULK_FORMAT = errors.New("Bad bulk format supplied")
	ERROR_COMMAND_PARSE = errors.New("Command parse error")

	//Commands declared once for convenience
	DEL_COMMAND         = []byte{'d', 'e', 'l'}
	SUBSCRIBE_COMMAND   = []byte{'s', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e'}
	UNSUBSCRIBE_COMMAND = []byte{'u', 'n', 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e'}
	PING_COMMAND        = []byte{'p', 'i', 'n', 'g'}
	INFO_COMMAND        = []byte{'i', 'n', 'f', 'o'}
	SHORT_PING_COMMAND  = []byte{'P', 'I', 'N', 'G'}
	SELECT_COMMAND      = []byte{'s', 'e', 'l', 'e', 'c', 't'}
	QUIT_COMMAND        = []byte{'q', 'u', 'i', 't'}

	//Responses declared once for convenience
	OK_RESPONSE   = []byte{'+', 'O', 'K'}
	PONG_RESPONSE = []byte{'+', 'P', 'O', 'N', 'G'}
	ERR_RESPONSE  = []byte{'$', '-', '1'}

	//Redis expects \r\n newlines.  Using this means we can stop remembering that
	REDIS_NEWLINE = []byte{'\r', '\n'}

	//These functions should not be executed through a proxy.
	//If you know what you're doing, you are welcome to execute them directly on your server
	UNSAFE_FUNCTIONS = map[string]bool{
		"auth":         true,
		"bgrewriteaof": true,
		"bgsave":       true,
		"client":       true,
		"config":       true,
		"dbsize":       true,
		"discard":     true,
		"debug":        true,
		"exec":        true,
		"flushall":     true,
		"flushdb":      true,
		"lastsave":     true,
		"move":         true,
		"monitor":      true,
		"migrate":      true,
		"multi":       true,
		"object":       true,
		"punsubscribe": true,
		"psubscribe":   true,
		"pubsub":       true,
		"randomkey":    true,
		"save":         true,
		"shutdown":     true,
		"slaveof":      true,
		"slowlog":      true,
		"sync":         true,
		"time":         true,
		"unsubscribe":  true, // TODO, build in unsubscribe support to pubsub handlers
		"unwatch":     true,
		"watch":       true,
	}

	//These functions will only work if multiplexing is disabled.
	//It would be rather worthless to watch on one server, multi on another, and increment on a third
	SINGLE_DB_FUNCTIONS = map[string]bool{
		"bitop":       true,
		"brpoplpush":  true,
		"eval":        true,
		"keys":        true,
		"mget":        true,
		"mset":        true,
		"msetnx":      true,
		"rename":      true,
		"renamenx":    true,
		"rpoplpush":   true,
		"script":      true,
		"sdiff":       true,
		"sdiffstore":  true,
		"sinter":      true,
		"sinterstore": true,
		"smove":       true,
		"sunion":      true,
		"sunionstore": true,
		"zinterstore": true,
		"zunionstore": true,
	}

	//Only Publish/Subscribe are supported at this time.  Unsubscribe will come later
	PUBSUB_FUNCTIONS = map[string]bool{
		"subscribe": true,
	}
)

func IsSupportedFunction(command [20]byte, commandLength int, isMultiplexing, isMultipleArgument bool) (bool) {
	if command[0] == 'd' {
		//*del is only supported if we're not multiplexing, or there's only one argument
		if command[2] == 'l' && isMultipleArgument {
			return false
		}
		//supported: decr, decrby, del, dump
		//unsupported: debug, dbsize, discard
		return (command[1] == 'e' || command[1] == 'u') && command[2] != 'b'
	} else if command[0] == 'g' {
		//supported: get, getbit, getrange, getset
		return true
	} else if command[0] == 's' {
		//supported: select, set, setbit, setex, setnx, setrange, sort, spop, srandmember, srem, strlen
		if command[1] == 'e' || command[1] == 'o' || command[1] == 'p' || command[1] == 'r' || command[1] == 't' {
			return true
		}
		//supported: sadd
		//unsupported: save
		if command[1] == 'a' {
			return command[2] == 'd'
		}
		//unsupported: shutdown. slaveof, slowlog, sync
		if command[1] == 'a' || command[1] == 'h' || command[1] == 'l' || command[1] == 'y' {
			return false
		}
		//supported if multiplexing is disabled: script, sdiff, sdiffstore, sinter, sinterstore, smove, sunion, sunionstore
		if isMultiplexing {
			return true
		}
		//supported: scard
		if command[1] == 'c' && command[2] != 'a' {
			return true
		} else if command[1] == 'i' && command[2] == 's' {
			return true
		} else if command[1] == 'm' && command[2] == 'e' {
			return true
		} else if command[1] == 'u' && command[2] == 'b' {
			return true
		}
	} else if command[0] == 'h' {
		//supported: hdel, hexists, hget, hgetall, hincrby, hincrbyfloat, hkeys, hlen, hmget, hmset, hsetnx, hvals
		return true
	} else if command[0] == 'i' {
		//supported: incr, incrby, incrbyfloat
		return true
	} else if command[0] == 'l' {
		//unsupported: lastsave
		//supported: lindex, linsert, llen, lpop, lpush, lpushx, lrange, lrem, lset, ltrim
		return command[1] != 'a'
	} else if command[0] == 'z' {
		//supported if multiplexing is disabled: zinterstore, zunionstore
		//supported: everything else
		if !isMultiplexing {
			return true
		}
		//supported if multiplexing is disabled: zinterstore, zunionstore
		if command[1] == 'i' && command[3] == 't' || command[1] == 'u' {
			return false
		}
	} else if command[0] == 'p' {
		//supported: persist, pexpire, pexpireat, ping, psetex, pttl, publish
		//unsupported: punsubscribe, psubscribe, pubsub
		return command[1] != 'u' || (commandLength == 7 && !isMultipleArgument)
	} else if command[0] == 'q' {
		return true
	} else if command[0] == 'r' {
		//supported: rpop, rpush, rpushx
		//supported if multiplexing is disabled: rename, renamenx, rpoplpush
		//unsupported: randomkey, restore
		if command[1] == 'p' &&  commandLength < 8 {
			return true
		} else if !isMultiplexing {
			return command[1] != 'a' && command[2] != 's'
		}
		return false
	} else if command[0] == 't' {
		//supported: time, ttl, type
		return true
	} else if command[0] == 'u' {
		//unsupported: unsubscribe (TODO), unwatch
		return false
	} else if command[0] == 'w' {
		//unsupported: watch
		return false
	} else if command[0] == 'a' {
		//supported: append
		//unsupported: auth
		return command[1] == 'p'
	} else if command[0] == 'b' {
		//supported: bitcount
		if commandLength == 8 {
			return true
		}
		//unsupported: bgsave, bgwriteaof
		if command[1] == 'g' {
			return false;
		}
		//supported if not multiplexing: bitop, brpop, blpop, brpoplpush
		return !isMultiplexing
	} else if command[0] == 'c' {
		//unsupported: client, config
		return false
	} else if command[0] == 'e' {
		//unsupported: exec
		if command[2] == 'e' {
			return false
		}
		//supported: echo, exists, expire, expireat
		//supported if not multiplexing: eval, evalsha
		return command[1] != 'v' || !isMultiplexing
	} else if command[0] == 'f' {
		//unsupported: flushall, flushdb
		return false
	} else if command[0] == 'k' {
		//supported if not multiplexing: keys
		return !isMultiplexing
	} else if command[0] == 'm' {
		//supported if not multiplexing: mget, mset, msetnx
		//unsupported: move, monitor, migrate, multi
		if isMultiplexing {
			return false
		}
		return command[1] == 'g' || command[1] == 's'
	} else if command[0] == 'o' {
		return false
	}
	return false
}

//Parses a string into an int.
//Differs from atoi in that this only parses positive ints--hex, octal, and negatives are not allowed
//Upon invalid character received, a PANIC_INVALID_INT is caught and err'd
func ParseInt(response []byte) (length int, err error) {
	length = 0
	//It's worth re-inventing the wheel, if you have a good understanding of your particular wheel's usage
	for _, b := range response {
		//Subtract 48 from our byte.  bytes are uint8s, so if the value is below 48, it will wrap-around back to 255 and dec. from there
		b = b - '0'
		//Since we know we have a positive value, we can now do this single check
		if b > 9 {
			Debug("ParseInt: Invalid int character: %s", b)
			err = ERROR_INVALID_INT
			return
		}
		length *= 10
		length += int(b)
	}
	return
}

//Inspects the incoming payload, and returns the command, and first argument for that command if there is one.
//If the packet is not in a valid multibulk format, ERROR_MULTIBULK_FORMAT_REQUIRED is returned
func GetCommand(source *bufio.Reader, command, firstArgument []byte) (commandLength, argumentLength int, err error) {
	var nextLine, messageLength int
	//Peek at everything that we can look at
	contents, err := source.Peek(source.Buffered())
	if err != nil {
		Debug("GetCommand: Error received during peek: %s", err)
		return
	}

	//scan contents until newline
	nextLine = bytes.IndexByte(contents, '\r')

	//If there is no newline in the middle of the message, this is in the old/invalid format
	if nextLine == -1 {
		Debug("GetCommand: No newline found in command\r\n")
		err = ERROR_MULTIBULK_FORMAT_REQUIRED
		return
	}

	//Snag the length of the bulk-message that follows
	commandLength, err = ParseInt(contents[1:nextLine])
	if err != nil {
		Debug("GetCommand: Error received from command's ParseInt: %s\r\n", err)
		return
	}

	//add 2 for newline.
	nextLine = nextLine + 2
	if nextLine+commandLength+2 > len(contents) {
		Debug("GetCommand: Message is not as long as the bulk header suggests\r\n")
		err = ERROR_MULTIBULK_FORMAT_REQUIRED
		return
	}

	//and then snag the command out of the source
	copy(command, contents[nextLine : nextLine + commandLength])
	if DEBUG {
		Debug("GetCommand: We peeked at %d bytes for command: %s\r\n", commandLength, command)
	}

	for index := 0; index < commandLength; index++ {
		if len(command) < index + 1 {
			Debug("Command length mismatch")
			err = ERROR_COMMAND_PARSE
			return
		}

		//if we have a capital value
		if command[index] <= 'Z' {
			//lowercaseize it
			command[index] += 32
		}
	}

	//Short-circuit if there is no argument
	if nextLine+commandLength+2 == len(contents) {
		return
	}

	//Find the start and end of the next bulk-message header
	messageLength = nextLine + commandLength + 2
	nextLine = bytes.IndexByte(contents[messageLength:], '\r')
	//If there is no newline, this invalid
	if nextLine == -1 {
		Debug("GetCommand: No newline found in argument\r\n")
		err = ERROR_MULTIBULK_FORMAT_REQUIRED
		return
	}
	nextLine = nextLine + messageLength

	//Find the first argument's length
	argumentLength, err = ParseInt(contents[messageLength+1 : nextLine])
	if err != nil {
		Debug("GetCommand: Error received from argument's ParseInt: %s", err)
		return
	}
	nextLine = nextLine + 2

	//If we have less source than we expect, this is also invalid
	if nextLine+argumentLength+2 > len(contents) {
		err = ERROR_MULTIBULK_FORMAT_REQUIRED
		Debug("GetCommand: Argument is not as long as the bulk header suggests")
		return
	}

	//And then read that out of the source as well
	copy(firstArgument, contents[nextLine : nextLine+argumentLength])
	if DEBUG {
		Debug("GetCommand: We peeked at %d bytes for argument: %s\r\n", argumentLength, firstArgument)
	}
	return
}

//Writes the given bytes to destination, with a GO_NEWLINE appended, and then flushes the buffer
//Bubbles up any errors from the underlying writer
func FlushLine(line []byte, destination *bufio.Writer) (err error) {
	err = writeLine(line, destination)
	if err != nil {
		Debug("FlushLine: Error received from writeLine: %s", err)
		return
	}
	startTime := time.Now()
	err = destination.Flush()
	Debug("FlushLine: Time to flush: %s\r\n", time.Since(startTime))
	return
}

//Writes the given line to the buffer, followed by a GO_NEWLINE
//Does not explicitly flush the buffer.  Final lines in a sequence should be followed by FlushLine
func writeLine(line []byte, destination *bufio.Writer) (err error) {
	startTime := time.Now()
	_, err = destination.Write(line)
	if err != nil {
		Debug("writeLine: Error received from write: %s", err)
		return
	}

	_, err = destination.Write(REDIS_NEWLINE)
	if err != nil {
		Debug("writeLine: Error received from writing GO_NEWLINE: %s", err)
		return
	}
	Debug("writeLine: Time to write line: %s\r\n", time.Since(startTime))
	return
}

//Ignores a single bulk message from the source reader, beginning with firstLine
//Not needed publicly, since client implementations should always use the multibulk format
func ignoreBulkMessage(firstLine []byte, source *bufio.Reader) (err error) {
	var copied int
	if len(firstLine) < 2 || firstLine[0] != '$' {
		err = ERROR_BAD_BULK_FORMAT
		Debug("ignoreBulkMessage: Invalid first line sequence")
		return
	}

	if bytes.Equal(firstLine, ERR_RESPONSE) {
		//If we have a $-1 error, that's an entire response.  ignore and return
		return
	}

	n, err := ParseInt(firstLine[1:])
	if err != nil {
		Debug("ignoreBulkMessage: Error received from ParseInt")
		return
	}

	if DEBUG {
		Debug("I think we want to ignore %d bytes: %s\r\n", n, firstLine)
	}
	//As long as we have more stuff to ignore
	for n > 0 {
		//We're using a throw-away buffer, so that we don't have to re-allocate a buffer each time, that we don't care about
		if n > BUFFER_SIZE {
			copied, err = source.Read(REFUSE_HEAP[0:BUFFER_SIZE])
		} else {
			copied, err = source.Read(REFUSE_HEAP[0:n])
		}
		if err != nil || copied == 0 {
			return
		}
		n -= copied
	}

	char, err := source.ReadByte()
	if err != nil {
		Debug("ignoreBulkMessage: Error received from readByte: %s", err)
		return
	}
	if char != '\r' {
		Debug("ignoreBulkMessage: Missing carriage-return character", err)
		err = ERROR_BAD_BULK_FORMAT
		return
	}

	char, err = source.ReadByte()
	if err != nil {
		Debug("ignoreBulkMessage: Error received from readByte: %s", err)
		return
	}
	if char != '\n' {
		Debug("ignoreBulkMessage: Missing newline character", err)
		err = ERROR_BAD_BULK_FORMAT
		return
	}

	return
}

//Ignores a multi-bulk message from the source reader, beginning with firstLine
//Bubbles up any underlying protocol or buffer error
func IgnoreMultiBulkMessage(firstLine []byte, source *bufio.Reader) (err error) {
	//validate format
	if len(firstLine) < 2 || firstLine[0] != '*' {
		err = ERROR_BAD_BULK_FORMAT
		Debug("IgnoreMultiBulkMessage: Invalid multibulk response first line")
		return
	}

	//Snag out the amount of lines coming in
	n, err := ParseInt(firstLine[1:])
	if err != nil {
		Debug("IgnoreMultiBulkMessage: Error received from ParseInt: %s", err)
		return
	}

	if DEBUG {
		Debug("IgnoreMultiBulkMessage: We have %d lines to return: %s\r\n", n, firstLine)
	}
	for i := 0; i < n; i++ {
		Debug("IgnoreMultiBulkMessage: Working on line %d\r\n", i)
		firstLine, _, err = source.ReadLine()
		if err != nil {
			Debug("IgnoreMultiBulkMessage: Error received from ReadLine attempt: %s", err)
			return
		}
		if firstLine[0] == ':' {
			continue
		}
		err = ignoreBulkMessage(firstLine, source)
		if err != nil {
			Debug("IgnoreMultiBulkMessage: Error received from ignoreBulkMessage: %s", err)
			return
		}
	}
	return
}

//Copies a single bulk message from source to destination, beginning with firstLine
//If a protocol or a buffer error is encountered, it is bubbled up
func copyBulkMessage(firstLine []byte, destination *bufio.Writer, source *bufio.Reader) (err error) {
	if len(firstLine) < 2 || firstLine[0] != '$' {
		err = ERROR_BAD_BULK_FORMAT
		Debug("copyBulkMessage: Invalid bulk response first line")
		return
	}

	if bytes.Equal(firstLine, ERR_RESPONSE) {
		//If we have a $-1, that's an error.  write and flush
		err = writeLine(ERR_RESPONSE, destination)
		if err != nil {
			Debug("copyBulkMessage: Error received from writeLine: %s", err)
		}
		return
	}

	//add two for the newline
	n, err := ParseInt(firstLine[1:])
	if err != nil {
		Debug("copyBulkMessage: Error received from ParseInt: %s", err)
		return
	}

	if DEBUG {
		Debug("copyBulkMessage: I think we want %d bytes: %s\r\n", n, firstLine)
	}

	err = writeLine(firstLine, destination)
	if err != nil {
		Debug("copyBulkMessage: Error received from writeLine: %s", err)
		return
	}

	written, err := io.CopyN(destination, source, int64(n))
	if err != nil {
		Debug("copyBulkMessage: Error received from io.CopyN: %s", err)
		return
	}

	if written != int64(n) {
		Debug("copyBulkMessage: Ran out of bytes to copy: %s", err)
		return
	}

	char, err := source.ReadByte()
	if err != nil {
		Debug("copyBulkMessage: Error received from readByte: %s", err)
		return
	}

	if char != '\r' {
		Debug("copyBulkMessage: Missing carriage-return character", err)
		err = ERROR_BAD_BULK_FORMAT
		return
	}

	char, err = source.ReadByte()
	if err != nil {
		Debug("copyBulkMessage: Error received from readByte: %s", err)
		return
	}
	if char != '\n' {
		Debug("copyBulkMessage: Missing newline character", err)
		err = ERROR_BAD_BULK_FORMAT
		return
	}

	_, err = destination.Write(REDIS_NEWLINE)
	if err != nil {
		Debug("copyBulkMessage: Error received from write: %s", err)
		return
	}

	return
}

//Copies a multi bulk message from source to destination, beginning with firstLine
//If a protocol or a buffer error is encountered, it is bubbled up
func CopyMultiBulkMessage(firstLine []byte, destination *bufio.Writer, source *bufio.Reader) (err error) {
	//validate format
	if len(firstLine) < 2 || firstLine[0] != '*' {
		err = ERROR_BAD_BULK_FORMAT
		Debug("IgnoreMultiBulkMessage: Invalid multibulk response first line")
		return
	}

	n, err := ParseInt(firstLine[1:])
	if err != nil {
		Debug("CopyMultiBulkMessage: Error received from ParseInt: %s", err)
		return
	}

	err = writeLine(firstLine, destination)
	if err != nil {
		Debug("CopyMultiBulkMessage: Error received from writeLine: %s", err)
		return
	}

	Debug("We have %d lines to return: %s\r\n", n, firstLine)
	for i := 0; i < n; i++ {
		Debug("Working on line %d\r\n", i)
		firstLine, _, err = source.ReadLine()
		if err != nil {
			Debug("CopyMultiBulkMessage: Error received from ReadLine: %s", err)
			return
		}
		err = copyBulkMessage(firstLine, destination, source)
		if err != nil {
			Debug("CopyMultiBulkMessage: Error received from copyBulkMessage: %s", err)
			return
		}
	}
	err = destination.Flush()
	if err != nil {
		Debug("CopyMultiBulkMessage: Error received from Flush: %s", err)
		return
	}
	return
}

//Copies a server response from the remoteBuffer into your localBuffer, beginning with firstLine
//If a protocol or buffer error is encountered, it is bubbled up
func CopyServerResponse(remoteBuffer *bufio.Reader, localBuffer *bufio.Writer) (err error) {
	startTime := time.Now()
	firstLine, _, err := remoteBuffer.ReadLine()
	if err != nil {
		return
	}

	//validate format
	if len(firstLine) < 2 {
		err = ERROR_BAD_BULK_FORMAT
		Debug("CopyServerResponse: Invalid multibulk response on first line")
		return
	}

	//If we have a $, write it and the rest on out
	if firstLine[0] == '$' {
		err = copyBulkMessage(firstLine, localBuffer, remoteBuffer)
		if err != nil {
			return
		}
	} else if firstLine[0] == '*' && firstLine[1] != '-' {
		err = CopyMultiBulkMessage(firstLine, localBuffer, remoteBuffer)
		if err != nil {
			return
		}
	} else {
		//This should be limited to +ok, but just fwd everything on incase the format changes
		err = writeLine(firstLine, localBuffer)
		if err != nil {
			return
		}
	}

	localBuffer.Flush()
	Debug("Time to copy line: %s\r\n", time.Since(startTime))
	return
}
