/*
 * Copyright (c) 2015, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package protocol

import (
	"bufio"
	"io"
	"rmux/writer"
)

const (
	//This is set to match bufio's default buffer size, so taht we can safely read&ignore large chunks of data when necessary
	BUFFER_SIZE = 4096
)

var (
	//Used when we are trying to parse the size of a bulk or multibulk message, and do not receive a valid number
	ERROR_INVALID_INT            = &RecoverableError{"Did not receive valid int value"}
	ERROR_INVALID_COMMAND_FORMAT = &RecoverableError{"Bad command format provided"}
	//Used when we expect a redis bulk-format payload, and do not receive one
	ERROR_BAD_BULK_FORMAT = &RecoverableError{"Bad bulk format supplied"}
	ERROR_COMMAND_PARSE   = &RecoverableError{"Command parse error"}

	//Error for unsupported (deemed unsafe for multiplexing) commands
	ERR_COMMAND_UNSUPPORTED = &RecoverableError{"This command is not supported"}

	//Error for when we receive bad arguments (for multiplexing) accompanying a command
	ERR_BAD_ARGUMENTS = &RecoverableError{"Bad arguments for command"}

	//Commands declared once for convenience
	DEL_COMMAND         = []byte("del")
	SUBSCRIBE_COMMAND   = []byte("subscribe")
	UNSUBSCRIBE_COMMAND = []byte("unsubscribe")
	PING_COMMAND        = []byte("ping")
	INFO_COMMAND        = []byte("info")
	SHORT_PING_COMMAND  = []byte("PING")
	SELECT_COMMAND      = []byte("select")
	QUIT_COMMAND        = []byte("quit")
	WATCH_COMMAND       = []byte("watch")
	UNWATCH_COMMAND     = []byte("unwatch")
	MULTI_COMMAND       = []byte("multi")
	EXEC_COMMAND        = []byte("exec")
	DISCARD_COMMAND     = []byte("discard")

	//Responses declared once for convenience
	OK_RESPONSE   = []byte("+OK")
	PONG_RESPONSE = []byte("+PONG")
	ERR_RESPONSE  = []byte("$-1")

	//Redis expects \r\n newlines.  Using this means we can stop remembering that
	REDIS_NEWLINE = []byte("\r\n")

	//These functions should not be executed through a proxy.
	//If you know what you're doing, you are welcome to execute them directly on your server
	UNSAFE_FUNCTIONS = map[string]bool{
		"auth":         true,
		"bgrewriteaof": true,
		"bgsave":       true,
		"client":       true,
		"config":       true,
		"dbsize":       true,
		"debug":        true,
		"lastsave":     true,
		"move":         true,
		"monitor":      true,
		"migrate":      true,
		"object":       true,
		"punsubscribe": true,
		"psubscribe":   true,
		"pubsub":       true,
		"randomkey":    true,
		"save":         true,
		"shutdown":     true,
		"slaveof":      true,
		"slowlog":      true,
		"subscribe":    true,
		"sync":         true,
		"time":         true,
		"unsubscribe":  true,
	}

	//These functions will only work if multiplexing is disabled.
	//It would be rather worthless to watch on one server, multi on another, and increment on a third
	SINGLE_DB_FUNCTIONS = map[string]bool{
		"bitop":       true,
		"brpoplpush":  true,
		"discard":     true,
		"eval":        true,
		"exec":        true,
		"keys":        true,
		"flushall":    true,
		"flushdb":     true,
		"mget":        true,
		"mset":        true,
		"msetnx":      true,
		"multi":       true,
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
		"unwatch":     true,
		"watch":       true,
		"sunionstore": true,
		"zinterstore": true,
		"zunionstore": true,
	}
)

func IsSupportedFunction(command []byte, isMultiplexing, isMultipleArgument bool) bool {
	commandLength := len(command)

	if command[0] == 'd' {
		//*del is only supported if we're not multiplexing, or there's only one argument
		if command[2] == 'l' && isMultipleArgument {
			return false
		}
		//supported if multiplexing is disabled: discard
		if command[1] == 'i' {
			return !isMultiplexing
		}
		//supported: decr, decrby, del, dump
		//unsupported: debug, dbsize
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
		if command[1] == 'h' || command[1] == 'l' || command[1] == 'y' {
			return false
		}
		// not supported: subscribe
		if command[1] == 'u' && command[2] == 'b' {
			return false
		}
		//supported if multiplexing is disabled: script, sdiff, sdiffstore, sinter, sinterstore, smove, sunion, sunionstore
		if !isMultiplexing {
			return true
		}
		if command[1] == 'c' {
			if command[3] == 'n' {
				// supported if not multiplexing: scan
				return !isMultiplexing
			}
			// supported: scard
			return true
		} else if command[1] == 'i' && command[2] == 's' {
			// supported: sismember
			return true
		} else if command[1] == 'm' && command[2] == 'e' {
			// supported: smembers
			return true
		} else if command[1] == 's' {
			// supported: sscan
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
		// supported: zadd, zcard, zcount, zincrby, zlexcount, zrange, zrangebylex,
		//            zrevrangebylex, zrangebyscore, zrank, zrem, zremrangebylex, zremrangebyrank,
		//            zremrangebyscore, zrevrank, zscore
		if !isMultiplexing {
			return true
		}

		//supported if multiplexing is disabled: zinterstore, zunionstore
		return !(command[1] == 'i' && command[3] == 't' || command[1] == 'u')
	} else if command[0] == 'p' {
		//supported: ping, psetex, pttl
		if command[1] == 'u' {
			if command[2] == 'n' || (command[2] == 'b' && command[3] == 's') {
				// unsupported: punsubscribe, pubsub
				return false
			} else if command[2] == 'b' && command[3] == 'l' {
				// supported: publish
				return true
			} else {
				return false
			}
		} else if command[1] == 's' && command[2] == 'u' {
			// unsupported: psubscribe
			return false
		} else if command[1] == 'f' {
			// pf* class of functions (hyperloglog)
			if command[2] == 'm' {
				// supported if not multiplexing: pfmerge
				return !isMultiplexing
			} else if command[2] == 'c' {
				// supported with one key if multiplexing: pfcount
				if isMultiplexing {
					return !isMultipleArgument
				}
				return true
			}
			// supported: pfadd
			return true
		} else if command[1] == 'e' {
			// supported: persist, pexpire, pexpireat
			return true
		}
		return true
	} else if command[0] == 'q' {
		//supported: quit
		return true
	} else if command[0] == 'r' {
		if command[1] == 'p' && commandLength < 8 {
			//supported: rpop, rpush, rpushx
			return true
		} else if !isMultiplexing {
			// supported if multiplexing is disabled: rename, renamenx, rpoplpush, randomkey
			// not supported: role
			return command[1] != 'o'
		} else if command[1] == 'e' && command[2] == 's' {
			// supported: restore
			return true
		}
		return false
	} else if command[0] == 't' {
		//supported: time, ttl, type
		return true
	} else if command[0] == 'u' {
		//supported if multiplexing is disabled: unwatch
		//unsupported: unsubscribe
		return command[2] == 'w' && !isMultiplexing
	} else if command[0] == 'w' {
		//supported if not multiplexing: watch
		return !isMultiplexing
	} else if command[0] == 'a' {
		//supported: append
		//unsupported: auth
		return command[1] == 'p'
	} else if command[0] == 'b' {
		if command[1] == 'i' && (commandLength == 8 || commandLength == 6) {
			//supported: bitcount, bitpos
			return true
		} else if command[1] == 'g' {
			//unsupported: bgsave, bgwriteaof
			return false
		}
		//supported if not multiplexing: bitop, brpop, blpop, brpoplpush
		return !isMultiplexing
	} else if command[0] == 'c' {
		//unsupported: client, config
		return false
	} else if command[0] == 'e' {
		//supported if multiplexing is disabled: exec
		if command[2] == 'e' {
			return !isMultiplexing
		}
		//supported: echo, exists, expire, expireat
		//supported if not multiplexing: eval, evalsha
		return command[1] != 'v' || !isMultiplexing
	} else if command[0] == 'f' {
		//Support flushall and flushdb in non-multiplexing mode
		return !isMultiplexing
	} else if command[0] == 'k' {
		//supported if not multiplexing: keys
		return !isMultiplexing
	} else if command[0] == 'm' {
		//supported if not multiplexing: mget, mset, msetnx, multi
		//unsupported: move, monitor, migrate
		if isMultiplexing {
			return false
		}
		return command[1] == 'g' || command[1] == 's' || command[1] == 'u'
	} else if command[0] == 'o' {
		return false
	}
	return false
}

// Parses a string into an int.
// Differs from atoi in that this only parses positive dec ints--hex, octal, and negatives are not allowed
// Upon invalid character received, a PANIC_INVALID_INT is caught and err'd
func ParseInt(response []byte) (value int, err error) {
	if len(response) == 0 {
		//		Debug("ParseInt: Zero-length int")
		err = ERROR_INVALID_INT
		return
	}

	value = 0
	isNegative := false
	//It's worth re-inventing the wheel, if you have a good understanding of your particular wheel's usage
	for i, b := range response {
		if i == 0 && b == '-' {
			isNegative = true
			continue
		}

		//Subtract 48 from our byte.  bytes are uint8s, so if the value is below 48, it will wrap-around back to 255 and dec. from there
		b = b - '0'
		//Since we know we have a positive value, we can now do this single check
		if b > 9 {
			//			Debug("ParseInt: Invalid int character: %q when parsing %q", b+'0', response)
			err = ERROR_INVALID_INT
			return
		}
		value *= 10
		value += int(b)
	}

	if isNegative {
		value *= -1
	}

	return
}

func ParseCommand(b []byte) (command Command, err error) {
	if len(b) < 0 {
		return nil, ERROR_COMMAND_PARSE
	}

	bc := make([]byte, len(b))
	copy(bc, b)

	switch peek := bc[0]; peek {
	case '+':
		command, err = ParseSimpleCommand(bc)
	case '$':
		command, err = ParseStringCommand(bc)
	case '*':
		command, err = ParseMultibulkCommand(bc)
	default:
		if (peek >= 'a' && peek <= 'z') || (peek >= 'A' && peek <= 'Z') {
			command, err = ParseInlineCommand(bc)
		} else {
			command, err = nil, ERROR_INVALID_COMMAND_FORMAT
		}
	}

	return
}

// Writes the given error to the buffer, preceded by a '-' and followed by a GO_NEWLINE
// Bubbles any errors from underlying writer
func WriteError(line []byte, dest *writer.FlexibleWriter, flush bool) (err error) {
	_, err = dest.Write([]byte("-ERR "))
	if err != nil {
		//		Debug("WriteError: Error received from write: %s", err)
		return err
	}

	err = WriteLine(line, dest, flush)
	if err != nil {
		//		Debug("WriteError: Error received from write: %s", err)
		return err
	}

	if flush {
		err = dest.Flush()
	}

	return
}

// Writes the given line to the buffer, followed by a GO_NEWLINE
// Does not explicitly flush the buffer.  Final lines in a sequence should be followed by FlushLine
func WriteLine(line []byte, destination *writer.FlexibleWriter, flush bool) (err error) {
	// startTime := time.Now()
	_, err = destination.Write(line)
	if err != nil {
		//		Debug("writeLine: Error received from write: %s", err)
		return
	}

	_, err = destination.Write(REDIS_NEWLINE)
	if err != nil {
		//		Debug("writeLine: Error received from writing GO_NEWLINE: %s", err)
		return
	}

	if flush {
		err = destination.Flush()
	}

	return
}

// Copies a server response from the remoteBuffer into your localBuffer
// If a protocol or buffer error is encountered, it is bubbled up
func CopyServerResponses(reader *bufio.Reader, localBuffer *writer.FlexibleWriter, numResponses int) (err error) {
	//start := time.Now()
	//defer func() {
	//	graphite.Timing("copy_server_responses", time.Now().Sub(start))
	//}()

	scanner := NewRespScanner(reader)

	numRead := 0

	for numRead < numResponses && scanner.Scan() {
		localBuffer.Write(scanner.Bytes())
		localBuffer.Flush()
		numRead++
	}

	if numRead < numResponses {
		return io.EOF
	}

	if sErr := scanner.Err(); sErr != nil {
		return sErr
	}

	if err != nil {
		return err
	}

	return nil
}
