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
	"bytes"
	"io"
)

// ================== Base =================
func NewRespScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(ScanResp)
	return scanner
}

func ScanResp(data []byte, atEOF bool) (advance int, token []byte, err error) {
	//	if len(data) > 0 {
//	//		Debug("Scanning %q", data)
	//	}

	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if len(data) == 0 {
		return 0, nil, nil
	}

	switch peek := data[0]; peek {
	case '+':
		advance, token, err = ScanSimpleString(data, atEOF)
	case '$':
		advance, token, err = ScanBulkString(data, atEOF)
	case ':':
		advance, token, err = ScanInteger(data, atEOF)
	case '-':
		advance, token, err = ScanError(data, atEOF)
	case '*':
		advance, token, err = ScanArray(data, atEOF)
	default:
		advance, token, err = 0, nil, ERROR_INVALID_COMMAND_FORMAT
	}

	return
}

func scanNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	dlen := len(data)

	if atEOF && dlen == 0 {
		return 0, nil, nil
	}

	s := 0
	for {
		ndxNL := bytes.IndexByte(data[s:], '\n')

		if ndxNL == 0 || ndxNL == 1 {
			// the newline is at 0 or 1! what! parse error.
			return dlen, nil, ERROR_COMMAND_PARSE
		} else if ndxNL > 1 {
			if data[s+ndxNL-1] == '\r' {
				// If we match \r\n, then advance and return that
				advance = s + ndxNL + 1
				return advance, data[:advance], nil
			} else {
				// Didn't match a CRNL, scan past the newline
				s += ndxNL + 1
				continue
			}
		} else if ndxNL < 0 {
			if atEOF {
				// Advance to the end, don't return anything
				return dlen, nil, nil
			} else {
				// No newline found, ask for more
				return 0, nil, nil
			}
		}
	}
}

// =============== Simple String ==============
func ScanSimpleString(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != '+' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	// Find the newline
	return scanNewline(data, atEOF)
}

// =============== Bulk String ==============
func ScanBulkString(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != '$' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	advance, token, err = scanNewline(data, atEOF)
	if err != nil || advance == 0 {
		return advance, token, err
	}

	if len(token) < 4 {
		return 0, nil, nil
	}

	strLenBytes := token[1 : len(token)-2]
	if len(strLenBytes) == 0 {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	strLen, err := ParseInt(strLenBytes)
	if err != nil {
		return 0, nil, err
	}

	if strLen < 0 {
		// There's a negative string length, so it's a 'null' string. Return what we read
		return advance, data[:advance], nil
	}

	if len(data[advance:]) < 2+strLen {
		// Ask for more if we can't read what we have
		return 0, nil, nil
	}

	advance = advance + strLen + 2
	return advance, data[:advance], nil
}

// =============== Errors ==============
func ScanError(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != '-' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	return scanNewline(data, atEOF)
}

// =============== Integer ==============
func ScanInteger(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != ':' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	return scanNewline(data, atEOF)
}

// =============== Inline String ==============
func ScanInlineString(data []byte, atEOF bool) (advance int, token []byte, err error) {
	return scanNewline(data, atEOF)
}

// =============== Array ==============
func ScanArray(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != '*' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	advance, token, err = scanNewline(data, atEOF)
	if err != nil {
		return 0, nil, err
	} else if advance == 0 || token == nil || len(token) < 3 {
		if len(token) < 3 && len(token) > 0 {
//			Debug("Hm. %q", token)
		}
		// Asking for more data
		return 0, nil, nil
	}

	arrayCountBytes := token[1 : len(token)-2]
	if len(arrayCountBytes) == 0 {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	arrayCount, err := ParseInt(arrayCountBytes)
	if err != nil {
		return 0, nil, err
	}

	s := advance
	rData := data[s:]
	for i := 0; i < arrayCount; i++ {
		advance, token, err = ScanResp(rData, atEOF)
		if token == nil || err != nil {
			if advance == 0 {
				return 0, token, err
			} else {
				return s + advance, token, err
			}
		}

		s += advance

		rData = data[s:]
	}

	return s, data[:s], nil
}
