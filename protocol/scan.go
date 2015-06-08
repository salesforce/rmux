package protocol

import (
	"bufio"
	"bytes"
//	. "github.com/forcedotcom/rmux/log"
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
		if (peek >= 'a' && peek <= 'z') || (peek >= 'A' && peek <= 'Z') {
			advance, token, err = ScanInlineString(data, atEOF)
		} else {
			advance, token, err = 0, nil, ERROR_INVALID_COMMAND_FORMAT
		}
	}

	//	if err != nil {
//	//		Debug("Scanned an error %s", err)
	//	} else {
//	//		Debug("Scanned a chunk %d %q", advance, token)
	//	}

	return
}

func scanNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	s := 0
	for {
		if iNL := bytes.IndexByte(data[s:], '\n'); iNL > 1 {
			if data[s+iNL-1] == '\r' {
				// If we match \r\n, then advance and return that
				advance = s + iNL + 1
				return advance, data[:advance], nil
			} else {
				// Didn't match a CRNL, scan past the newline
				s += iNL + 1
				continue
			}
		} else if iNL < 0 {
			if atEOF {
				// Advance to the end, don't return anything
				return len(data), nil, nil
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
	advance, token, err = scanNewline(data, atEOF)
	return
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
		return advance, nil, err
	}

	if strLen < 0 {
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

	advance, token, err = scanNewline(data, atEOF)
	return
}

// =============== Integer ==============
func ScanInteger(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if data[0] != ':' {
		return 0, nil, ERROR_COMMAND_PARSE
	}

	advance, token, err = scanNewline(data, atEOF)
	return
}

// =============== Inline String ==============
func ScanInlineString(data []byte, atEOF bool) (advance int, token []byte, err error) {
	advance, token, err = scanNewline(data, atEOF)
	return
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
		return advance, nil, err
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
		return advance, nil, err
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
