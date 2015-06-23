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
	"bytes"
)

var NIL_STRING []byte = nil

type MultibulkCommand struct {
	Buffer  []byte
	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func ParseMultibulkCommand(b []byte) (*MultibulkCommand, error) {
	c := &MultibulkCommand{}
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	if c.Buffer[0] != '*' {
		return nil, ERROR_COMMAND_PARSE
	}

	newlinePos := bytes.Index(c.Buffer, REDIS_NEWLINE)
	if newlinePos < 0 {
		return nil, ERROR_COMMAND_PARSE
	}

	count, err := ParseInt(c.Buffer[1:newlinePos])
	if err != nil {
		return nil, err
	}

	if count > 0 {
		c.ArgCount = count - 1
	}

	cBuf := c.Buffer[newlinePos+2:]
	for i := 0; i < 2 && i < count; i++ {
		if cBuf[0] != '$' {
			return nil, ERROR_COMMAND_PARSE
		}

		newlinePos := bytes.Index(cBuf, REDIS_NEWLINE)
		if newlinePos < 0 {
			return nil, ERROR_COMMAND_PARSE
		}

		count, err := ParseInt(cBuf[1:newlinePos])
		if err != nil {
			return nil, err
		} else if count < 0 {
			cBuf = cBuf[newlinePos+2:]
			continue
		}

		if i == 0 {
			c.Command = cBuf[newlinePos+2 : newlinePos+2+count]
		} else {
			c.FirstArg = cBuf[newlinePos+2 : newlinePos+2+count]
		}

		cBuf = cBuf[newlinePos+2+count+2:]
	}

	for i := 0; i < len(c.Command); i++ {
		if char := c.Command[i]; char >= 'A' && char <= 'Z' {
			c.Command[i] = c.Command[i] + 0x20
		}
	}

	return c, nil
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
