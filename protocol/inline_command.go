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

type InlineCommand struct {
	Buffer  []byte
	Command []byte
	// Usually denotes the key
	FirstArg []byte
	ArgCount int
}

func NewInlineCommand() *InlineCommand {
	c := &InlineCommand{}
	c.Buffer = nil
	c.Command = nil
	c.FirstArg = nil
	return c
}

func ParseInlineCommand(b []byte) (*InlineCommand, error) {
	c := NewInlineCommand()

	// Copy the bytes, probably not going to have access to that dataspace later
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	parts := bytes.Split(c.Buffer[:len(c.Buffer)-2], []byte(" "))

	for i, part := range parts {
		if i == 0 {
			c.Command = part

			for i := 0; i < len(c.Command); i++ {
				if c.Command[i] >= 'A' && c.Command[i] <= 'Z' {
					c.Command[i] = c.Command[i] + 0x20
				}
			}

			continue
		}

		if len(part) == 0 {
			continue
		}

		if c.FirstArg == nil {
			c.FirstArg = part
		}

		c.ArgCount++
	}

	return c, nil
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
