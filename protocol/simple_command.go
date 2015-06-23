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
	"fmt"
)

type SimpleCommand struct {
	Buffer  []byte
	Command []byte
}

func ParseSimpleCommand(b []byte) (*SimpleCommand, error) {
	c := &SimpleCommand{}
	c.Buffer = make([]byte, len(b))
	copy(c.Buffer, b)

	if c.Buffer[0] != '+' {
		return nil, fmt.Errorf("Expected '+', got '%c'", c.Buffer[0])
	}

	c.Command = c.Buffer[1 : len(c.Buffer)-2]
	for i := 0; i < len(c.Command); i++ {
		// lowercase it
		if char := c.Command[i]; char >= 'A' && char <= 'Z' {
			c.Command[i] = c.Command[i] + 0x20
		}
	}

	return c, nil
}

// Satisfy Command Interface
func (this *SimpleCommand) GetCommand() []byte {
	return this.Command
}

func (this *SimpleCommand) GetBuffer() []byte {
	return this.Buffer
}

func (this *SimpleCommand) GetFirstArg() []byte {
	return nil
}

func (this *SimpleCommand) GetArgCount() int {
	return 0
}
