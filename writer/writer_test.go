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

package writer

import (
	"bytes"
	"strings"
	"testing"
)

func TestFlexibleWriter_WriteFlush(t *testing.T) {
	b := new(bytes.Buffer)
	fw := NewFlexibleWriter(b)

	toWrite := []byte("First write")

	if n, err := fw.Write(toWrite); err != nil {
		t.Errorf("fw.Write errored: %s", err)
	} else if n != len(toWrite) {
		t.Errorf("fw.Write did not write expected number of bytes. got:%d expected:%d", n, len(toWrite))
	} else if b.Len() != 0 {
		t.Error("Should have not flushed on a write.")
	}

	fw.Flush()
	if !bytes.Equal(toWrite, b.Bytes()) {
		t.Error("Should have flushed after call to Flush()")
	}
}

func TestFlexibleWriter_HugeWrite(t *testing.T) {
	b := new(bytes.Buffer)
	fw := NewFlexibleWriter(b)

	// 10 * 64KB of data
	hugeWrite := []byte(strings.Repeat("0123456789", 64*1024))

	// Sanity check
	if len(hugeWrite) != 64*1024*10 {
		t.Errorf("Sanity check failed. Byte slice size does not match. got:%d expected:%d", 64*1024*10, len(hugeWrite))
	}

	if n, err := fw.Write(hugeWrite); err != nil {
		t.Errorf("fw.Write errored: %s", err)
	} else if n != len(hugeWrite) {
		t.Errorf("fw.Write did not write expected number of bytes. got:%d expected:%d", n, len(hugeWrite))
	} else if b.Len() != 0 {
		t.Error("Should have not flushed on a write.")
	}

	fw.Flush()
	if !bytes.Equal(hugeWrite, b.Bytes()) {
		t.Error("Should have flushed after call to Flush()")
	}
}
