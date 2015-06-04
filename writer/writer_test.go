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
