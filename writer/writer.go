package writer

import (
	"io"
	"bytes"
)

const (
	defaultFlexibleWriterSize = 64
)

type FlexibleWriter struct {
	*bytes.Buffer
	writer io.Writer
}

func NewFlexibleWriter(writer io.Writer) *FlexibleWriter {
	w := &FlexibleWriter{}
	w.writer = writer
	buf := make([]byte, 0, defaultFlexibleWriterSize)
	w.Buffer = bytes.NewBuffer(buf)
	return w
}

func (this *FlexibleWriter) Flush() (err error) {
	_, err = this.Buffer.WriteTo(this.writer)

	return
}

func (this *FlexibleWriter) Buffered() int {
	return this.Len()
}
