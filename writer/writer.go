package writer

import (
	"io"
)

const (
	defaultFlexibleWriterSize = 4096
)

type FlexibleWriter struct {
	buffer []byte
	writer io.Writer
}

func NewFlexibleWriter(writer io.Writer) *FlexibleWriter {
	w := &FlexibleWriter{}
	w.writer = writer
	w.buffer = make([]byte, 0, defaultFlexibleWriterSize)
	return w
}

func (this *FlexibleWriter) Write(p []byte) (int, error) {
	// TODO: Profile
	this.buffer = append(this.buffer, p...)

	return len(p), nil
}

func (this *FlexibleWriter) Flush() (err error) {
	if len(this.buffer) == 0 {
		return nil
	}

	n, err := this.writer.Write(this.buffer)

	if n < len(this.buffer) {
		err = io.ErrShortWrite
	}

	if err != nil {
		if n > 0 && n < len(this.buffer) {
			numLeft := len(this.buffer) - n
			copy(this.buffer[0:numLeft], this.buffer[n:len(this.buffer)])
			this.buffer = this.buffer[0:numLeft]
		}

		return err
	}

	this.buffer = this.buffer[0:0]

	return nil
}

func (this *FlexibleWriter) Buffered() int {
	return len(this.buffer)
}
