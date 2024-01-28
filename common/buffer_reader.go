package common

import "io"

type BufferReader struct {
	buffer []byte
}

func (r *BufferReader) Read(p []byte) (n int, err error) {
	if len(r.buffer) == 0 {
		return 0, io.EOF
	}

	copied := copy(p[n:], r.buffer)
	n += copied
	r.buffer = r.buffer[copied:]

	return n, nil
}

func (r *BufferReader) ResetBuffer(data []byte) {
	r.buffer = data
}

func NewChanReader() *BufferReader {
	return &BufferReader{
		buffer: nil,
	}
}
