package common

import "io"

type ChanReader struct {
	ch     chan []byte
	buffer []byte
}

func (r *ChanReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		if len(r.buffer) == 0 {
			if val, ok := <-r.ch; ok {
				r.buffer = val
			} else {
				if n == 0 {
					return 0, io.EOF
				}
				return n, nil
			}
		}

		copied := copy(p[n:], r.buffer)
		n += copied
		r.buffer = r.buffer[copied:]
	}

	return n, nil
}

func NewChanReader(ch chan []byte) *ChanReader {
	return &ChanReader{
		ch:     ch,
		buffer: nil,
	}
}
