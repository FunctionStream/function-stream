package common

import (
	"fmt"
)

type ChanWriter struct {
	// The caller should make sure that the ch shouldn't be closed before calling the Write.
	ch chan []byte
}

func (w *ChanWriter) Write(p []byte) (n int, err error) {
	data := make([]byte, len(p))
	copy(data, p)
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = fmt.Errorf("write failed: %v", r)
		}
	}()
	w.ch <- data
	return len(p), nil
}

func NewChanWriter(ch chan []byte) *ChanWriter {
	return &ChanWriter{
		ch: ch,
	}
}
