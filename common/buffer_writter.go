package common

type BufferWriter struct {
	buffer []byte
}

func (w *BufferWriter) Write(p []byte) (n int, err error) {
	if w.buffer == nil {
		w.buffer = make([]byte, 0)
	}
	w.buffer = append(w.buffer, p...)
	return len(p), nil
}

func (w *BufferWriter) GetAndReset() []byte {
	result := w.buffer
	w.buffer = nil
	return result
}

func NewChanWriter() *BufferWriter {
	return &BufferWriter{}
}
