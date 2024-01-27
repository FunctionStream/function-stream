package common

import (
	"io"
	"testing"
)

func TestChanReader_Read_HappyPath(t *testing.T) {
	ch := make(chan []byte, 1)
	ch <- []byte("Hello, world!")
	close(ch)

	reader := NewChanReader(ch)
	buffer := make([]byte, 13)

	n, err := reader.Read(buffer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 13 {
		t.Errorf("Expected to read 13 bytes, but read %d", n)
	}

	if string(buffer) != "Hello, world!" {
		t.Errorf("Expected to read 'Hello, world!', but read '%s'", buffer)
	}
}

func TestChanReader_Read_EmptyChannel(t *testing.T) {
	ch := make(chan []byte)
	close(ch)

	reader := NewChanReader(ch)
	buffer := make([]byte, 10)

	n, err := reader.Read(buffer)
	if err != io.EOF {
		t.Errorf("Expected error to be io.EOF, but got %v", err)
	}

	if n != 0 {
		t.Errorf("Expected to read 0 bytes, but read %d", n)
	}
}

func TestChanReader_Read_BufferSmallerThanData(t *testing.T) {
	ch := make(chan []byte, 1)
	ch <- []byte("Hello, world!")
	close(ch)

	reader := NewChanReader(ch)
	buffer := make([]byte, 5)

	n, err := reader.Read(buffer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected to read 5 bytes, but read %d", n)
	}

	if string(buffer) != "Hello" {
		t.Errorf("Expected to read 'Hello', but read '%s'", buffer)
	}
}

func TestChanReader_Read_BufferLargerThanData(t *testing.T) {
	ch := make(chan []byte, 1)
	ch <- []byte("Hello")
	close(ch)

	reader := NewChanReader(ch)
	buffer := make([]byte, 10)

	n, err := reader.Read(buffer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected to read 5 bytes, but read %d", n)
	}

	if string(buffer[:n]) != "Hello" {
		t.Errorf("Expected to read 'Hello', but read '%s'", buffer[:n])
	}
}
