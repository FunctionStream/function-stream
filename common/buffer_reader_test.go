/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"io"
	"testing"
)

func TestChanReader_Read_HappyPath(t *testing.T) {
	reader := NewChanReader()
	reader.ResetBuffer([]byte("Hello, world!"))
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
	reader := NewChanReader()
	reader.ResetBuffer([]byte(""))
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
	reader := NewChanReader()
	reader.ResetBuffer([]byte("Hello, world!"))
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
	reader := NewChanReader()
	reader.ResetBuffer([]byte("Hello"))
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
