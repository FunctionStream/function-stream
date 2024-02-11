/*
 * Copyright 2024 Function Stream Org.
 *
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
	"testing"
)

func TestChanWriter_Write_HappyPath(t *testing.T) {
	writer := NewChanWriter()
	n, err := writer.Write([]byte("Hello, world!"))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 13 {
		t.Errorf("Expected to write 13 bytes, but wrote %d", n)
	}

	data := writer.GetAndReset()
	if string(data) != "Hello, world!" {
		t.Errorf("Expected to write 'Hello, world!', but wrote '%s'", data)
	}
}

func TestChanWriter_Write_EmptyData(t *testing.T) {
	writer := NewChanWriter()
	n, err := writer.Write([]byte(""))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 0 {
		t.Errorf("Expected to write 0 bytes, but wrote %d", n)
	}

	data := writer.GetAndReset()
	if string(data) != "" {
		t.Errorf("Expected to write '', but wrote '%s'", data)
	}
}
