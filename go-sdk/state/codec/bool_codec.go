// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import "fmt"

type BoolCodec struct{}

func (c BoolCodec) Encode(value bool) ([]byte, error) {
	if value {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (c BoolCodec) Decode(data []byte) (bool, error) {
	if len(data) != 1 {
		return false, fmt.Errorf("invalid bool payload length: %d", len(data))
	}
	switch data[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool payload byte: %d", data[0])
	}
}

func (c BoolCodec) EncodedSize() int { return 1 }

func (c BoolCodec) IsOrderedKeyCodec() bool { return true }
