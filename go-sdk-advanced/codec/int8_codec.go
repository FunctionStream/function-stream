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

import (
	"fmt"
)

type Int8Codec struct{}

func (c Int8Codec) Encode(value int8) ([]byte, error) {
	return []byte{byte(uint8(value) ^ (1 << 7))}, nil
}

func (c Int8Codec) Decode(data []byte) (int8, error) {
	if len(data) != 1 {
		return 0, fmt.Errorf("invalid int8 payload length: %d", len(data))
	}
	return int8(data[0] ^ (1 << 7)), nil
}

func (c Int8Codec) EncodedSize() int { return 1 }

func (c Int8Codec) IsOrderedKeyCodec() bool { return true }
