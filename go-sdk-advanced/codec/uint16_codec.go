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
	"encoding/binary"
	"fmt"
)

type Uint16Codec struct{}

func (c Uint16Codec) Encode(value uint16) ([]byte, error) {
	out := make([]byte, 2)
	binary.BigEndian.PutUint16(out, value)
	return out, nil
}

func (c Uint16Codec) Decode(data []byte) (uint16, error) {
	if len(data) != 2 {
		return 0, fmt.Errorf("invalid uint16 payload length: %d", len(data))
	}
	return binary.BigEndian.Uint16(data), nil
}

func (c Uint16Codec) EncodedSize() int { return 2 }

func (c Uint16Codec) IsOrderedKeyCodec() bool { return true }
