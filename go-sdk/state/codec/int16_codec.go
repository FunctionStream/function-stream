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

type Int16Codec struct{}

func (c Int16Codec) Encode(value int16) ([]byte, error) {
	out := make([]byte, 2)
	binary.BigEndian.PutUint16(out, uint16(value)^(uint16(1)<<15))
	return out, nil
}

func (c Int16Codec) Decode(data []byte) (int16, error) {
	if len(data) != 2 {
		return 0, fmt.Errorf("invalid int16 payload length: %d", len(data))
	}
	return int16(binary.BigEndian.Uint16(data) ^ (uint16(1) << 15)), nil
}

func (c Int16Codec) EncodedSize() int { return 2 }

func (c Int16Codec) IsOrderedKeyCodec() bool { return true }
