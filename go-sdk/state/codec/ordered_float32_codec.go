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
	"math"
)

type OrderedFloat32Codec struct{}

func (c OrderedFloat32Codec) Encode(value float32) ([]byte, error) {
	bits := math.Float32bits(value)
	if (bits & (uint32(1) << 31)) != 0 {
		bits = ^bits
	} else {
		bits ^= uint32(1) << 31
	}
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, bits)
	return out, nil
}

func (c OrderedFloat32Codec) Decode(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid ordered float32 payload length: %d", len(data))
	}
	encoded := binary.BigEndian.Uint32(data)
	if (encoded & (uint32(1) << 31)) != 0 {
		encoded ^= uint32(1) << 31
	} else {
		encoded = ^encoded
	}
	return math.Float32frombits(encoded), nil
}

func (c OrderedFloat32Codec) EncodedSize() int { return 4 }

func (c OrderedFloat32Codec) IsOrderedKeyCodec() bool { return true }
