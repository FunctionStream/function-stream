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

type Float32Codec struct{}

func (c Float32Codec) Encode(value float32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, math.Float32bits(value))
	return out, nil
}

func (c Float32Codec) Decode(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid float32 payload length: %d", len(data))
	}
	return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
}

func (c Float32Codec) EncodedSize() int        { return 4 }
func (c Float32Codec) IsOrderedKeyCodec() bool { return false }
