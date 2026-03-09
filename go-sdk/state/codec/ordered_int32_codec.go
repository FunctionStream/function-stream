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

type OrderedInt32Codec struct{}

func (c OrderedInt32Codec) Encode(value int32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(value)^(uint32(1)<<31))
	return out, nil
}

func (c OrderedInt32Codec) Decode(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid ordered int32 payload length: %d", len(data))
	}
	return int32(binary.BigEndian.Uint32(data) ^ (uint32(1) << 31)), nil
}

func (c OrderedInt32Codec) EncodedSize() int { return 4 }

func (c OrderedInt32Codec) IsOrderedKeyCodec() bool { return true }
