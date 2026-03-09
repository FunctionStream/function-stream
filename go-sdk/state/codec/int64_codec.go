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

type Int64Codec struct{}

func (c Int64Codec) Encode(value int64) ([]byte, error) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(value))
	return out, nil
}

func (c Int64Codec) Decode(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid int64 payload length: %d", len(data))
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func (c Int64Codec) EncodedSize() int        { return 8 }
func (c Int64Codec) IsOrderedKeyCodec() bool { return false }
