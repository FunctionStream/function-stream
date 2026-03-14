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

type Uint8Codec struct{}

func (c Uint8Codec) Encode(value uint8) ([]byte, error) {
	return []byte{byte(value)}, nil
}

func (c Uint8Codec) Decode(data []byte) (uint8, error) {
	if len(data) != 1 {
		return 0, fmt.Errorf("invalid uint8 payload length: %d", len(data))
	}
	return data[0], nil
}

func (c Uint8Codec) EncodedSize() int { return 1 }

func (c Uint8Codec) IsOrderedKeyCodec() bool { return true }
