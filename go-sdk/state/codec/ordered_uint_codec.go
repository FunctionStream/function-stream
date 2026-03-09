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

import "strconv"

type OrderedUintCodec struct{}

func (c OrderedUintCodec) Encode(value uint) ([]byte, error) {
	if strconv.IntSize == 32 {
		return OrderedUint32Codec{}.Encode(uint32(value))
	}
	return OrderedUint64Codec{}.Encode(uint64(value))
}

func (c OrderedUintCodec) Decode(data []byte) (uint, error) {
	if strconv.IntSize == 32 {
		v, err := OrderedUint32Codec{}.Decode(data)
		return uint(v), err
	}
	v, err := OrderedUint64Codec{}.Decode(data)
	return uint(v), err
}

func (c OrderedUintCodec) EncodedSize() int { return strconv.IntSize / 8 }

func (c OrderedUintCodec) IsOrderedKeyCodec() bool { return true }
