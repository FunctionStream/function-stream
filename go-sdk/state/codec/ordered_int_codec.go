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

type OrderedIntCodec struct{}

func (c OrderedIntCodec) Encode(value int) ([]byte, error) {
	if strconv.IntSize == 32 {
		return OrderedInt32Codec{}.Encode(int32(value))
	}
	return OrderedInt64Codec{}.Encode(int64(value))
}

func (c OrderedIntCodec) Decode(data []byte) (int, error) {
	if strconv.IntSize == 32 {
		v, err := OrderedInt32Codec{}.Decode(data)
		return int(v), err
	}
	v, err := OrderedInt64Codec{}.Decode(data)
	return int(v), err
}

func (c OrderedIntCodec) EncodedSize() int { return strconv.IntSize / 8 }

func (c OrderedIntCodec) IsOrderedKeyCodec() bool { return true }
