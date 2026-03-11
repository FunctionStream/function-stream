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

// IntCodec implements Codec[int] by delegating to Int64Codec.
// It is used for reflect.Int (platform-sized int) so that DefaultCodecFor[int]()
// returns a valid Codec[int] instead of panicking on type assertion.
type IntCodec struct{}

var _ Codec[int] = IntCodec{}

func (c IntCodec) Encode(value int) ([]byte, error) {
	return Int64Codec{}.Encode(int64(value))
}

func (c IntCodec) Decode(data []byte) (int, error) {
	v, err := Int64Codec{}.Decode(data)
	return int(v), err
}

func (c IntCodec) EncodedSize() int { return 8 }

func (c IntCodec) IsOrderedKeyCodec() bool { return true }
