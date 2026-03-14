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

// UintCodec implements Codec[uint] by delegating to Uint64Codec.
// It is used for reflect.Uint (platform-sized uint) so that DefaultCodecFor[uint]()
// returns a valid Codec[uint] instead of panicking on type assertion.
type UintCodec struct{}

var _ Codec[uint] = UintCodec{}

func (c UintCodec) Encode(value uint) ([]byte, error) {
	return Uint64Codec{}.Encode(uint64(value))
}

func (c UintCodec) Decode(data []byte) (uint, error) {
	v, err := Uint64Codec{}.Decode(data)
	return uint(v), err
}

func (c UintCodec) EncodedSize() int { return 8 }

func (c UintCodec) IsOrderedKeyCodec() bool { return true }
