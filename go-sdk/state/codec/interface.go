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

// Codec is the core encoding interface. EncodedSize reports encoded byte length:
// >0 for fixed-size, <=0 for variable-size.
// IsOrderedKeyCodec reports whether the encoding is byte-orderable (for use as map/keyed state key).
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
	EncodedSize() int
	IsOrderedKeyCodec() bool
}

func FixedEncodedSize[T any](c Codec[T]) (int, bool) {
	n := c.EncodedSize()
	return n, n > 0
}
