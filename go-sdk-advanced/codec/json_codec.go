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

import "encoding/json"

type JSONCodec[T any] struct{}

func (c JSONCodec[T]) Encode(value T) ([]byte, error) { return json.Marshal(value) }

func (c JSONCodec[T]) Decode(data []byte) (T, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (c JSONCodec[T]) EncodedSize() int        { return -1 }
func (c JSONCodec[T]) IsOrderedKeyCodec() bool { return false }
