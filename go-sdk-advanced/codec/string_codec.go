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

type StringCodec struct{}

func (c StringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }

func (c StringCodec) Decode(data []byte) (string, error) { return string(data), nil }

func (c StringCodec) EncodedSize() int { return -1 }

func (c StringCodec) IsOrderedKeyCodec() bool { return true }
