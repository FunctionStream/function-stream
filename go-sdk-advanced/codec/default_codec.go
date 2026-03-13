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
	"reflect"
)

func DefaultCodecFor[V any]() (Codec[V], error) {
	var zero V
	t := reflect.TypeOf(zero)
	if t == nil {
		return nil, fmt.Errorf("default codec: type parameter V must not be interface without type constraint")
	}
	k := t.Kind()
	switch k {
	case reflect.Bool:
		return any(BoolCodec{}).(Codec[V]), nil
	case reflect.Int32:
		return any(Int32Codec{}).(Codec[V]), nil
	case reflect.Int64:
		return any(Int64Codec{}).(Codec[V]), nil
	case reflect.Uint32:
		return any(Uint32Codec{}).(Codec[V]), nil
	case reflect.Uint64:
		return any(Uint64Codec{}).(Codec[V]), nil
	case reflect.Float32:
		return any(Float32Codec{}).(Codec[V]), nil
	case reflect.Float64:
		return any(Float64Codec{}).(Codec[V]), nil
	case reflect.String:
		return any(StringCodec{}).(Codec[V]), nil
	case reflect.Int:
		return any(IntCodec{}).(Codec[V]), nil
	case reflect.Int8:
		return any(Int8Codec{}).(Codec[V]), nil
	case reflect.Int16:
		return any(Int16Codec{}).(Codec[V]), nil
	case reflect.Uint:
		return any(UintCodec{}).(Codec[V]), nil
	case reflect.Uint8:
		return any(Uint8Codec{}).(Codec[V]), nil
	case reflect.Uint16:
		return any(Uint16Codec{}).(Codec[V]), nil
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Interface:
		return any(JSONCodec[V]{}).(Codec[V]), nil
	default:
		return any(JSONCodec[V]{}).(Codec[V]), nil
	}
}
