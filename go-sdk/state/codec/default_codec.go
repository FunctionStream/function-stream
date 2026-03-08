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
		return any(OrderedInt32Codec{}).(Codec[V]), nil
	case reflect.Int64:
		return any(OrderedInt64Codec{}).(Codec[V]), nil
	case reflect.Uint32:
		return any(OrderedUint32Codec{}).(Codec[V]), nil
	case reflect.Uint64:
		return any(OrderedUint64Codec{}).(Codec[V]), nil
	case reflect.Float32:
		return any(OrderedFloat32Codec{}).(Codec[V]), nil
	case reflect.Float64:
		return any(OrderedFloat64Codec{}).(Codec[V]), nil
	case reflect.String:
		return any(StringCodec{}).(Codec[V]), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint8, reflect.Uint16:
		return any(JSONCodec[V]{}).(Codec[V]), nil
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Interface:
		return any(JSONCodec[V]{}).(Codec[V]), nil
	default:
		return any(JSONCodec[V]{}).(Codec[V]), nil
	}
}
