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
