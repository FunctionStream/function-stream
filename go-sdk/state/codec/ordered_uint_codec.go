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
