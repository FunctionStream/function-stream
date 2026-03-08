package codec

import (
	"encoding/binary"
	"fmt"
	"math"
)

type OrderedFloat32Codec struct{}

func (c OrderedFloat32Codec) Encode(value float32) ([]byte, error) {
	bits := math.Float32bits(value)
	if (bits & (uint32(1) << 31)) != 0 {
		bits = ^bits
	} else {
		bits ^= uint32(1) << 31
	}
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, bits)
	return out, nil
}

func (c OrderedFloat32Codec) Decode(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid ordered float32 payload length: %d", len(data))
	}
	encoded := binary.BigEndian.Uint32(data)
	if (encoded & (uint32(1) << 31)) != 0 {
		encoded ^= uint32(1) << 31
	} else {
		encoded = ^encoded
	}
	return math.Float32frombits(encoded), nil
}

func (c OrderedFloat32Codec) EncodedSize() int { return 4 }

func (c OrderedFloat32Codec) IsOrderedKeyCodec() bool { return true }
