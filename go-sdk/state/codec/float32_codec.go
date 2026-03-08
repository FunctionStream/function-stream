package codec

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Float32Codec struct{}

func (c Float32Codec) Encode(value float32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, math.Float32bits(value))
	return out, nil
}

func (c Float32Codec) Decode(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid float32 payload length: %d", len(data))
	}
	return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
}

func (c Float32Codec) EncodedSize() int        { return 4 }
func (c Float32Codec) IsOrderedKeyCodec() bool { return false }
