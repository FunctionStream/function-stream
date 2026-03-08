package codec

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Float64Codec struct{}

func (c Float64Codec) Encode(value float64) ([]byte, error) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, math.Float64bits(value))
	return out, nil
}

func (c Float64Codec) Decode(data []byte) (float64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid float64 payload length: %d", len(data))
	}
	return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
}

func (c Float64Codec) EncodedSize() int        { return 8 }
func (c Float64Codec) IsOrderedKeyCodec() bool { return false }
