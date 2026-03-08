package codec

import (
	"encoding/binary"
	"fmt"
)

type Uint64Codec struct{}

func (c Uint64Codec) Encode(value uint64) ([]byte, error) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, value)
	return out, nil
}

func (c Uint64Codec) Decode(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid uint64 payload length: %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func (c Uint64Codec) EncodedSize() int        { return 8 }
func (c Uint64Codec) IsOrderedKeyCodec() bool { return false }
