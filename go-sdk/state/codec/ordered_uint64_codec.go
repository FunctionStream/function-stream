package codec

import (
	"encoding/binary"
	"fmt"
)

type OrderedUint64Codec struct{}

func (c OrderedUint64Codec) Encode(value uint64) ([]byte, error) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, value)
	return out, nil
}

func (c OrderedUint64Codec) Decode(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid ordered uint64 payload length: %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func (c OrderedUint64Codec) EncodedSize() int { return 8 }

func (c OrderedUint64Codec) IsOrderedKeyCodec() bool { return true }
