package codec

import (
	"encoding/binary"
	"fmt"
)

type OrderedUint32Codec struct{}

func (c OrderedUint32Codec) Encode(value uint32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, value)
	return out, nil
}

func (c OrderedUint32Codec) Decode(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid ordered uint32 payload length: %d", len(data))
	}
	return binary.BigEndian.Uint32(data), nil
}

func (c OrderedUint32Codec) EncodedSize() int { return 4 }

func (c OrderedUint32Codec) IsOrderedKeyCodec() bool { return true }
