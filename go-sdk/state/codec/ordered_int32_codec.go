package codec

import (
	"encoding/binary"
	"fmt"
)

type OrderedInt32Codec struct{}

func (c OrderedInt32Codec) Encode(value int32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(value)^(uint32(1)<<31))
	return out, nil
}

func (c OrderedInt32Codec) Decode(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid ordered int32 payload length: %d", len(data))
	}
	return int32(binary.BigEndian.Uint32(data) ^ (uint32(1) << 31)), nil
}

func (c OrderedInt32Codec) EncodedSize() int { return 4 }

func (c OrderedInt32Codec) IsOrderedKeyCodec() bool { return true }
