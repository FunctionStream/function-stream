package codec

import (
	"encoding/binary"
	"fmt"
)

type Int32Codec struct{}

func (c Int32Codec) Encode(value int32) ([]byte, error) {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(value))
	return out, nil
}

func (c Int32Codec) Decode(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid int32 payload length: %d", len(data))
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

func (c Int32Codec) EncodedSize() int        { return 4 }
func (c Int32Codec) IsOrderedKeyCodec() bool { return false }
