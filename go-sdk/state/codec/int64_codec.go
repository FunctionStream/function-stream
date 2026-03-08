package codec

import (
	"encoding/binary"
	"fmt"
)

type Int64Codec struct{}

func (c Int64Codec) Encode(value int64) ([]byte, error) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(value))
	return out, nil
}

func (c Int64Codec) Decode(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid int64 payload length: %d", len(data))
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func (c Int64Codec) EncodedSize() int        { return 8 }
func (c Int64Codec) IsOrderedKeyCodec() bool { return false }
