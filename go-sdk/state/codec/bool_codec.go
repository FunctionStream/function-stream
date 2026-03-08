package codec

import "fmt"

type BoolCodec struct{}

func (c BoolCodec) Encode(value bool) ([]byte, error) {
	if value {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (c BoolCodec) Decode(data []byte) (bool, error) {
	if len(data) != 1 {
		return false, fmt.Errorf("invalid bool payload length: %d", len(data))
	}
	switch data[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool payload byte: %d", data[0])
	}
}

func (c BoolCodec) EncodedSize() int { return 1 }

func (c BoolCodec) IsOrderedKeyCodec() bool { return true }
