package codec

type StringCodec struct{}

func (c StringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }

func (c StringCodec) Decode(data []byte) (string, error) { return string(data), nil }

func (c StringCodec) EncodedSize() int { return -1 }

func (c StringCodec) IsOrderedKeyCodec() bool { return true }
