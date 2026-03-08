package codec

import "github.com/functionstream/function-stream/go-sdk/state/common"

type BytesCodec struct{}

func (c BytesCodec) Encode(value []byte) ([]byte, error) { return common.DupBytes(value), nil }

func (c BytesCodec) Decode(data []byte) ([]byte, error) { return common.DupBytes(data), nil }

func (c BytesCodec) EncodedSize() int { return -1 }

func (c BytesCodec) IsOrderedKeyCodec() bool { return true }
