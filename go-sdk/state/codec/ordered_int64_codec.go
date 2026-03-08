package codec

import "github.com/functionstream/function-stream/go-sdk/state/common"

type OrderedInt64Codec struct{}

func (c OrderedInt64Codec) Encode(value int64) ([]byte, error) {
	return common.EncodeInt64Lex(value), nil
}

func (c OrderedInt64Codec) Decode(data []byte) (int64, error) { return common.DecodeInt64Lex(data) }

func (c OrderedInt64Codec) EncodedSize() int { return 8 }

func (c OrderedInt64Codec) IsOrderedKeyCodec() bool { return true }
