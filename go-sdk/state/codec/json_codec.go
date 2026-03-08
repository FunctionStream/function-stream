package codec

import "encoding/json"

type JSONCodec[T any] struct{}

func (c JSONCodec[T]) Encode(value T) ([]byte, error) { return json.Marshal(value) }

func (c JSONCodec[T]) Decode(data []byte) (T, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (c JSONCodec[T]) EncodedSize() int        { return -1 }
func (c JSONCodec[T]) IsOrderedKeyCodec() bool { return false }
