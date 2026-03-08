package codec

// Codec is the core encoding interface. EncodedSize reports encoded byte length:
// >0 for fixed-size, <=0 for variable-size.
// IsOrderedKeyCodec reports whether the encoding is byte-orderable (for use as map/keyed state key).
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
	EncodedSize() int
	IsOrderedKeyCodec() bool
}

func FixedEncodedSize[T any](c Codec[T]) (int, bool) {
	n := c.EncodedSize()
	return n, n > 0
}
