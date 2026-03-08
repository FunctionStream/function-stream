package structures

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type ValueState[T any] struct {
	store      common.Store
	complexKey api.ComplexKey
	codec      codec.Codec[T]
}

func NewValueState[T any](store common.Store, valueCodec codec.Codec[T]) (*ValueState[T], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "value state store must not be nil")
	}
	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "value state codec must not be nil")
	}
	ck := api.ComplexKey{
		KeyGroup:  []byte{},
		Key:       []byte{},
		Namespace: []byte{},
		UserKey:   []byte{},
	}
	return &ValueState[T]{store: store, complexKey: ck, codec: valueCodec}, nil
}

func (v *ValueState[T]) buildCK() api.ComplexKey {
	return v.complexKey
}

func (v *ValueState[T]) Update(value T) error {
	encoded, err := v.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value state failed: %w", err)
	}
	return v.store.Put(v.buildCK(), encoded)
}

func (v *ValueState[T]) Value() (T, bool, error) {
	var zero T
	ck := v.buildCK()
	raw, found, err := v.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := v.codec.Decode(raw)
	if err != nil {
		return zero, false, fmt.Errorf("decode value state failed: %w", err)
	}
	return decoded, true, nil
}

func (v *ValueState[T]) Clear() error {
	return v.store.Delete(v.buildCK())
}
