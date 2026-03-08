package keyed

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)
type KeyedValueStateFactory[V any] struct {
	inner      *keyedStateFactory
	groupKey   []byte
	valueCodec codec.Codec[V]
}

func NewKeyedValueStateFactory[V any](
	store common.Store,
	keyGroup []byte,
	valueCodec codec.Codec[V],
) (*KeyedValueStateFactory[V], error) {

	inner, err := newKeyedStateFactory(store, "", "value")
	if err != nil {
		return nil, err
	}

	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "value codec must not be nil")
	}

	return &KeyedValueStateFactory[V]{
		inner:      inner,
		groupKey:   common.DupBytes(keyGroup),
		valueCodec: valueCodec,
	}, nil
}

type KeyedValueState[V any] struct {
	factory    *KeyedValueStateFactory[V]
	primaryKey []byte
	namespace  []byte
}

func (f *KeyedValueStateFactory[V]) NewKeyedValue(primaryKey []byte, stateName string) (*KeyedValueState[V], error) {
	if primaryKey == nil || stateName == "" {
		return nil, api.NewError(api.ErrStoreInternal, "primary key and state name are required")
	}
	return &KeyedValueState[V]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  []byte(stateName),
	}, nil
}

func (s *KeyedValueState[V]) buildCK() api.ComplexKey {
	return api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   []byte{}, 
	}
}

func (s *KeyedValueState[V]) Update(value V) error {
	ck := s.buildCK()
	encoded, err := s.factory.valueCodec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value state failed: %w", err)
	}
	return s.factory.inner.store.Put(ck, encoded)
}

func (s *KeyedValueState[V]) Value() (V, bool, error) {
	var zero V
	ck := s.buildCK()
	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := s.factory.valueCodec.Decode(raw)
	if err != nil {
		return zero, false, fmt.Errorf("decode value state failed: %w", err)
	}
	return decoded, true, nil
}

func (s *KeyedValueState[V]) Clear() error {
	return s.factory.inner.store.Delete(s.buildCK())
}