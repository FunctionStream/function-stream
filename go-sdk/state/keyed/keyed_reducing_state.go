package keyed

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type ReduceFunc[V any] func(value1 V, value2 V) (V, error)

type KeyedReducingStateFactory[V any] struct {
	inner      *keyedStateFactory
	groupKey   []byte
	valueCodec codec.Codec[V]
	reduceFunc ReduceFunc[V]
}

func NewKeyedReducingStateFactory[V any](
	store common.Store,
	keyGroup []byte,
	valueCodec codec.Codec[V],
	reduceFunc ReduceFunc[V],
) (*KeyedReducingStateFactory[V], error) {

	inner, err := newKeyedStateFactory(store, "", "reducing")
	if err != nil {
		return nil, err
	}

	if valueCodec == nil || reduceFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "value codec and reduce function are required")
	}

	return &KeyedReducingStateFactory[V]{
		inner:      inner,
		groupKey:   common.DupBytes(keyGroup),
		valueCodec: valueCodec,
		reduceFunc: reduceFunc,
	}, nil
}

func (f *KeyedReducingStateFactory[V]) NewReducingState(primaryKey []byte, namespace []byte) (*KeyedReducingState[V], error) {
	if primaryKey == nil || namespace == nil {
		return nil, api.NewError(api.ErrStoreInternal, "primary key and state name are required")
	}
	return &KeyedReducingState[V]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  common.DupBytes(namespace),
	}, nil
}

type KeyedReducingState[V any] struct {
	factory    *KeyedReducingStateFactory[V]
	primaryKey []byte
	namespace  []byte
}

func (s *KeyedReducingState[V]) buildCK() api.ComplexKey {
	return api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   []byte{},
	}
}

func (s *KeyedReducingState[V]) Add(value V) error {
	ck := s.buildCK()
	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil {
		return fmt.Errorf("failed to get old value for reducing state: %w", err)
	}

	var result V
	if !found {
		result = value
	} else {
		oldValue, err := s.factory.valueCodec.Decode(raw)
		if err != nil {
			return fmt.Errorf("failed to decode old value: %w", err)
		}

		result, err = s.factory.reduceFunc(oldValue, value)
		if err != nil {
			return fmt.Errorf("error in user reduce function: %w", err)
		}
	}

	encoded, err := s.factory.valueCodec.Encode(result)
	if err != nil {
		return fmt.Errorf("failed to encode reduced value: %w", err)
	}

	return s.factory.inner.store.Put(ck, encoded)
}

func (s *KeyedReducingState[V]) Get() (V, bool, error) {
	var zero V
	ck := s.buildCK()
	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}
	val, err := s.factory.valueCodec.Decode(raw)
	if err != nil {
		return zero, false, fmt.Errorf("failed to decode value: %w", err)
	}
	return val, true, nil
}

func (s *KeyedReducingState[V]) Clear() error {
	return s.factory.inner.store.Delete(s.buildCK())
}
