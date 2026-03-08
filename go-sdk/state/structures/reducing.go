package structures

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type ReduceFunc[V any] func(value1 V, value2 V) (V, error)

type ReducingState[V any] struct {
	store      common.Store
	complexKey api.ComplexKey
	valueCodec codec.Codec[V]
	reduceFunc ReduceFunc[V]
}

func NewReducingState[V any](
	store common.Store,
	valueCodec codec.Codec[V],
	reduceFunc ReduceFunc[V],
) (*ReducingState[V], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "reducing state store must not be nil")
	}
	if valueCodec == nil || reduceFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "reducing state value codec and reduce function are required")
	}
	ck := api.ComplexKey{
		KeyGroup:  []byte{},
		Key:       []byte{},
		Namespace: []byte{},
		UserKey:   []byte{},
	}
	return &ReducingState[V]{
		store:      store,
		complexKey: ck,
		valueCodec: valueCodec,
		reduceFunc: reduceFunc,
	}, nil
}

func (s *ReducingState[V]) buildCK() api.ComplexKey {
	return s.complexKey
}

func (s *ReducingState[V]) Add(value V) error {
	ck := s.buildCK()
	raw, found, err := s.store.Get(ck)
	if err != nil {
		return fmt.Errorf("failed to get old value for reducing state: %w", err)
	}

	var result V
	if !found {
		result = value
	} else {
		oldValue, err := s.valueCodec.Decode(raw)
		if err != nil {
			return fmt.Errorf("failed to decode old value: %w", err)
		}

		result, err = s.reduceFunc(oldValue, value)
		if err != nil {
			return fmt.Errorf("error in user reduce function: %w", err)
		}
	}

	encoded, err := s.valueCodec.Encode(result)
	if err != nil {
		return fmt.Errorf("failed to encode reduced value: %w", err)
	}

	return s.store.Put(ck, encoded)
}

func (s *ReducingState[V]) Get() (V, bool, error) {
	var zero V
	ck := s.buildCK()
	raw, found, err := s.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}
	val, err := s.valueCodec.Decode(raw)
	if err != nil {
		return zero, false, fmt.Errorf("failed to decode value: %w", err)
	}
	return val, true, nil
}

func (s *ReducingState[V]) Clear() error {
	return s.store.Delete(s.buildCK())
}
