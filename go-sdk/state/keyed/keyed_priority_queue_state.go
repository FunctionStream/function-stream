package keyed

import (
	"fmt"
	"iter"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type KeyedPriorityQueueStateFactory[V any] struct {
	inner      *keyedStateFactory
	groupKey   []byte
	valueCodec codec.Codec[V]
}

func NewKeyedPriorityQueueStateFactory[V any](
	store common.Store,
	keyGroup []byte,
	valueCodec codec.Codec[V],
) (*KeyedPriorityQueueStateFactory[V], error) {

	inner, err := newKeyedStateFactory(store, "", "pq")
	if err != nil {
		return nil, err
	}

	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "value codec must not be nil")
	}

	if !valueCodec.IsOrderedKeyCodec() {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue value codec must be ordered")
	}

	return &KeyedPriorityQueueStateFactory[V]{
		inner:      inner,
		groupKey:   common.DupBytes(keyGroup),
		valueCodec: valueCodec,
	}, nil
}

type KeyedPriorityQueueState[V any] struct {
	factory    *KeyedPriorityQueueStateFactory[V]
	primaryKey []byte
	namespace  []byte
}

func (f *KeyedPriorityQueueStateFactory[V]) NewKeyedPriorityQueue(primaryKey []byte, namespace []byte) (*KeyedPriorityQueueState[V], error) {
	if primaryKey == nil || namespace == nil {
		return nil, api.NewError(api.ErrStoreInternal, "primary key and queue name are required")
	}
	return &KeyedPriorityQueueState[V]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  common.DupBytes(namespace),
	}, nil
}

func (s *KeyedPriorityQueueState[V]) Add(value V) error {
	userKey, err := s.factory.valueCodec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode pq element failed: %w", err)
	}

	ck := api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   userKey,
	}

	return s.factory.inner.store.Put(ck, []byte{})
}

func (s *KeyedPriorityQueueState[V]) Peek() (V, bool, error) {
	var zero V

	iter, err := s.factory.inner.store.ScanComplex(
		s.factory.groupKey,
		s.primaryKey,
		s.namespace,
	)
	if err != nil {
		return zero, false, err
	}
	defer iter.Close()

	has, err := iter.HasNext()
	if err != nil || !has {
		return zero, false, err
	}

	userKey, _, ok, err := iter.Next()
	if err != nil || !ok {
		return zero, false, err
	}

	val, err := s.factory.valueCodec.Decode(userKey)
	if err != nil {
		return zero, false, err
	}
	return val, true, nil
}

func (s *KeyedPriorityQueueState[V]) Poll() (V, bool, error) {
	val, found, err := s.Peek()
	if err != nil || !found {
		return val, found, err
	}

	userKey, _ := s.factory.valueCodec.Encode(val)
	ck := api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   userKey,
	}

	err = s.factory.inner.store.Delete(ck)
	return val, true, err
}

func (s *KeyedPriorityQueueState[V]) Clear() error {
	return s.factory.inner.store.DeletePrefix(api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   []byte{},
	})
}

func (s *KeyedPriorityQueueState[V]) All() iter.Seq[V] {
	return func(yield func(V) bool) {
		iter, err := s.factory.inner.store.ScanComplex(
			s.factory.groupKey,
			s.primaryKey,
			s.namespace,
		)
		if err != nil {
			return
		}
		defer iter.Close()

		for {
			has, err := iter.HasNext()
			if err != nil || !has {
				return
			}
			userKey, _, ok, err := iter.Next()
			if err != nil || !ok {
				return
			}

			v, _ := s.factory.valueCodec.Decode(userKey)
			if !yield(v) {
				return
			}
		}
	}
}