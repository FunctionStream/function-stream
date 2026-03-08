package structures

import (
	"fmt"
	"iter"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type PriorityQueueState[T any] struct {
	store      common.Store
	keyGroup   []byte
	key        []byte
	namespace  []byte
	valueCodec codec.Codec[T]
}

func NewPriorityQueueState[T any](store common.Store, name string, itemCodec codec.Codec[T]) (*PriorityQueueState[T], error) {
	stateName, err := common.ValidateStateName(name)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue state %q store must not be nil", stateName)
	}
	if itemCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue state %q codec must not be nil", stateName)
	}
	if !itemCodec.IsOrderedKeyCodec() {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue value codec must be ordered")
	}
	return &PriorityQueueState[T]{
		store:      store,
		keyGroup:   []byte(common.StatePQGroup),
		key:        []byte(stateName),
		namespace:  []byte("items"),
		valueCodec: itemCodec,
	}, nil
}

func (q *PriorityQueueState[T]) ck(userKey []byte) api.ComplexKey {
	return api.ComplexKey{KeyGroup: q.keyGroup, Key: q.key, Namespace: q.namespace, UserKey: userKey}
}

func (q *PriorityQueueState[T]) Add(value T) error {
	userKey, err := q.valueCodec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode pq element failed: %w", err)
	}
	return q.store.Put(q.ck(userKey), []byte{})
}

func (q *PriorityQueueState[T]) Peek() (T, bool, error) {
	var zero T
	it, err := q.store.ScanComplex(q.keyGroup, q.key, q.namespace)
	if err != nil {
		return zero, false, err
	}
	defer it.Close()

	has, err := it.HasNext()
	if err != nil || !has {
		return zero, false, err
	}

	userKey, _, ok, err := it.Next()
	if err != nil || !ok {
		return zero, false, err
	}

	val, err := q.valueCodec.Decode(userKey)
	if err != nil {
		return zero, false, err
	}
	return val, true, nil
}

func (q *PriorityQueueState[T]) Poll() (T, bool, error) {
	val, found, err := q.Peek()
	if err != nil || !found {
		return val, found, err
	}

	userKey, _ := q.valueCodec.Encode(val)
	if err = q.store.Delete(q.ck(userKey)); err != nil {
		return val, true, err
	}
	return val, true, nil
}

func (q *PriorityQueueState[T]) Clear() error {
	return q.store.DeletePrefix(api.ComplexKey{
		KeyGroup:  q.keyGroup,
		Key:       q.key,
		Namespace: q.namespace,
		UserKey:   nil,
	})
}

func (q *PriorityQueueState[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		it, err := q.store.ScanComplex(q.keyGroup, q.key, q.namespace)
		if err != nil {
			return
		}
		defer it.Close()

		for {
			has, err := it.HasNext()
			if err != nil || !has {
				return
			}
			userKey, _, ok, err := it.Next()
			if err != nil || !ok {
				return
			}

			v, _ := q.valueCodec.Decode(userKey)
			if !yield(v) {
				return
			}
		}
	}
}
