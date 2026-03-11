// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package structures

import (
	"fmt"
	"iter"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

// PriorityQueueState holds a priority queue. itemCodec must be ordered (IsOrderedKeyCodec() true).
type PriorityQueueState[T any] struct {
	store      common.Store
	keyGroup   []byte
	key        []byte
	namespace  []byte
	valueCodec codec.Codec[T]
}

// NewPriorityQueueStateFromContext creates a PriorityQueueState using the store from ctx.GetOrCreateStore(storeName).
func NewPriorityQueueStateFromContext[T any](ctx api.Context, storeName string, itemCodec codec.Codec[T]) (*PriorityQueueState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newPriorityQueueState(store, itemCodec)
}

// NewPriorityQueueStateFromContextAutoCodec creates a PriorityQueueState with default codec for T. T must have an ordered default codec (e.g. primitive types).
func NewPriorityQueueStateFromContextAutoCodec[T any](ctx api.Context, storeName string) (*PriorityQueueState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	itemCodec, err := codec.DefaultCodecFor[T]()
	if err != nil {
		return nil, err
	}
	return newPriorityQueueState(store, itemCodec)
}

// newPriorityQueueState creates a priority queue state. itemCodec must support ordered key encoding.
func newPriorityQueueState[T any](store common.Store, itemCodec codec.Codec[T]) (*PriorityQueueState[T], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue state store must not be nil")
	}
	if itemCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue state codec must not be nil")
	}
	if !itemCodec.IsOrderedKeyCodec() {
		return nil, api.NewError(api.ErrStoreInternal, "priority queue codec must support ordered key encoding")
	}
	return &PriorityQueueState[T]{
		store:      store,
		keyGroup:   []byte{},
		key:        []byte{},
		namespace:  []byte{},
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
