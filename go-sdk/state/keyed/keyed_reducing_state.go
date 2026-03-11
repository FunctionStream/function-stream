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

package keyed

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type ReduceFunc[V any] func(value1 V, value2 V) (V, error)

type KeyedReducingStateFactory[V any] struct {
	store      common.Store
	groupKey   []byte
	valueCodec codec.Codec[V]
	reduceFunc ReduceFunc[V]
}

// NewKeyedReducingStateFactoryFromContext creates a KeyedReducingStateFactory using the store from ctx.GetOrCreateStore(storeName).
func NewKeyedReducingStateFactoryFromContext[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V], reduceFunc ReduceFunc[V]) (*KeyedReducingStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newKeyedReducingStateFactory(store, keyGroup, valueCodec, reduceFunc)
}

// NewKeyedReducingStateFactoryFromContextAutoCodec creates a KeyedReducingStateFactory with default value codec from ctx.GetOrCreateStore(storeName).
func NewKeyedReducingStateFactoryFromContextAutoCodec[V any](ctx api.Context, storeName string, keyGroup []byte, reduceFunc ReduceFunc[V]) (*KeyedReducingStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	valueCodec, err := codec.DefaultCodecFor[V]()
	if err != nil {
		return nil, err
	}
	return newKeyedReducingStateFactory(store, keyGroup, valueCodec, reduceFunc)
}

func newKeyedReducingStateFactory[V any](
	store common.Store,
	keyGroup []byte,
	valueCodec codec.Codec[V],
	reduceFunc ReduceFunc[V],
) (*KeyedReducingStateFactory[V], error) {

	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed reducing state factory store must not be nil")
	}
	if keyGroup == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed reducing state factory key_group must not be nil")
	}
	if valueCodec == nil || reduceFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed reducing state factory value_codec and reduce_func must not be nil")
	}

	return &KeyedReducingStateFactory[V]{
		store:      store,
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
	raw, found, err := s.factory.store.Get(ck)
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

	return s.factory.store.Put(ck, encoded)
}

func (s *KeyedReducingState[V]) Get() (V, bool, error) {
	var zero V
	ck := s.buildCK()
	raw, found, err := s.factory.store.Get(ck)
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
	return s.factory.store.Delete(s.buildCK())
}
