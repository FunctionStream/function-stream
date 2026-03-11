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

// NewReducingStateFromContext creates a ReducingState using the store from ctx.GetOrCreateStore(storeName).
func NewReducingStateFromContext[V any](
	ctx api.Context,
	storeName string,
	valueCodec codec.Codec[V],
	reduceFunc ReduceFunc[V],
) (*ReducingState[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newReducingState(store, valueCodec, reduceFunc)
}

// NewReducingStateFromContextAutoCodec creates a ReducingState with default value codec from ctx.GetOrCreateStore(storeName).
func NewReducingStateFromContextAutoCodec[V any](
	ctx api.Context,
	storeName string,
	reduceFunc ReduceFunc[V],
) (*ReducingState[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	valueCodec, err := codec.DefaultCodecFor[V]()
	if err != nil {
		return nil, err
	}
	return newReducingState(store, valueCodec, reduceFunc)
}

func newReducingState[V any](
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
