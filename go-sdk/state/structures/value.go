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

type ValueState[T any] struct {
	store      common.Store
	complexKey api.ComplexKey
	codec      codec.Codec[T]
}

// NewValueStateFromContext creates a ValueState using the store from ctx.GetOrCreateStore(storeName).
func NewValueStateFromContext[T any](ctx api.Context, storeName string, valueCodec codec.Codec[T]) (*ValueState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newValueState(store, valueCodec)
}

// NewValueStateFromContextAutoCodec creates a ValueState with default codec for T from ctx.GetOrCreateStore(storeName).
func NewValueStateFromContextAutoCodec[T any](ctx api.Context, storeName string) (*ValueState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	valueCodec, err := codec.DefaultCodecFor[T]()
	if err != nil {
		return nil, err
	}
	return newValueState(store, valueCodec)
}

func newValueState[T any](store common.Store, valueCodec codec.Codec[T]) (*ValueState[T], error) {
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
