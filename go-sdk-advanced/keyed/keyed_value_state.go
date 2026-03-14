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

// Package keyed provides keyed state types (KeyedValueState, KeyedListState, etc.)
// for the Advanced SDK. This library depends on go-sdk (low-level).
package keyed

import (
	"fmt"

	"github.com/functionstream/function-stream/go-sdk-advanced/codec"
	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type KeyedValueStateFactory[V any] struct {
	store      common.Store
	groupKey   []byte
	valueCodec codec.Codec[V]
}

// NewKeyedValueStateFactoryFromContext creates a KeyedValueStateFactory using the store from ctx.GetOrCreateStore(storeName).
func NewKeyedValueStateFactoryFromContext[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V]) (*KeyedValueStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newKeyedValueStateFactory(store, keyGroup, valueCodec)
}

// NewKeyedValueStateFactoryFromContextAutoCodec creates a KeyedValueStateFactory with default value codec from ctx.GetOrCreateStore(storeName).
func NewKeyedValueStateFactoryFromContextAutoCodec[V any](ctx api.Context, storeName string, keyGroup []byte) (*KeyedValueStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	valueCodec, err := codec.DefaultCodecFor[V]()
	if err != nil {
		return nil, err
	}
	return newKeyedValueStateFactory(store, keyGroup, valueCodec)
}

func newKeyedValueStateFactory[V any](
	store common.Store,
	keyGroup []byte,
	valueCodec codec.Codec[V],
) (*KeyedValueStateFactory[V], error) {

	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed value state factory store must not be nil")
	}
	if keyGroup == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed value state factory key_group must not be nil")
	}
	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed value state factory value codec must not be nil")
	}

	return &KeyedValueStateFactory[V]{
		store:      store,
		groupKey:   common.DupBytes(keyGroup),
		valueCodec: valueCodec,
	}, nil
}

// NewKeyedValue creates a KeyedValueState for the given primary key and namespace.
func (f *KeyedValueStateFactory[V]) NewKeyedValue(primaryKey []byte, namespace []byte) (*KeyedValueState[V], error) {
	if primaryKey == nil || namespace == nil {
		return nil, api.NewError(api.ErrStoreInternal, "primary key and namespace are required")
	}
	return &KeyedValueState[V]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  common.DupBytes(namespace),
	}, nil
}

type KeyedValueState[V any] struct {
	factory    *KeyedValueStateFactory[V]
	primaryKey []byte
	namespace  []byte
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
	return s.factory.store.Put(ck, encoded)
}

func (s *KeyedValueState[V]) Value() (V, bool, error) {
	var zero V
	ck := s.buildCK()
	raw, found, err := s.factory.store.Get(ck)
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
	return s.factory.store.Delete(s.buildCK())
}
