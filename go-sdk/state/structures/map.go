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

type MapEntry[K any, V any] struct {
	Key   K
	Value V
}

type MapState[K any, V any] struct {
	store      common.Store
	keyGroup   []byte
	key        []byte
	namespace  []byte
	keyCodec   codec.Codec[K]
	valueCodec codec.Codec[V]
}

func NewMapState[K any, V any](store common.Store, keyCodec codec.Codec[K], valueCodec codec.Codec[V]) (*MapState[K, V], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "map state store must not be nil")
	}
	if keyCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "map state key codec must not be nil")
	}
	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "map state value codec must not be nil")
	}
	if !keyCodec.IsOrderedKeyCodec() {
		return nil, api.NewError(api.ErrStoreInternal, "map state key codec must be ordered (IsOrderedKeyCodec)")
	}
	return &MapState[K, V]{store: store, keyGroup: []byte{}, key: []byte{}, namespace: []byte{}, keyCodec: keyCodec, valueCodec: valueCodec}, nil
}

func NewMapStateAutoKeyCodec[K any, V any](store common.Store, valueCodec codec.Codec[V]) (*MapState[K, V], error) {
	autoKeyCodec, err := codec.DefaultCodecFor[K]()
	if err != nil {
		return nil, err
	}
	return NewMapState[K, V](store, autoKeyCodec, valueCodec)
}

func (m *MapState[K, V]) Put(key K, value V) error {
	encodedKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode map key failed: %w", err)
	}
	encodedValue, err := m.valueCodec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode map value failed: %w", err)
	}
	return m.store.Put(m.ck(encodedKey), encodedValue)
}

func (m *MapState[K, V]) Get(key K) (V, bool, error) {
	var zero V
	encodedKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return zero, false, fmt.Errorf("encode map key failed: %w", err)
	}
	raw, found, err := m.store.Get(m.ck(encodedKey))
	if err != nil {
		return zero, false, err
	}
	if !found {
		return zero, false, nil
	}
	decoded, err := m.valueCodec.Decode(raw)
	if err != nil {
		return zero, false, fmt.Errorf("decode map value failed: %w", err)
	}
	return decoded, true, nil
}

func (m *MapState[K, V]) Delete(key K) error {
	encodedKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode map key failed: %w", err)
	}
	return m.store.Delete(m.ck(encodedKey))
}

func (m *MapState[K, V]) Clear() error {
	return m.store.DeletePrefix(api.ComplexKey{KeyGroup: m.keyGroup, Key: m.key, Namespace: m.namespace, UserKey: nil})
}

func (m *MapState[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		it, err := m.store.ScanComplex(m.keyGroup, m.key, m.namespace)
		if err != nil {
			return
		}
		defer it.Close()

		for {
			has, err := it.HasNext()
			if err != nil || !has {
				return
			}
			keyRaw, valRaw, ok, err := it.Next()
			if err != nil || !ok {
				return
			}

			k, _ := m.keyCodec.Decode(keyRaw)
			v, _ := m.valueCodec.Decode(valRaw)

			if !yield(k, v) {
				return
			}
		}
	}
}

func (m *MapState[K, V]) ck(userKey []byte) api.ComplexKey {
	return api.ComplexKey{KeyGroup: m.keyGroup, Key: m.key, Namespace: m.namespace, UserKey: userKey}
}
