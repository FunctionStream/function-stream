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
	autoKeyCodec, err := inferOrderedKeyCodec[K]()
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

func (m *MapState[K, V]) Len() (uint64, error) {
	iter, err := m.store.ScanComplex(m.keyGroup, m.key, m.namespace)
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var count uint64
	for {
		has, err := iter.HasNext()
		if err != nil {
			return 0, err
		}
		if !has {
			return count, nil
		}
		_, _, ok, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			return count, nil
		}
		count++
	}
}

func (m *MapState[K, V]) Entries() ([]MapEntry[K, V], error) {
	iter, err := m.store.ScanComplex(m.keyGroup, m.key, m.namespace)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := make([]MapEntry[K, V], 0, 16)
	for {
		has, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !has {
			return out, nil
		}
		keyBytes, valueBytes, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		decodedKey, err := m.keyCodec.Decode(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("decode map key failed: %w", err)
		}
		decodedValue, err := m.valueCodec.Decode(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("decode map value failed: %w", err)
		}
		out = append(out, MapEntry[K, V]{Key: decodedKey, Value: decodedValue})
	}
}

func (m *MapState[K, V]) Range(startInclusive K, endExclusive K) ([]MapEntry[K, V], error) {
	startBytes, err := m.keyCodec.Encode(startInclusive)
	if err != nil {
		return nil, fmt.Errorf("encode map start key failed: %w", err)
	}
	endBytes, err := m.keyCodec.Encode(endExclusive)
	if err != nil {
		return nil, fmt.Errorf("encode map end key failed: %w", err)
	}
	userKeys, err := m.store.ListComplex(m.keyGroup, m.key, m.namespace, startBytes, endBytes)
	if err != nil {
		return nil, err
	}
	out := make([]MapEntry[K, V], 0, len(userKeys))
	for _, userKey := range userKeys {
		raw, found, err := m.store.Get(m.ck(userKey))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, api.NewError(api.ErrResultUnexpected, "map range key disappeared during scan")
		}
		decodedKey, err := m.keyCodec.Decode(userKey)
		if err != nil {
			return nil, fmt.Errorf("decode map key failed: %w", err)
		}
		decodedValue, err := m.valueCodec.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("decode map value failed: %w", err)
		}
		out = append(out, MapEntry[K, V]{Key: decodedKey, Value: decodedValue})
	}
	return out, nil
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

func inferOrderedKeyCodec[K any]() (codec.Codec[K], error) {
	var zero K
	switch any(zero).(type) {
	case string:
		return any(codec.StringCodec{}).(codec.Codec[K]), nil
	case []byte:
		return any(codec.BytesCodec{}).(codec.Codec[K]), nil
	case bool:
		return any(codec.BoolCodec{}).(codec.Codec[K]), nil
	case int:
		return any(codec.OrderedIntCodec{}).(codec.Codec[K]), nil
	case uint:
		return any(codec.OrderedUintCodec{}).(codec.Codec[K]), nil
	case int32:
		return any(codec.OrderedInt32Codec{}).(codec.Codec[K]), nil
	case uint32:
		return any(codec.OrderedUint32Codec{}).(codec.Codec[K]), nil
	case int64:
		return any(codec.OrderedInt64Codec{}).(codec.Codec[K]), nil
	case uint64:
		return any(codec.OrderedUint64Codec{}).(codec.Codec[K]), nil
	case float32:
		return any(codec.OrderedFloat32Codec{}).(codec.Codec[K]), nil
	case float64:
		return any(codec.OrderedFloat64Codec{}).(codec.Codec[K]), nil
	default:
		return nil, api.NewError(api.ErrStoreInternal, "unsupported map key type for auto codec")
	}
}
