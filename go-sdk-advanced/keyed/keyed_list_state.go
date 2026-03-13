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
	"encoding/binary"
	"fmt"

	"github.com/functionstream/function-stream/go-sdk-advanced/codec"
	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type KeyedListStateFactory[V any] struct {
	store      common.Store
	keyGroup   []byte
	fixedSize  int
	valueCodec codec.Codec[V]
	isFixed    bool
}

// NewKeyedListStateFactoryFromContext creates a KeyedListStateFactory using the store from ctx.GetOrCreateStore(storeName).
func NewKeyedListStateFactoryFromContext[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V]) (*KeyedListStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newKeyedListStateFactory(store, keyGroup, valueCodec)
}

// NewKeyedListStateFactoryAutoCodecFromContext creates a KeyedListStateFactory with default value codec using the store from context.
func NewKeyedListStateFactoryAutoCodecFromContext[V any](ctx api.Context, storeName string, keyGroup []byte) (*KeyedListStateFactory[V], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newKeyedListStateFactoryAutoCodec[V](store, keyGroup)
}

func newKeyedListStateFactory[V any](store common.Store, keyGroup []byte, valueCodec codec.Codec[V]) (*KeyedListStateFactory[V], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list state factory store must not be nil")
	}
	if keyGroup == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list state factory key group must not be nil")
	}
	if valueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list value codec must not be nil")
	}
	fixedSize, isFixed := codec.FixedEncodedSize[V](valueCodec)
	return &KeyedListStateFactory[V]{
		store:      store,
		keyGroup:   common.DupBytes(keyGroup),
		fixedSize:  fixedSize,
		valueCodec: valueCodec,
		isFixed:    isFixed,
	}, nil
}

func newKeyedListStateFactoryAutoCodec[V any](store common.Store, keyGroup []byte) (*KeyedListStateFactory[V], error) {
	valueCodec, err := codec.DefaultCodecFor[V]()
	if err != nil {
		return nil, err
	}
	return newKeyedListStateFactory[V](store, keyGroup, valueCodec)
}

type KeyedListState[V any] struct {
	factory        *KeyedListStateFactory[V]
	complexKey     api.ComplexKey
	fixedSize      int
	valueCodec     codec.Codec[V]
	serialize      func(V) ([]byte, error)
	serializeBatch func([]V) ([]byte, error)
	decode         func([]byte) ([]V, error)
}

func newKeyedListFromFactory[V any](f *KeyedListStateFactory[V], key []byte, namespace []byte) (*KeyedListState[V], error) {
	if f == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list factory must not be nil")
	}
	if key == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list key must not be nil")
	}
	if namespace == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed list namespace must not be nil")
	}
	s := &KeyedListState[V]{
		factory:    f,
		valueCodec: f.valueCodec,
		complexKey: api.ComplexKey{
			KeyGroup:  f.keyGroup,
			Key:       key,
			Namespace: namespace,
			UserKey:   []byte{},
		},
		fixedSize: f.fixedSize,
	}
	if f.isFixed {
		s.serialize = s.serializeValueFixed
		s.serializeBatch = s.serializeValuesFixedBatch
		s.decode = s.deserializeValuesFixed
	} else {
		s.serialize = s.serializeValueVarLen
		s.serializeBatch = s.serializeValuesVarLenBatch
		s.decode = s.deserializeValuesVarLen
	}
	return s, nil
}

func NewKeyedListFromFactory[V any](f *KeyedListStateFactory[V], key []byte, namespace []byte) (*KeyedListState[V], error) {
	return newKeyedListFromFactory[V](f, key, namespace)
}

func (s *KeyedListState[V]) Add(value V) error {
	payload, err := s.serialize(value)
	if err != nil {
		return err
	}
	return s.factory.store.Merge(s.complexKey, payload)
}

func (s *KeyedListState[V]) AddAll(values []V) error {
	payload, err := s.serializeBatch(values)
	if err != nil {
		return err
	}
	if err := s.factory.store.Merge(s.complexKey, payload); err != nil {
		return err
	}
	return nil
}

func (s *KeyedListState[V]) Get() ([]V, error) {
	raw, found, err := s.factory.store.Get(s.complexKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return []V{}, nil
	}
	return s.decode(raw)
}

// Update replaces the list with the given values (one Put with batch payload).
func (s *KeyedListState[V]) Update(values []V) error {
	if len(values) == 0 {
		return s.Clear()
	}
	payload, err := s.serializeBatch(values)
	if err != nil {
		return err
	}
	return s.factory.store.Put(s.complexKey, payload)
}

func (s *KeyedListState[V]) Clear() error {
	return s.factory.store.Delete(s.complexKey)
}

func (s *KeyedListState[V]) serializeValueVarLen(value V) ([]byte, error) {
	encoded, err := s.valueCodec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode keyed list value failed: %w", err)
	}
	out := make([]byte, 4, 4+len(encoded))
	binary.BigEndian.PutUint32(out, uint32(len(encoded)))
	out = append(out, encoded...)
	return out, nil
}

func (s *KeyedListState[V]) serializeValuesVarLenBatch(values []V) ([]byte, error) {
	total := 0
	encodedValues := make([][]byte, 0, len(values))
	for _, value := range values {
		encoded, err := s.valueCodec.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("encode keyed list value failed: %w", err)
		}
		encodedValues = append(encodedValues, encoded)
		total += 4 + len(encoded)
	}
	out := make([]byte, 0, total)
	for _, encoded := range encodedValues {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(encoded)))
		out = append(out, lenBuf[:]...)
		out = append(out, encoded...)
	}
	return out, nil
}

func (s *KeyedListState[V]) deserializeValuesVarLen(raw []byte) ([]V, error) {
	out := make([]V, 0, 16)
	idx := 0
	for idx < len(raw) {
		if len(raw)-idx < 4 {
			return nil, api.NewError(api.ErrResultUnexpected, "corrupted keyed list payload: truncated length")
		}

		itemLen := int(binary.BigEndian.Uint32(raw[idx : idx+4]))
		idx += 4

		if itemLen < 0 || len(raw)-idx < itemLen {
			return nil, api.NewError(api.ErrResultUnexpected, "corrupted keyed list payload: invalid element length")
		}

		itemRaw := raw[idx : idx+itemLen]
		idx += itemLen

		value, err := s.valueCodec.Decode(itemRaw)
		if err != nil {
			return nil, fmt.Errorf("decode keyed list value failed: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

func (s *KeyedListState[V]) serializeValueFixed(value V) ([]byte, error) {
	if s.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}
	encoded, err := s.valueCodec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode keyed list value failed: %w", err)
	}
	if len(encoded) != s.fixedSize {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec encoded unexpected length: got %d, want %d", len(encoded), s.fixedSize)
	}
	out := make([]byte, 0, s.fixedSize)
	out = append(out, encoded...)
	return out, nil
}

func (s *KeyedListState[V]) serializeValuesFixedBatch(values []V) ([]byte, error) {
	if s.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}
	total := s.fixedSize * len(values)
	out := make([]byte, 0, total)
	for _, value := range values {
		encoded, err := s.valueCodec.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("encode keyed list value failed: %w", err)
		}
		if len(encoded) != s.fixedSize {
			return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec encoded unexpected length: got %d, want %d", len(encoded), s.fixedSize)
		}
		out = append(out, encoded...)
	}
	return out, nil
}

func (s *KeyedListState[V]) deserializeValuesFixed(raw []byte) ([]V, error) {
	if s.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}

	if len(raw)%s.fixedSize != 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "corrupted keyed list payload: fixed-size data length mismatch")
	}

	count := len(raw) / s.fixedSize
	out := make([]V, 0, count)

	for idx := 0; idx < len(raw); idx += s.fixedSize {
		itemRaw := raw[idx : idx+s.fixedSize]
		value, err := s.valueCodec.Decode(itemRaw)
		if err != nil {
			return nil, fmt.Errorf("decode keyed list value failed: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}
