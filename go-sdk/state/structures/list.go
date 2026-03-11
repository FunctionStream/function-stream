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
	"encoding/binary"
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type ListState[T any] struct {
	store          common.Store
	complexKey     api.ComplexKey
	codec          codec.Codec[T]
	fixedSize      int
	serialize      func(T) ([]byte, error)
	decode         func([]byte) ([]T, error)
	serializeBatch func([]T) ([]byte, error)
}

// NewListStateFromContext creates a ListState using the store from ctx.GetOrCreateStore(storeName).
func NewListStateFromContext[T any](ctx api.Context, storeName string, itemCodec codec.Codec[T]) (*ListState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	return newListState(store, itemCodec)
}

// NewListStateFromContextAutoCodec creates a ListState with default codec for T from ctx.GetOrCreateStore(storeName).
func NewListStateFromContextAutoCodec[T any](ctx api.Context, storeName string) (*ListState[T], error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	itemCodec, err := codec.DefaultCodecFor[T]()
	if err != nil {
		return nil, err
	}
	return newListState(store, itemCodec)
}

func newListState[T any](store common.Store, itemCodec codec.Codec[T]) (*ListState[T], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "list state store must not be nil")
	}
	if itemCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "list state codec must not be nil")
	}
	fixedSize, isFixed := codec.FixedEncodedSize[T](itemCodec)
	l := &ListState[T]{
		store: store,
		complexKey: api.ComplexKey{
			KeyGroup:  []byte{},
			Key:       []byte{},
			Namespace: []byte{},
			UserKey:   []byte{},
		},
		codec:     itemCodec,
		fixedSize: fixedSize,
	}
	if isFixed {
		l.serialize = l.serializeValueFixed
		l.serializeBatch = l.serializeValuesFixedBatch
		l.decode = l.deserializeValuesFixed
	} else {
		l.serialize = l.serializeValueVarLen
		l.serializeBatch = l.serializeValuesVarLenBatch
		l.decode = l.deserializeValuesVarLen
	}
	return l, nil
}

func (l *ListState[T]) Add(value T) error {
	payload, err := l.serialize(value)
	if err != nil {
		return err
	}
	return l.store.Merge(l.complexKey, payload)
}

func (l *ListState[T]) AddAll(values []T) error {
	payload, err := l.serializeBatch(values)
	if err != nil {
		return err
	}
	if err := l.store.Merge(l.complexKey, payload); err != nil {
		return err
	}
	return nil
}

func (l *ListState[T]) Get() ([]T, error) {
	raw, found, err := l.store.Get(l.complexKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return []T{}, nil
	}
	return l.decode(raw)
}

func (l *ListState[T]) Update(values []T) error {
	if len(values) == 0 {
		return l.Clear()
	}
	payload, err := l.serializeBatch(values)
	if err != nil {
		return err
	}
	return l.store.Put(l.complexKey, payload)
}

func (l *ListState[T]) Clear() error {
	return l.store.Delete(l.complexKey)
}

func (l *ListState[T]) serializeValueVarLen(value T) ([]byte, error) {
	encoded, err := l.codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode list value failed: %w", err)
	}
	out := make([]byte, 4, 4+len(encoded))
	binary.BigEndian.PutUint32(out, uint32(len(encoded)))
	out = append(out, encoded...)
	return out, nil
}

func (l *ListState[T]) serializeValuesVarLenBatch(values []T) ([]byte, error) {
	total := 0
	encodedValues := make([][]byte, 0, len(values))
	for _, value := range values {
		encoded, err := l.codec.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("encode list value failed: %w", err)
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

func (l *ListState[T]) deserializeValuesVarLen(raw []byte) ([]T, error) {
	out := make([]T, 0, 16)
	idx := 0
	for idx < len(raw) {
		if len(raw)-idx < 4 {
			return nil, api.NewError(api.ErrResultUnexpected, "corrupted list payload: truncated length")
		}
		itemLen := int(binary.BigEndian.Uint32(raw[idx : idx+4]))
		idx += 4
		if itemLen < 0 || len(raw)-idx < itemLen {
			return nil, api.NewError(api.ErrResultUnexpected, "corrupted list payload: invalid element length")
		}
		itemRaw := raw[idx : idx+itemLen]
		idx += itemLen
		value, err := l.codec.Decode(itemRaw)
		if err != nil {
			return nil, fmt.Errorf("decode list value failed: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

func (l *ListState[T]) serializeValueFixed(value T) ([]byte, error) {
	if l.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}
	encoded, err := l.codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode list value failed: %w", err)
	}
	if len(encoded) != l.fixedSize {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec encoded unexpected length: got %d, want %d", len(encoded), l.fixedSize)
	}
	out := make([]byte, 0, l.fixedSize)
	out = append(out, encoded...)
	return out, nil
}

func (l *ListState[T]) serializeValuesFixedBatch(values []T) ([]byte, error) {
	if l.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}
	total := l.fixedSize * len(values)
	out := make([]byte, 0, total)
	for _, value := range values {
		encoded, err := l.codec.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("encode list value failed: %w", err)
		}
		if len(encoded) != l.fixedSize {
			return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec encoded unexpected length: got %d, want %d", len(encoded), l.fixedSize)
		}
		out = append(out, encoded...)
	}
	return out, nil
}

func (l *ListState[T]) deserializeValuesFixed(raw []byte) ([]T, error) {
	if l.fixedSize <= 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "fixed-size codec must report positive size")
	}
	if len(raw)%l.fixedSize != 0 {
		return nil, api.NewError(api.ErrResultUnexpected, "corrupted list payload: fixed-size data length mismatch")
	}
	out := make([]T, 0, len(raw)/l.fixedSize)
	for idx := 0; idx < len(raw); idx += l.fixedSize {
		itemRaw := raw[idx : idx+l.fixedSize]
		value, err := l.codec.Decode(itemRaw)
		if err != nil {
			return nil, fmt.Errorf("decode list value failed: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}
