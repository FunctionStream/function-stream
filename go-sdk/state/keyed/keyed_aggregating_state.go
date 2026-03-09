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

type AggregateFunc[T any, ACC any, R any] interface {
	CreateAccumulator() ACC
	Add(value T, accumulator ACC) ACC
	GetResult(accumulator ACC) R
	Merge(a ACC, b ACC) ACC
}

type KeyedAggregatingStateFactory[T any, ACC any, R any] struct {
	inner    *keyedStateFactory
	groupKey []byte
	accCodec codec.Codec[ACC]
	aggFunc  AggregateFunc[T, ACC, R]
}

func NewKeyedAggregatingStateFactory[T any, ACC any, R any](
	store common.Store,
	keyGroup []byte,
	accCodec codec.Codec[ACC],
	aggFunc AggregateFunc[T, ACC, R],
) (*KeyedAggregatingStateFactory[T, ACC, R], error) {

	inner, err := newKeyedStateFactory(store, "", "aggregating")
	if err != nil {
		return nil, err
	}

	if keyGroup == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed aggregating state factory key_group must not be nil")
	}
	if accCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed aggregating state factory acc_codec must not be nil")
	}
	if aggFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed aggregating state factory agg_func must not be nil")
	}

	return &KeyedAggregatingStateFactory[T, ACC, R]{
		inner:    inner,
		groupKey: common.DupBytes(keyGroup),
		accCodec: accCodec,
		aggFunc:  aggFunc,
	}, nil
}

func (f *KeyedAggregatingStateFactory[T, ACC, R]) NewAggregatingState(primaryKey []byte, stateName string) (*KeyedAggregatingState[T, ACC, R], error) {
	return &KeyedAggregatingState[T, ACC, R]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  []byte(stateName),
	}, nil
}

type KeyedAggregatingState[T any, ACC any, R any] struct {
	factory    *KeyedAggregatingStateFactory[T, ACC, R]
	primaryKey []byte
	namespace  []byte
}

func (s *KeyedAggregatingState[T, ACC, R]) buildCK() api.ComplexKey {
	return api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   []byte{},
	}
}

func (s *KeyedAggregatingState[T, ACC, R]) Add(value T) error {
	ck := s.buildCK()

	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil {
		return fmt.Errorf("failed to get accumulator: %w", err)
	}

	var acc ACC
	if !found {
		acc = s.factory.aggFunc.CreateAccumulator()
	} else {
		var err error
		acc, err = s.factory.accCodec.Decode(raw)
		if err != nil {
			return fmt.Errorf("failed to decode accumulator: %w", err)
		}
	}

	newAcc := s.factory.aggFunc.Add(value, acc)

	encoded, err := s.factory.accCodec.Encode(newAcc)
	if err != nil {
		return fmt.Errorf("failed to encode new accumulator: %w", err)
	}
	return s.factory.inner.store.Put(ck, encoded)
}

func (s *KeyedAggregatingState[T, ACC, R]) Get() (R, bool, error) {
	var zero R
	ck := s.buildCK()
	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}

	acc, err := s.factory.accCodec.Decode(raw)
	if err != nil {
		return zero, false, err
	}

	return s.factory.aggFunc.GetResult(acc), true, nil
}

func (s *KeyedAggregatingState[T, ACC, R]) Clear() error {
	return s.factory.inner.store.Delete(s.buildCK())
}
