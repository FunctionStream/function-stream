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

type AggregateFunc[T any, ACC any, R any] interface {
	CreateAccumulator() ACC
	Add(value T, accumulator ACC) ACC
	GetResult(accumulator ACC) R
	Merge(a ACC, b ACC) ACC
}

type AggregatingState[T any, ACC any, R any] struct {
	store      common.Store
	complexKey api.ComplexKey
	accCodec   codec.Codec[ACC]
	aggFunc    AggregateFunc[T, ACC, R]
}

func NewAggregatingState[T any, ACC any, R any](
	store common.Store,
	accCodec codec.Codec[ACC],
	aggFunc AggregateFunc[T, ACC, R],
) (*AggregatingState[T, ACC, R], error) {
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state store must not be nil")
	}
	if accCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state acc codec must not be nil")
	}
	if aggFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state agg func must not be nil")
	}
	ck := api.ComplexKey{
		KeyGroup:  []byte{},
		Key:       []byte{},
		Namespace: []byte{},
		UserKey:   []byte{},
	}
	return &AggregatingState[T, ACC, R]{
		store:      store,
		complexKey: ck,
		accCodec:   accCodec,
		aggFunc:    aggFunc,
	}, nil
}

func (s *AggregatingState[T, ACC, R]) buildCK() api.ComplexKey {
	return s.complexKey
}

func (s *AggregatingState[T, ACC, R]) Add(value T) error {
	ck := s.buildCK()

	raw, found, err := s.store.Get(ck)
	if err != nil {
		return fmt.Errorf("failed to get accumulator: %w", err)
	}

	var acc ACC
	if !found {
		acc = s.aggFunc.CreateAccumulator()
	} else {
		var err error
		acc, err = s.accCodec.Decode(raw)
		if err != nil {
			return fmt.Errorf("failed to decode accumulator: %w", err)
		}
	}

	newAcc := s.aggFunc.Add(value, acc)

	encoded, err := s.accCodec.Encode(newAcc)
	if err != nil {
		return fmt.Errorf("failed to encode new accumulator: %w", err)
	}
	return s.store.Put(ck, encoded)
}

func (s *AggregatingState[T, ACC, R]) Get() (R, bool, error) {
	var zero R
	ck := s.buildCK()
	raw, found, err := s.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}

	acc, err := s.accCodec.Decode(raw)
	if err != nil {
		return zero, false, err
	}

	return s.aggFunc.GetResult(acc), true, nil
}

func (s *AggregatingState[T, ACC, R]) Clear() error {
	return s.store.Delete(s.buildCK())
}
