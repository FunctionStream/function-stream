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
	name string,
	accCodec codec.Codec[ACC],
	aggFunc AggregateFunc[T, ACC, R],
) (*AggregatingState[T, ACC, R], error) {
	stateName, err := common.ValidateStateName(name)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state %q store must not be nil", stateName)
	}
	if accCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state %q acc codec must not be nil", stateName)
	}
	if aggFunc == nil {
		return nil, api.NewError(api.ErrStoreInternal, "aggregating state %q agg func must not be nil", stateName)
	}
	ck := api.ComplexKey{
		KeyGroup:  []byte(common.StateAggregatingPrefix),
		Key:       []byte(stateName),
		Namespace: []byte("data"),
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
