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

package impl

import (
	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/keyed"
	"github.com/functionstream/function-stream/go-sdk/state/structures"
)

func getStoreFromContext(ctx api.Context, storeName string) (*storeImpl, error) {
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	s, ok := store.(*storeImpl)
	if !ok {
		return nil, api.NewError(api.ErrStoreInternal, "store %q is not the default implementation", storeName)
	}
	return s, nil
}

func NewValueState[T any](ctx api.Context, storeName string, valueCodec codec.Codec[T]) (*structures.ValueState[T], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewValueState(s, valueCodec)
}

func NewListState[T any](ctx api.Context, storeName string, itemCodec codec.Codec[T]) (*structures.ListState[T], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewListState(s, itemCodec)
}

func NewMapState[K any, V any](ctx api.Context, storeName string, keyCodec codec.Codec[K], valueCodec codec.Codec[V]) (*structures.MapState[K, V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewMapState(s, keyCodec, valueCodec)
}

func NewMapStateAutoKeyCodec[K any, V any](ctx api.Context, storeName string, valueCodec codec.Codec[V]) (*structures.MapState[K, V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewMapStateAutoKeyCodec[K, V](s, valueCodec)
}

func NewPriorityQueueState[T any](ctx api.Context, storeName string, itemCodec codec.Codec[T]) (*structures.PriorityQueueState[T], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewPriorityQueueState(s, itemCodec)
}

func NewAggregatingState[T any, ACC any, R any](ctx api.Context, storeName string, accCodec codec.Codec[ACC], aggFunc structures.AggregateFunc[T, ACC, R]) (*structures.AggregatingState[T, ACC, R], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewAggregatingState(s, accCodec, aggFunc)
}

func NewReducingState[V any](ctx api.Context, storeName string, valueCodec codec.Codec[V], reduceFunc structures.ReduceFunc[V]) (*structures.ReducingState[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return structures.NewReducingState(s, valueCodec, reduceFunc)
}

func NewKeyedListStateFactory[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V]) (*keyed.KeyedListStateFactory[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedListStateFactory(s, keyGroup, valueCodec)
}

func NewKeyedListStateFactoryAutoCodec[V any](ctx api.Context, storeName string, keyGroup []byte) (*keyed.KeyedListStateFactory[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedListStateFactoryAutoCodec[V](s,  keyGroup)
}

func NewKeyedValueStateFactory[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V]) (*keyed.KeyedValueStateFactory[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedValueStateFactory(s, keyGroup, valueCodec)
}

func NewKeyedMapStateFactory[MK any, MV any](ctx api.Context, storeName string, keyGroup []byte, keyCodec codec.Codec[MK], valueCodec codec.Codec[MV]) (*keyed.KeyedMapStateFactory[MK, MV], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedMapStateFactory(s, keyGroup, keyCodec, valueCodec)
}

func NewKeyedPriorityQueueStateFactory[V any](ctx api.Context, storeName string, keyGroup []byte, itemCodec codec.Codec[V]) (*keyed.KeyedPriorityQueueStateFactory[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedPriorityQueueStateFactory(s, keyGroup, itemCodec)
}

func NewKeyedAggregatingStateFactory[T any, ACC any, R any](ctx api.Context, storeName string, keyGroup []byte, accCodec codec.Codec[ACC], aggFunc keyed.AggregateFunc[T, ACC, R]) (*keyed.KeyedAggregatingStateFactory[T, ACC, R], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedAggregatingStateFactory(s, keyGroup, accCodec, aggFunc)
}

func NewKeyedReducingStateFactory[V any](ctx api.Context, storeName string, keyGroup []byte, valueCodec codec.Codec[V], reduceFunc keyed.ReduceFunc[V]) (*keyed.KeyedReducingStateFactory[V], error) {
	s, err := getStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return keyed.NewKeyedReducingStateFactory(s, keyGroup, valueCodec, reduceFunc)
}
