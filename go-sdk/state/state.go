package state

import (
	"github.com/functionstream/function-stream/go-sdk/api"
	statecodec "github.com/functionstream/function-stream/go-sdk/state/codec"
)

type Store = api.Store
type Context = api.Context
//
//type Codec[T any] = statecodec.Codec[T]
//
//type JSONCodec[T any] = statecodec.JSONCodec[T]
type BytesCodec = statecodec.BytesCodec
type StringCodec = statecodec.StringCodec
type BoolCodec = statecodec.BoolCodec

type Int64Codec = statecodec.Int64Codec
type Uint64Codec = statecodec.Uint64Codec
type Int32Codec = statecodec.Int32Codec
type Uint32Codec = statecodec.Uint32Codec
type Float64Codec = statecodec.Float64Codec
type Float32Codec = statecodec.Float32Codec

type OrderedInt64Codec = statecodec.OrderedInt64Codec
type OrderedIntCodec = statecodec.OrderedIntCodec
type OrderedUint64Codec = statecodec.OrderedUint64Codec
type OrderedUintCodec = statecodec.OrderedUintCodec
type OrderedInt32Codec = statecodec.OrderedInt32Codec
type OrderedUint32Codec = statecodec.OrderedUint32Codec
type OrderedFloat64Codec = statecodec.OrderedFloat64Codec
type OrderedFloat32Codec = statecodec.OrderedFloat32Codec
//
//type ValueState[T any] = structuresstate.ValueState[T]
//type MapEntry[K any, V any] = structuresstate.MapEntry[K, V]
//type MapState[K any, V any] = structuresstate.MapState[K, V]
//type ListState[T any] = structuresstate.ListState[T]
//type PriorityQueueState[T any] = structuresstate.PriorityQueueState[T]
//
//type KeyedStateFactory = keyedstate.KeyedValueStateFactory
//type KeyedValueStateFactory = keyedstate.KeyedValueStateFactory
//type KeyedMapStateFactory[MK any, MV any] = keyedstate.KeyedMapStateFactory[MK, MV]
//type KeyedListStateFactory[V any] = keyedstate.KeyedListStateFactory[V]
//type KeyedPriorityQueueStateFactory = keyedstate.KeyedPriorityQueueStateFactory
//type KeyedValueState[K any, V any] = keyedstate.KeyedValueState[K, V]
//type KeyedMapEntry[K any, V any] = keyedstate.KeyedMapEntry[K, V]
//type KeyedMapState[MK any, MV any] = keyedstate.KeyedMapState[MK, MV]
//type KeyedListState[V any] = keyedstate.KeyedListState[V]
//type KeyedPriorityItem[T any] = keyedstate.KeyedPriorityItem[T]
//type KeyedPriorityQueueState[K any, V any] = keyedstate.KeyedPriorityQueueState[K, V]
//
