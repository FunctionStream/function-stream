package state

import "fmt"

const DefaultStateStoreName = "__fssdk_structured_state__"

func NewValueStateFromContext[T any](ctx Context, stateName string, codec Codec[T]) (*ValueState[T], error) {
	return NewValueStateFromContextWithStore[T](ctx, DefaultStateStoreName, stateName, codec)
}

func NewValueStateFromContextWithStore[T any](ctx Context, storeName string, stateName string, codec Codec[T]) (*ValueState[T], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewValueState[T](store, stateName, codec)
}

func NewMapStateFromContext[K any, V any](ctx Context, stateName string, keyCodec Codec[K], valueCodec Codec[V]) (*MapState[K, V], error) {
	return NewMapStateFromContextWithStore[K, V](ctx, DefaultStateStoreName, stateName, keyCodec, valueCodec)
}

func NewMapStateFromContextWithStore[K any, V any](ctx Context, storeName string, stateName string, keyCodec Codec[K], valueCodec Codec[V]) (*MapState[K, V], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewMapState[K, V](store, stateName, keyCodec, valueCodec)
}

func NewListStateFromContext[T any](ctx Context, stateName string, codec Codec[T]) (*ListState[T], error) {
	return NewListStateFromContextWithStore[T](ctx, DefaultStateStoreName, stateName, codec)
}

func NewListStateFromContextWithStore[T any](ctx Context, storeName string, stateName string, codec Codec[T]) (*ListState[T], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewListState[T](store, stateName, codec)
}

func NewPriorityQueueStateFromContext[T any](ctx Context, stateName string, codec Codec[T]) (*PriorityQueueState[T], error) {
	return NewPriorityQueueStateFromContextWithStore[T](ctx, DefaultStateStoreName, stateName, codec)
}

func NewPriorityQueueStateFromContextWithStore[T any](ctx Context, storeName string, stateName string, codec Codec[T]) (*PriorityQueueState[T], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewPriorityQueueState[T](store, stateName, codec)
}

func NewKeyedStateFactoryFromContext(ctx Context, stateName string) (*KeyedStateFactory, error) {
	return NewKeyedStateFactoryFromContextWithStore(ctx, DefaultStateStoreName, stateName)
}

func NewKeyedStateFactoryFromContextWithStore(ctx Context, storeName string, stateName string) (*KeyedStateFactory, error) {
	return NewKeyedValueStateFactoryFromContextWithStore(ctx, storeName, stateName)
}

func NewKeyedValueStateFactoryFromContext(ctx Context, stateName string) (*KeyedValueStateFactory, error) {
	return NewKeyedValueStateFactoryFromContextWithStore(ctx, DefaultStateStoreName, stateName)
}

func NewKeyedValueStateFactoryFromContextWithStore(ctx Context, storeName string, stateName string) (*KeyedValueStateFactory, error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewKeyedValueStateFactory(store, stateName)
}

func NewKeyedMapStateFactoryFromContext[MK any, MV any](ctx Context, stateName string, mapKeyCodec Codec[MK], mapValueCodec Codec[MV]) (*KeyedMapStateFactory[MK, MV], error) {
	return NewKeyedMapStateFactoryFromContextWithStore[MK, MV](ctx, DefaultStateStoreName, stateName, mapKeyCodec, mapValueCodec)
}

func NewKeyedMapStateFactoryFromContextWithStore[MK any, MV any](ctx Context, storeName string, stateName string, mapKeyCodec Codec[MK], mapValueCodec Codec[MV]) (*KeyedMapStateFactory[MK, MV], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewKeyedMapStateFactory[MK, MV](store, stateName, mapKeyCodec, mapValueCodec)
}

func NewKeyedListStateFactoryFromContext[V any](ctx Context, stateName string, keyGroup []byte, valueCodec Codec[V]) (*KeyedListStateFactory[V], error) {
	return NewKeyedListStateFactoryFromContextWithStore[V](ctx, DefaultStateStoreName, stateName, keyGroup, valueCodec)
}

func NewKeyedListStateFactoryFromContextWithStore[V any](ctx Context, storeName string, stateName string, keyGroup []byte, valueCodec Codec[V]) (*KeyedListStateFactory[V], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewKeyedListStateFactory[V](store, stateName, keyGroup, valueCodec)
}

// NewKeyedListStateFactoryAutoCodecFromContext 从 Context 创建 KeyedListStateFactory，不显式传入 valueCodec，
// 由工厂根据类型 V 自动选择 codec（基础类型 / string / struct 用 JSON）。
func NewKeyedListStateFactoryAutoCodecFromContext[V any](ctx Context, stateName string, keyGroup []byte) (*KeyedListStateFactory[V], error) {
	return NewKeyedListStateFactoryAutoCodecFromContextWithStore[V](ctx, DefaultStateStoreName, stateName, keyGroup)
}

func NewKeyedListStateFactoryAutoCodecFromContextWithStore[V any](ctx Context, storeName string, stateName string, keyGroup []byte) (*KeyedListStateFactory[V], error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewKeyedListStateFactoryAutoCodec[V](store, stateName, keyGroup)
}

func NewKeyedPriorityQueueStateFactoryFromContext(ctx Context, stateName string) (*KeyedPriorityQueueStateFactory, error) {
	return NewKeyedPriorityQueueStateFactoryFromContextWithStore(ctx, DefaultStateStoreName, stateName)
}

func NewKeyedPriorityQueueStateFactoryFromContextWithStore(ctx Context, storeName string, stateName string) (*KeyedPriorityQueueStateFactory, error) {
	store, err := requireStoreFromContext(ctx, storeName)
	if err != nil {
		return nil, err
	}
	return NewKeyedPriorityQueueStateFactory(store, stateName)
}

func requireStoreFromContext(ctx Context, storeName string) (Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	store, err := ctx.GetOrCreateStore(storeName)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("context returned nil store for %q", storeName)
	}
	return store, nil
}
