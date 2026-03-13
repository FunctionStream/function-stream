<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Go SDK — Advanced State API

This document describes the **typed, high-level state API** for the Function Stream Go SDK: state abstractions (ValueState, ListState, MapState, PriorityQueueState, AggregatingState, ReducingState, and Keyed\* factories) built on top of the low-level `Store`, with serialization via **codecs** and optional **keyed state** per primary key. Use it when you need structured state without manual byte encoding or key layout.

**In this document:**

- [When to use which API](#1-when-to-use-which-api) — choose the right state type for your use case.
- [Packages and imports](#2-packages-and-imports) — where to find types and codecs.
- [Codec contract](#3-codec-contract-and-default-codecs) — encoding, decoding, and ordering requirements.
- [Creating state](#4-creating-state-with-codec-vs-autocodec) — explicit codec vs AutoCodec constructors.
- [Non-keyed state reference](#5-non-keyed-state-structures) — methods and constructor summary.
- [AggregateFunc and ReduceFunc](#6-aggregatefunc-and-reducefunc) — interfaces for aggregation and reduction.
- [Keyed state](#7-keyed-state-factories-and-per-key-instances) — keyGroup, primaryKey, namespace, and factory methods.
- [Error handling and best practices](#8-error-handling-and-best-practices) — production guidance.
- [Examples](#9-examples) — ValueState, Keyed list, MapState, and AggregatingState.

---

## 1. When to Use Which API

The advanced state API offers typed views over a single logical store. Pick the abstraction that matches your access pattern:

| Use case                                                | Recommended API               | Notes                                                      |
|---------------------------------------------------------|-------------------------------|------------------------------------------------------------|
| Single logical value (counter, config blob, last value) | **ValueState[T]**             | One value per store; replace on update.                    |
| Append-only sequence (event log, history)               | **ListState[T]**              | Batch add, full read/replace; no key iteration.            |
| Key-value map with range/iteration                      | **MapState[K,V]**             | Key type **must** have an ordered codec (e.g. primitives). |
| Priority queue (min/max, top-K)                         | **PriorityQueueState[T]**     | Element type **must** have an ordered codec.               |
| Running aggregate (sum, count, custom accumulator)      | **AggregatingState[T,ACC,R]** | Uses `AggregateFunc`; mergeable accumulators.              |
| Running reduce (binary combine)                         | **ReducingState[V]**          | Uses `ReduceFunc`; associative combine.                    |
| **Per-key** state (per user, per partition)             | **Keyed\*** factories         | For **keyed operators**; factory + primaryKey per record.  |
| Custom key layout, bulk scan, non-typed storage         | Low-level **Store**           | `Put`/`Get`/`ScanComplex`/`ComplexKey`; full control.      |

**Keyed vs non-keyed**

- **Keyed state** is for **keyed operators**: streams partitioned by a key (e.g. after keyBy). The runtime delivers records per key; each key should have isolated state. Obtain a **factory** once (from context, store name, and keyGroup), then create state **per primary key** (the stream key) via e.g. `factory.NewKeyedValue(primaryKey, stateName)`.
- **Non-keyed state** (ValueState, ListState, etc.) stores one logical entity per store. Use it when there is no key partitioning or you maintain a single global state.

---

## 2. Packages and Imports

**Two separate libraries:** low-level **go-sdk**, high-level **go-sdk-advanced** (depends on go-sdk).

| Package           | Import path                                                                  | Responsibility                                                                                                          |
|-------------------|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| **codec** (advanced) | `github.com/functionstream/function-stream/go-sdk-advanced/codec`               | `Codec[T]` interface and built-in codecs.                                                                               |
| **structures** (advanced) | `github.com/functionstream/function-stream/go-sdk-advanced/structures` | ValueState, ListState, MapState, PriorityQueueState, AggregatingState, ReducingState.                                |
| **keyed** (advanced) | `github.com/functionstream/function-stream/go-sdk-advanced/keyed`       | Keyed state factories and per-key types. Use in keyed operators.                                                          |

All state constructors take `api.Context` (i.e. `fssdk.Context`) and a **store name**. The store is obtained internally via `ctx.GetOrCreateStore(storeName)`. The same store name always refers to the same backing store (RocksDB in the default implementation).

---

## 3. Codec Contract and Default Codecs

### 3.1 Codec interface

`codec.Codec[T]`:

| Method                            | Description                                                                                                                                                                                    |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Encode(value T) ([]byte, error)` | Serialize a value to bytes.                                                                                                                                                                    |
| `Decode(data []byte) (T, error)`  | Deserialize from bytes.                                                                                                                                                                        |
| `EncodedSize() int`               | Fixed size if `> 0`; variable size if `<= 0` (used for list optimizations).                                                                                                                    |
| `IsOrderedKeyCodec() bool`        | If `true`, the byte encoding is **totally ordered**: lexicographic order of bytes corresponds to a well-defined order of values. **Required** for MapState key and PriorityQueueState element. |

### 3.2 DefaultCodecFor[T]()

`codec.DefaultCodecFor[T]()` returns a default codec for type `T`:

- **Primitives** (`int32`, `int64`, `uint32`, `uint64`, `float32`, `float64`, `string`, `bool`, `int`, `uint`, etc.): built-in codecs; **ordered** when used as map keys or PQ elements.
- **Struct, map, slice, array**: `JSONCodec[T]` — JSON encoding; **not ordered** (`IsOrderedKeyCodec() == false`). Do **not** use as MapState key or PriorityQueueState element type with AutoCodec (operations that depend on ordering may fail or panic).
- **Interface type without constraint**: returns error; the type parameter must be concrete.

### 3.3 Ordering requirement

For **MapState[K,V]** and **PriorityQueueState[T]**, the key (respectively element) type must use a codec with `IsOrderedKeyCodec() == true`. With **AutoCodec** constructors for Map or PQ, use primitive key/element types (e.g. `int64`, `string`) or provide an explicit ordered codec.

---

## 4. Creating State: With Codec vs AutoCodec

Two constructor families:

1. **Explicit codec** — `NewXxxFromContext(ctx, storeName, codec, ...)`  
   You supply a `Codec[T]` (and for Map: key + value codecs; for Aggregating/Reducing: acc/value codec plus function). Full control over encoding and ordering.

2. **AutoCodec** — `NewXxxFromContextAutoCodec(ctx, storeName)` or `(ctx, storeName, aggFunc/reduceFunc)`  
   The SDK uses `codec.DefaultCodecFor[T]()` for the value/accumulator type. For Map and PQ, the key/element type must have an **ordered** default (primitives); otherwise creation or operations may return `ErrStoreInternal`.

State instances are **lightweight**. You can create them per call (e.g. inside `Process`) or cache in the Driver (e.g. in `Init`). The same store name always refers to the same underlying store; only the typed view differs.

---

## 5. Non-Keyed State (structures)

### 5.1 Semantics and methods

| State                         | Semantics                                       | Main methods                                                                                                               | Ordered codec?     |
|-------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|--------------------|
| **ValueState[T]**             | Single replaceable value.                       | `Update(value T) error`; `Value() (T, bool, error)`; `Clear() error`                                                       | No                 |
| **ListState[T]**              | Append-only list; batch add and full replace.   | `Add(value T) error`; `AddAll(values []T) error`; `Get() ([]T, error)`; `Update(values []T) error`; `Clear() error`        | No                 |
| **MapState[K,V]**             | Key-value map; iteration via `All()`.           | `Put(key K, value V) error`; `Get(key K) (V, bool, error)`; `Delete(key K) error`; `Clear() error`; `All() iter.Seq2[K,V]` | **Key K: yes**     |
| **PriorityQueueState[T]**     | Priority queue (min-first by encoded order).    | `Add(value T) error`; `Peek() (T, bool, error)`; `Poll() (T, bool, error)`; `Clear() error`; `All() iter.Seq[T]`           | **Item T: yes**    |
| **AggregatingState[T,ACC,R]** | Running aggregation with mergeable accumulator. | `Add(value T) error`; `Get() (R, bool, error)`; `Clear() error`                                                            | No (ACC codec any) |
| **ReducingState[V]**          | Running reduce with binary combine.             | `Add(value V) error`; `Get() (V, bool, error)`; `Clear() error`                                                            | No                 |

### 5.2 Constructor summary (non-keyed)

| State                     | With codec                                                                                                                         | AutoCodec                                                             |
|---------------------------|------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| ValueState[T]             | `NewValueStateFromContext(ctx, storeName, valueCodec)`                                                                             | `NewValueStateFromContextAutoCodec[T](ctx, storeName)`                |
| ListState[T]              | `NewListStateFromContext(ctx, storeName, itemCodec)`                                                                               | `NewListStateFromContextAutoCodec[T](ctx, storeName)`                 |
| MapState[K,V]             | `NewMapStateFromContext(ctx, storeName, keyCodec, valueCodec)` or `NewMapStateAutoKeyCodecFromContext(ctx, storeName, valueCodec)` | `NewMapStateFromContextAutoCodec[K,V](ctx, storeName)`                |
| PriorityQueueState[T]     | `NewPriorityQueueStateFromContext(ctx, storeName, itemCodec)`                                                                      | `NewPriorityQueueStateFromContextAutoCodec[T](ctx, storeName)`        |
| AggregatingState[T,ACC,R] | `NewAggregatingStateFromContext(ctx, storeName, accCodec, aggFunc)`                                                                | `NewAggregatingStateFromContextAutoCodec(ctx, storeName, aggFunc)`    |
| ReducingState[V]          | `NewReducingStateFromContext(ctx, storeName, valueCodec, reduceFunc)`                                                              | `NewReducingStateFromContextAutoCodec[V](ctx, storeName, reduceFunc)` |

---

## 6. AggregateFunc and ReduceFunc

### 6.1 AggregateFunc[T, ACC, R]

**AggregatingState** requires an **AggregateFunc[T, ACC, R]** (in package `structures`):

| Method                              | Description                                                                         |
|-------------------------------------|-------------------------------------------------------------------------------------|
| `CreateAccumulator() ACC`           | Initial accumulator for empty state.                                                |
| `Add(value T, accumulator ACC) ACC` | Fold one input value into the accumulator.                                          |
| `GetResult(accumulator ACC) R`      | Produce the final result from the accumulator.                                      |
| `Merge(a, b ACC) ACC`               | Combine two accumulators (e.g. for merge in distributed or checkpointed execution). |

### 6.2 ReduceFunc[V]

**ReducingState** requires a **ReduceFunc[V]** (function type): `func(value1, value2 V) (V, error)`. It must be **associative** (and ideally commutative) so that repeated application yields a well-defined reduced value.

---

## 7. Keyed State — Factories and Per-Key Instances

Keyed state is for **keyed operators**: when the stream is partitioned by a key (e.g. after keyBy), each key is processed with isolated state. You obtain a **factory** once (from context, store name, and **keyGroup**), then create state **per primary key** — the stream key for the current record (e.g. user ID, partition key).

State is organized by **keyGroup** ([]byte) and **primary key** ([]byte). Create the factory from context, store name, and keyGroup; then call factory methods to get state for a given primary key.

### 7.1 keyGroup, key (primaryKey), and namespace

The Keyed API maps onto the store’s **ComplexKey** with three dimensions:

| Term          | Where it appears                                                                                               | Meaning                                                                                                                                                                                                                                |
|---------------|----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **keyGroup**  | Argument when creating the factory                                                                             | The **keyed group**: identifies which keyed partition/group this state belongs to. Use one keyGroup per logical “keyed group” or state kind (e.g. `[]byte("counters")`, `[]byte("sessions")`). Same keyed group ⇒ same keyGroup bytes. |
| **key**       | `primaryKey` in factory methods (e.g. `NewKeyedValue(primaryKey, ...)`, `NewKeyedList(primaryKey, namespace)`) | The **value of the stream key**: the key that partitioned the stream, serialized as bytes (e.g. user ID, partition key). Each distinct primaryKey gets isolated state.                                                                 |
| **namespace** | `namespace` ([]byte) in factory methods that take it                                                           | **With window functions**: use the **window identifier as bytes** (e.g. serialized window bounds or window ID) so state is scoped per key *and* per window. **Without windows**: pass **empty bytes** (`nil` or `[]byte{}`).           |

**Summary:** **keyGroup** = keyed group identifier; **key** (primaryKey) = stream key value; **namespace** = window bytes when using windows, otherwise empty.

### 7.2 Factory constructor summary (keyed)

| Factory                               | With codec                                                                                  | AutoCodec                                                                                   |
|---------------------------------------|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| KeyedValueStateFactory[V]             | `NewKeyedValueStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                | `NewKeyedValueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`                |
| KeyedListStateFactory[V]              | `NewKeyedListStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                 | `NewKeyedListStateFactoryAutoCodecFromContext[V](ctx, storeName, keyGroup)`                 |
| KeyedMapStateFactory[MK,MV]           | `NewKeyedMapStateFactoryFromContext(ctx, storeName, keyGroup, keyCodec, valueCodec)`        | `NewKeyedMapStateFactoryFromContextAutoCodec[MK,MV](ctx, storeName, keyGroup)`              |
| KeyedPriorityQueueStateFactory[V]     | `NewKeyedPriorityQueueStateFactoryFromContext(ctx, storeName, keyGroup, itemCodec)`         | `NewKeyedPriorityQueueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`        |
| KeyedAggregatingStateFactory[T,ACC,R] | `NewKeyedAggregatingStateFactoryFromContext(ctx, storeName, keyGroup, accCodec, aggFunc)`   | `NewKeyedAggregatingStateFactoryFromContextAutoCodec(ctx, storeName, keyGroup, aggFunc)`    |
| KeyedReducingStateFactory[V]          | `NewKeyedReducingStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec, reduceFunc)` | `NewKeyedReducingStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup, reduceFunc)` |

### 7.3 Obtaining per-key state from a factory

| Factory                           | Method                                                                                              | Returns                                        |
|-----------------------------------|-----------------------------------------------------------------------------------------------------|------------------------------------------------|
| KeyedValueStateFactory[V]         | `NewKeyedValue(primaryKey []byte, stateName string) (*KeyedValueState[V], error)`                   | One value state per (primaryKey, stateName).   |
| KeyedListStateFactory[V]          | `NewKeyedList(primaryKey []byte, namespace []byte) (*KeyedListState[V], error)`                     | List state per (primaryKey, namespace).        |
| KeyedMapStateFactory[MK,MV]       | `NewKeyedMap(primaryKey []byte, mapName string) (*KeyedMapState[MK,MV], error)`                     | Map state per (primaryKey, mapName).           |
| KeyedPriorityQueueStateFactory[V] | `NewKeyedPriorityQueue(primaryKey []byte, namespace []byte) (*KeyedPriorityQueueState[V], error)`   | PQ state per (primaryKey, namespace).          |
| KeyedAggregatingStateFactory      | `NewAggregatingState(primaryKey []byte, stateName string) (*KeyedAggregatingState[T,ACC,R], error)` | Aggregating state per (primaryKey, stateName). |
| KeyedReducingStateFactory[V]      | `NewReducingState(primaryKey []byte, namespace []byte) (*KeyedReducingState[V], error)`             | Reducing state per (primaryKey, namespace).    |

Here **primaryKey** is the stream key value; **namespace** is the window bytes when using window functions, or empty when not.

**Design tip:** Use a stable keyGroup per logical state (e.g. `[]byte("orders")`). In factory methods, pass the **stream key** your keyed operator received (e.g. from key extractor or message metadata) as primaryKey.

---

## 8. Error Handling and Best Practices

- **State API errors**: Creation and methods return errors compatible with `fssdk.SDKError` (e.g. `ErrStoreInternal`, `ErrStoreIO`). Codec encode/decode failures are wrapped (e.g. `"encode value state failed"`). Always check and handle errors in production.
- **Store naming**: Use stable, unique store names per logical state (e.g. `"counters"`, `"user-sessions"`). The same name in the same runtime refers to the same store.
- **Caching state**: You can create a state instance once in `Init` and reuse it in `Process`, or create it per message. Per-message creation is safe and keeps code simple when you do not need to amortize creation cost.
- **KeyGroup design**: For keyed state, use a consistent keyGroup per “logical table”. primaryKey is the **stream key** in keyed operators — use the key that identifies the current record. With **window functions**, pass the window identifier as **namespace** so state is per key and per window.
- **Ordered codec**: For MapState and PriorityQueueState with AutoCodec, use primitive key/element types. For custom struct keys, implement a `Codec[K]` with `IsOrderedKeyCodec() == true` and use the “with codec” constructor.

---

## 9. Examples

### 9.1 ValueState with AutoCodec (counter)

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk-advanced/structures"
)

func (p *MyProcessor) Process(ctx fssdk.Context, sourceID uint32, data []byte) error {
    valState, err := structures.NewValueStateFromContextAutoCodec[int64](ctx, "my-store")
    if err != nil {
        return err
    }
    cur, _, _ := valState.Value()
    if err := valState.Update(cur + 1); err != nil {
        return err
    }
    // emit or continue...
    return nil
}
```

### 9.2 Keyed list factory (keyed operator)

When the operator runs on a **keyed stream**, use a Keyed list factory and pass the **stream key** as primaryKey for each message:

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk-advanced/keyed"
)

type Order struct { Id string; Amount int64 }

func (p *MyProcessor) Init(ctx fssdk.Context, config map[string]string) error {
    keyGroup := []byte("orders")
    factory, err := keyed.NewKeyedListStateFactoryAutoCodecFromContext[Order](ctx, "app-store", keyGroup)
    if err != nil {
        return err
    }
    p.listFactory = factory
    return nil
}

func (p *MyProcessor) Process(ctx fssdk.Context, sourceID uint32, data []byte) error {
    userID := parseUserID(data)   // []byte — stream key for this record
    list, err := p.listFactory.NewKeyedList(userID, []byte{})  // empty namespace when no windows
    if err != nil {
        return err
    }
    if err := list.Add(Order{Id: "1", Amount: 100}); err != nil {
        return err
    }
    items, err := list.Get()
    if err != nil {
        return err
    }
    // use items...
    return nil
}
```

### 9.3 MapState and AggregatingState (sum)

Use the **go-sdk-advanced** `structures` package; `ctx` is `fssdk.Context` from the low-level go-sdk.

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk-advanced/structures"
)

// MapState: string -> int64 (both have ordered default codecs)
m, err := structures.NewMapStateFromContextAutoCodec[string, int64](ctx, "counts")
if err != nil {
    return err
}
_ = m.Put("a", 1)
v, ok, _ := m.Get("a")

// AggregatingState: sum of int64 (ACC = int64, R = int64)
type sumAgg struct{}
func (sumAgg) CreateAccumulator() int64   { return 0 }
func (sumAgg) Add(v int64, acc int64) int64 { return acc + v }
func (sumAgg) GetResult(acc int64) int64   { return acc }
func (sumAgg) Merge(a, b int64) int64      { return a + b }

agg, err := structures.NewAggregatingStateFromContextAutoCodec[int64, int64, int64](ctx, "sum-store", sumAgg{})
if err != nil {
    return err
}
_ = agg.Add(10)
total, _, _ := agg.Get()
```

---

## 10. See Also

- [Go SDK Guide](go-sdk-guide.md) — main guide: Driver, Context, Store, build, and deployment.
- [Go SDK — 高级状态 API（中文）](go-sdk-advanced-state-api-zh.md) — 本文档的中文版。
- [Python SDK — Advanced State API](../Python-SDK/python-sdk-advanced-state-api.md) — equivalent typed state API for the Python SDK.
- [examples/go-processor/README.md](../../examples/go-processor/README.md) — example operator and build instructions.
