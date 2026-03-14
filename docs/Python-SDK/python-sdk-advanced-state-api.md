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

# Python SDK — Advanced State API

This document describes the **high-level state API** for the Python SDK: typed state abstractions (ValueState, ListState, MapState, etc.) built on top of the low-level KvStore, with serialization via **codecs** and optional **keyed state** per primary key.

**Two separate libraries:** The advanced state API is provided by **functionstream-api-advanced**, which depends on the low-level **functionstream-api**. Install with: `pip install functionstream-api functionstream-api-advanced`. Import Codec, ValueState, ListState, MapState, etc. from `fs_api_advanced`.

| Library | Package | Contents |
|---------|---------|----------|
| **functionstream-api** (low-level) | `fs_api` | Context (getOrCreateKVStore, getConfig, emit only), KvStore, KvIterator, ComplexKey, error types. |
| **functionstream-api-advanced** (high-level) | `fs_api_advanced` | Codec, ValueState, ListState, MapState, PriorityQueueState, AggregatingState, ReducingState, Keyed\* factories and state types. |

---

## 1. Overview

Use the advanced state API when you need structured state (single value, list, map, priority queue, aggregation, reduction) without manual byte encoding or key layout. You can create state either from the **runtime Context** (e.g. `ctx.getOrCreateValueState(...)` when using functionstream-runtime) or via **type-level constructors** on the state class (recommended for clarity and reuse).

---

## 2. Creating State: Two Ways

### 2.1 From Context (getOrCreate\*)

When using **functionstream-api-advanced**, the runtime Context implementation (e.g. WitContext in functionstream-runtime) provides `getOrCreateValueState(store_name, codec)`, `getOrCreateValueStateAutoCodec(store_name)`, and the same pattern for ListState, MapState, PriorityQueueState, AggregatingState, ReducingState, and all Keyed\* factories; these delegate to the type-level `from_context` / `from_context_auto_codec` methods below.

### 2.2 From the state type (recommended)

Each state type and keyed factory provides:

- **With codec:** `XxxState.from_context(ctx, store_name, codec, ...)` — you pass the codec(s).
- **AutoCodec:** `XxxState.from_context_auto_codec(ctx, store_name)` or with optional type hint — the SDK uses a default codec (e.g. `PickleCodec`, or ordered codecs for map key / PQ element where required).

State instances are lightweight; you may create them per message in `process` or cache in the driver (e.g. in `init`). Same store name yields the same underlying store.

---

## 3. Non-Keyed State — Constructor Summary

| State              | With codec                                                                                                                               | AutoCodec                                                             |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| ValueState         | `ValueState.from_context(ctx, store_name, codec)`                                                                                        | `ValueState.from_context_auto_codec(ctx, store_name)`                 |
| ListState          | `ListState.from_context(ctx, store_name, codec)`                                                                                         | `ListState.from_context_auto_codec(ctx, store_name)`                  |
| MapState           | `MapState.from_context(ctx, store_name, key_codec, value_codec)` or `MapState.from_context_auto_key_codec(ctx, store_name, value_codec)` | —                                                                     |
| PriorityQueueState | `PriorityQueueState.from_context(ctx, store_name, codec)`                                                                                | `PriorityQueueState.from_context_auto_codec(ctx, store_name)`         |
| AggregatingState   | `AggregatingState.from_context(ctx, store_name, acc_codec, agg_func)`                                                                    | `AggregatingState.from_context_auto_codec(ctx, store_name, agg_func)` |
| ReducingState      | `ReducingState.from_context(ctx, store_name, value_codec, reduce_func)`                                                                  | `ReducingState.from_context_auto_codec(ctx, store_name, reduce_func)` |

All of the above can also be obtained via the corresponding `ctx.getOrCreate*` methods (e.g. `ctx.getOrCreateValueState(store_name, codec)`), which delegate to these constructors.

---

## 4. Keyed State — Factories and keyGroup / key / namespace

**Keyed state is for keyed operators.** When the stream is partitioned by a key (e.g. after keyBy), each key gets isolated state. You obtain a **factory** once (from context, store name, **namespace**, and **key_group**), then create state **per primary key** (the stream key for the current record).

### 4.1 keyGroup, key (primaryKey), and namespace

| Term          | API parameter                                                         | Meaning                                                                                                                                    |
|---------------|-----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| **key_group** | `key_group` when creating the factory                                 | The **keyed group**: identifies which keyed partition/group this state belongs to (e.g. one group for "counters", another for "sessions"). |
| **key**       | `primary_key` when constructing state (e.g. `KeyedValueState(factory, primary_key, namespace)`) | The **value of the stream key** for the current record (e.g. user ID, partition key). Each distinct key value gets isolated state.         |
| **namespace** | `namespace` (bytes) when creating the factory                         | **If a window function is present**, use the **window identifier as bytes**. **Without windows**, pass **empty bytes** (e.g. `b""`).       |

### 4.2 Factory constructor summary (keyed)

| Factory                        | With codec                                                                                                | AutoCodec                                                                                                                |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| KeyedValueStateFactory         | `KeyedValueStateFactory.from_context(ctx, store_name, key_group, value_codec)`                             | `KeyedValueStateFactory.from_context_auto_codec(ctx, store_name, key_group, value_type=None)`                             |
| KeyedListStateFactory          | `KeyedListStateFactory.from_context(ctx, store_name, key_group, value_codec)`                             | `KeyedListStateFactory.from_context_auto_codec(ctx, store_name, key_group, value_type=None)`                              |
| KeyedMapStateFactory           | `KeyedMapStateFactory.from_context(ctx, store_name, key_group, map_key_codec, map_value_codec)`         | `KeyedMapStateFactory.from_context_auto_codec(ctx, store_name, key_group, map_key_type=None, map_value_type=None)`       |
| KeyedPriorityQueueStateFactory | `KeyedPriorityQueueStateFactory.from_context(ctx, store_name, key_group, item_codec)`                     | `KeyedPriorityQueueStateFactory.from_context_auto_codec(ctx, store_name, key_group, item_type=None)`                     |
| KeyedAggregatingStateFactory   | `KeyedAggregatingStateFactory.from_context(ctx, store_name, key_group, acc_codec, agg_func)`               | `KeyedAggregatingStateFactory.from_context_auto_codec(ctx, store_name, key_group, agg_func, acc_type=None)`              |
| KeyedReducingStateFactory      | `KeyedReducingStateFactory.from_context(ctx, store_name, key_group, value_codec, reduce_func)`           | `KeyedReducingStateFactory.from_context_auto_codec(ctx, store_name, key_group, reduce_func, value_type=None)`           |

You can also use the corresponding `ctx.getOrCreateKeyed*Factory(...)` methods, which delegate to these constructors.

### 4.3 KeyedValueState

KeyedValueState aligns with the Go SDK: the factory takes only `key_group` (no namespace). Factory: `KeyedValueStateFactory.from_context(ctx, store_name, key_group, value_codec)` or `from_context_auto_codec(ctx, store_name, key_group, value_type=None)`. Construct state: `KeyedValueState(factory, primary_key, namespace)` with e.g. `namespace = state_name.encode("utf-8")`. State methods: `update(value)`, `value()` (returns `(value, found)`), `clear()`.

### 4.4 KeyedListState

KeyedListState aligns with the Go SDK: the factory takes only `key_group` (no namespace); **key** and **namespace** are passed when creating the list. Factory: `KeyedListStateFactory.from_context(ctx, store_name, key_group, value_codec)` or `from_context_auto_codec(ctx, store_name, key_group, value_type=None)`. Create list: `factory.new_keyed_list(key, namespace)`, yielding `KeyedListState[V]`. State methods: `add(value)`, `add_all(values)`, `get()` (returns `List[V]`), `update(values)` (clears then writes the full list), `clear()`.

### 4.5 KeyedAggregatingState

KeyedAggregatingState aligns with the Go SDK: the factory takes only `key_group` (no namespace). Factory: `KeyedAggregatingStateFactory.from_context(ctx, store_name, key_group, acc_codec, agg_func)` or `from_context_auto_codec(ctx, store_name, key_group, agg_func, acc_type=None)`. Create state: `factory.new_aggregating_state(primary_key, state_name="")`, yielding `KeyedAggregatingState[T, ACC, R]` bound to that (primary_key, namespace=state_name). State methods: `add(value)` (merge into this state’s accumulator), `get()` (returns `(result, found)`), `clear()`.

### 4.6 KeyedMapState

KeyedMapState aligns with the Go SDK: the factory takes only `key_group` (no namespace), and the map key codec must be ordered. Factory: `KeyedMapStateFactory.from_context(ctx, store_name, key_group, map_key_codec, map_value_codec)` or `from_context_auto_codec(ctx, store_name, key_group, map_key_type=None, map_value_type=None)`. Create map: `factory.new_keyed_map(primary_key, map_name)` (map_name required, used as namespace), yielding `KeyedMapState[MK, MV]`. State methods: `put(map_key, value)`, `get(map_key)` (returns `(value, found)`), `delete(map_key)`, `clear()` (delete all entries in this map by prefix), `all()` (iterate over `(map_key, value)` pairs).

### 4.7 KeyedPriorityQueueState

KeyedPriorityQueueState aligns with the Go SDK: the factory takes only `key_group` (no namespace), and the element codec must be ordered. Factory: `KeyedPriorityQueueStateFactory.from_context(ctx, store_name, key_group, item_codec)` or `from_context_auto_codec(ctx, store_name, key_group, item_type=None)`. Create queue: `factory.new_keyed_priority_queue(primary_key, namespace)` (both required, bytes), yielding `KeyedPriorityQueueState[V]`. State methods: `add(value)`, `peek()` (returns `(min_element, found)`), `poll()` (remove and return min), `clear()` (delete all by prefix), `all()` (iterate over all elements in order).

### 4.8 KeyedReducingState

KeyedReducingState aligns with the Go SDK: the factory takes only `key_group` (no namespace). Factory: `KeyedReducingStateFactory.from_context(ctx, store_name, key_group, value_codec, reduce_func)` or `from_context_auto_codec(ctx, store_name, key_group, reduce_func, value_type=None)`. Create state: `factory.new_reducing_state(primary_key, namespace)` (both required, bytes), yielding `KeyedReducingState[V]`. State methods: `add(value)` (merge with current value via reduce_func and put), `get()` (returns `(value, found)`), `clear()`.

---

## 5. Examples

### 5.1 ValueState (from_context_auto_codec)

Import ValueState from **fs_api_advanced** (Codec, ListState, MapState, etc. are in the same package):

```python
from fs_api import FSProcessorDriver, Context
from fs_api_advanced import ValueState

class CounterProcessor(FSProcessorDriver):
    def process(self, ctx: Context, source_id: int, data: bytes):
        state = ValueState.from_context_auto_codec(ctx, "my-store")
        cur = state.value()
        if cur is None:
            cur = 0
        state.update(cur + 1)
        ctx.emit(str(cur + 1).encode(), 0)
```

### 5.2 KeyedValueState (keyed operator)

When the stream is partitioned by key, create the factory in `init` and obtain state per record’s `primary_key` in `process`, then use `update(value)` / `value()` / `clear()`:

```python
from fs_api import FSProcessorDriver, Context
from fs_api_advanced import KeyedValueState, KeyedValueStateFactory

class KeyedCounterProcessor(FSProcessorDriver):
    def init(self, ctx: Context, config: dict):
        self._factory = KeyedValueStateFactory.from_context_auto_codec(
            ctx, "counters", b"by_key", value_type=int
        )

    def process(self, ctx: Context, source_id: int, data: bytes):
        primary_key = data[:8]
        state = KeyedValueState(self._factory, primary_key, "count".encode("utf-8"))
        cur, found = state.value()
        if not found:
            cur = 0
        state.update(cur + 1)
        ctx.emit(str(cur + 1).encode(), 0)
```

Same pattern for other state types: use `XxxState.from_context(ctx, store_name, ...)` or `XxxState.from_context_auto_codec(ctx, store_name)` as in the tables above.

---

## 6. See also

- [Python SDK Guide](python-sdk-guide.md) — main guide for fs_api, fs_client, and basic Context/KvStore usage.
