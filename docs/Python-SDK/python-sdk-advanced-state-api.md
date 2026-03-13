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

This document describes the **high-level state API** for the Python SDK: typed state abstractions (ValueState, ListState, MapState, etc.) built on top of the low-level KvStore, with serialization via **codecs** and optional **keyed state** per primary key. The design aligns with the [Go SDK Advanced State API](../Go-SDK/go-sdk-guide.md#7-advanced-state-api).

**Two separate libraries:** The advanced state API is provided by **functionstream-api-advanced**, which depends on the low-level **functionstream-api**. Install with: `pip install functionstream-api functionstream-api-advanced`. Import Codec, ValueState, ListState, MapState, etc. from `fs_api_advanced`.

| Library | Package | Contents |
|---------|---------|----------|
| **functionstream-api** (low-level) | `fs_api` | Context (getOrCreateKVStore, getConfig, emit only), KvStore, KvIterator, ComplexKey, error types. |
| **functionstream-api-advanced** (high-level) | `fs_api_advanced` | Codec, ValueState, ListState, MapState, PriorityQueueState, AggregatingState, ReducingState, Keyed\* factories and state types. |

---

## 1. Overview

Use the advanced state API when you need structured state (single value, list, map, priority queue, aggregation, reduction) without manual byte encoding or key layout. You can create state either from the **runtime Context** (e.g. `ctx.getOrCreateValueState(...)` when using functionstream-runtime) or via **type-level constructors** on the state class (recommended for clarity and reuse, same pattern as the Go SDK).

---

## 2. Creating State: Two Ways

### 2.1 From Context (getOrCreate\*)

When using **functionstream-api-advanced**, the runtime Context implementation (e.g. WitContext in functionstream-runtime) provides `getOrCreateValueState(store_name, codec)`, `getOrCreateValueStateAutoCodec(store_name)`, and the same pattern for ListState, MapState, PriorityQueueState, AggregatingState, ReducingState, and all Keyed\* factories; these delegate to the type-level `from_context` / `from_context_auto_codec` methods below.

### 2.2 From the state type (recommended, same as Go SDK)

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
| **key**       | The argument to factory methods (e.g. `new_keyed_value(primary_key)`) | The **value of the stream key** for the current record (e.g. user ID, partition key). Each distinct key value gets isolated state.         |
| **namespace** | `namespace` (bytes) when creating the factory                         | **If a window function is present**, use the **window identifier as bytes**. **Without windows**, pass **empty bytes** (e.g. `b""`).       |

### 4.2 Factory constructor summary (keyed)

| Factory                        | With codec                                                                                                | AutoCodec                                                                                                                |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| KeyedValueStateFactory         | `KeyedValueStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec)`                 | `KeyedValueStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_type=None)`                 |
| KeyedListStateFactory          | `KeyedListStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec)`                  | `KeyedListStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_type=None)`                  |
| KeyedMapStateFactory           | `KeyedMapStateFactory.from_context(ctx, store_name, namespace, key_group, key_codec, value_codec)`        | `KeyedMapStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_codec)`                       |
| KeyedPriorityQueueStateFactory | `KeyedPriorityQueueStateFactory.from_context(ctx, store_name, namespace, key_group, item_codec)`          | `KeyedPriorityQueueStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, item_type=None)`          |
| KeyedAggregatingStateFactory   | `KeyedAggregatingStateFactory.from_context(ctx, store_name, namespace, key_group, acc_codec, agg_func)`   | `KeyedAggregatingStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, agg_func, acc_type=None)`   |
| KeyedReducingStateFactory      | `KeyedReducingStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec, reduce_func)` | `KeyedReducingStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, reduce_func, value_type=None)` |

You can also use the corresponding `ctx.getOrCreateKeyed*Factory(...)` methods, which delegate to these constructors.

---

## 5. Example: ValueState with from_context_auto_codec

Import ValueState from **fs_api_advanced** (Codec, ListState, MapState, etc. are in the same package):

```python
from fs_api import FSProcessorDriver, Context
from fs_api_advanced import ValueState

class CounterProcessor(FSProcessorDriver):
    def process(self, ctx: Context, source_id: int, data: bytes):
        state = ValueState.from_context_auto_codec(ctx, "my-store")
        cur, found = state.value()
        if not found or cur is None:
            cur = 0
        state.update(cur + 1)
        ctx.emit(str(cur + 1).encode(), 0)
```

Same pattern for other state types: use `XxxState.from_context(ctx, store_name, ...)` or `XxxState.from_context_auto_codec(ctx, store_name)` as in the tables above.

---

## 6. See also

- [Python SDK Guide](python-sdk-guide.md) — main guide for fs_api, fs_client, and basic Context/KvStore usage.
- [Go SDK Guide — Advanced State API](../Go-SDK/go-sdk-guide.md#7-advanced-state-api) — equivalent API in the Go SDK.
