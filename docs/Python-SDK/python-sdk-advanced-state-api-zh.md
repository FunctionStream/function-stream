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

# Python SDK — 高级状态 API

本文档介绍 Python SDK 的**高级状态 API**：基于底层 KvStore 的带类型状态抽象（ValueState、ListState、MapState 等），通过 **codec** 序列化，并支持按主键的 **keyed state**。设计与 [Go SDK 高级状态 API](../Go-SDK/go-sdk-guide.md#7-advanced-state-api) 对齐。

---

## 1. 概述

当需要结构化状态（单值、列表、Map、优先队列、聚合、归约）而不想手写字节编码或 key 布局时，可使用高级状态 API。创建方式有两种：通过 **Context**（如 `ctx.getOrCreateValueState(...)`）或通过状态类型上的**类型级构造方法**（推荐，便于复用，与 Go SDK 用法一致）。

---

## 2. 创建状态的两种方式

### 2.1 通过 Context（getOrCreate\*）

`Context` 提供 `getOrCreateValueState(store_name, codec)`、`getOrCreateValueStateAutoCodec(store_name)` 等方法，以及 ListState、MapState、PriorityQueueState、AggregatingState、ReducingState 与所有 Keyed\* 工厂的对应方法。运行时实现会委托给下面所述的类型级 `from_context` / `from_context_auto_codec`。

### 2.2 通过状态类型（推荐，与 Go SDK 一致）

每种状态类型和 keyed 工厂提供：

- **带 codec：** `XxxState.from_context(ctx, store_name, codec, ...)`
- **AutoCodec：** `XxxState.from_context_auto_codec(ctx, store_name)` 或带可选类型参数，由 SDK 使用默认 codec（如 PickleCodec，或 Map key / PQ 元素所需的有序 codec）。

状态实例是轻量的；可在每次 `process` 中创建，或在 driver 中（如 `init`）缓存。同一 store 名称对应同一底层 store。

---

## 3. 非 Keyed 状态 — 构造方法一览

| 状态类型               | 带 codec                                                                                                                                 | AutoCodec                                                             |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| ValueState         | `ValueState.from_context(ctx, store_name, codec)`                                                                                       | `ValueState.from_context_auto_codec(ctx, store_name)`                 |
| ListState          | `ListState.from_context(ctx, store_name, codec)`                                                                                        | `ListState.from_context_auto_codec(ctx, store_name)`                  |
| MapState           | `MapState.from_context(ctx, store_name, key_codec, value_codec)` 或 `MapState.from_context_auto_key_codec(ctx, store_name, value_codec)` | —                                                                     |
| PriorityQueueState | `PriorityQueueState.from_context(ctx, store_name, codec)`                                                                               | `PriorityQueueState.from_context_auto_codec(ctx, store_name)`         |
| AggregatingState   | `AggregatingState.from_context(ctx, store_name, acc_codec, agg_func)`                                                                   | `AggregatingState.from_context_auto_codec(ctx, store_name, agg_func)` |
| ReducingState      | `ReducingState.from_context(ctx, store_name, value_codec, reduce_func)`                                                                 | `ReducingState.from_context_auto_codec(ctx, store_name, reduce_func)` |

以上均可通过 Context 的 `ctx.getOrCreate*` 方法获得（如 `ctx.getOrCreateValueState(store_name, codec)`），其内部会委托给上述构造方法。

---

## 4. Keyed 状态 — 工厂与 key_group / key / namespace

**Keyed 状态面向 keyed 算子。** 流按 key 分区（如 keyBy）时，每个 key 拥有独立状态。可先获取一次**工厂**（通过 context、store 名称、**namespace** 和 **key_group**），再按**主键**（当前记录的流 key）创建状态。

### 4.1 key_group、key（主键）与 namespace

| 概念            | API 参数                                   | 含义                                                      |
|---------------|------------------------------------------|---------------------------------------------------------|
| **key_group** | 创建工厂时的 `key_group`                       | **keyed 组**：标识该状态所属分区/组（如一组 “counters”，另一组 “sessions”）。 |
| **key**       | 工厂方法参数（如 `new_keyed_value(primary_key)`） | 当前记录的**流 key 的值**（如用户 ID、分区 key）。不同 key 对应不同状态。         |
| **namespace** | 创建工厂时的 `namespace`（bytes）                | **有窗口时**为**窗口标识的 bytes**；**无窗口时**传**空 bytes**（如 `b""`）。 |

### 4.2 Keyed 工厂构造方法一览

| 工厂                             | 带 codec                                                                                                   | AutoCodec                                                                                                                |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| KeyedValueStateFactory         | `KeyedValueStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec)`                 | `KeyedValueStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_type=None)`                 |
| KeyedListStateFactory          | `KeyedListStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec)`                  | `KeyedListStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_type=None)`                  |
| KeyedMapStateFactory           | `KeyedMapStateFactory.from_context(ctx, store_name, namespace, key_group, key_codec, value_codec)`        | `KeyedMapStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, value_codec)`                       |
| KeyedPriorityQueueStateFactory | `KeyedPriorityQueueStateFactory.from_context(ctx, store_name, namespace, key_group, item_codec)`          | `KeyedPriorityQueueStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, item_type=None)`          |
| KeyedAggregatingStateFactory   | `KeyedAggregatingStateFactory.from_context(ctx, store_name, namespace, key_group, acc_codec, agg_func)`   | `KeyedAggregatingStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, agg_func, acc_type=None)`   |
| KeyedReducingStateFactory      | `KeyedReducingStateFactory.from_context(ctx, store_name, namespace, key_group, value_codec, reduce_func)` | `KeyedReducingStateFactory.from_context_auto_codec(ctx, store_name, namespace, key_group, reduce_func, value_type=None)` |

也可使用 Context 的 `ctx.getOrCreateKeyed*Factory(...)` 方法，其内部会委托给上述构造方法。

---

## 5. 示例：使用 from_context_auto_codec 的 ValueState

```python
from fs_api import FSProcessorDriver, Context
from fs_api.store import ValueState

class CounterProcessor(FSProcessorDriver):
    def process(self, ctx: Context, source_id: int, data: bytes):
        state = ValueState.from_context_auto_codec(ctx, "my-store")
        cur, found = state.value()
        if not found or cur is None:
            cur = 0
        state.update(cur + 1)
        ctx.emit(str(cur + 1).encode(), 0)
```

其他状态类型用法相同：按上表使用 `XxxState.from_context(ctx, store_name, ...)` 或 `XxxState.from_context_auto_codec(ctx, store_name)`。

---

## 6. 参见

- [Python SDK 指南](python-sdk-guide-zh.md) — fs_api、fs_client 及 Context/KvStore 基础用法。
- [Go SDK 指南 — 高级状态 API](../Go-SDK/go-sdk-guide.md#7-advanced-state-api) — Go SDK 中的等价 API。
