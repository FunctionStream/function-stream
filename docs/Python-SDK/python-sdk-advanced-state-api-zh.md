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

本文档介绍 Python SDK 的**高级状态 API**：基于底层 KvStore 的带类型状态抽象（ValueState、ListState、MapState 等），通过 **codec** 序列化，并支持按主键的 **keyed state**。

**两个独立库：** 高级状态 API 由 **functionstream-api-advanced** 提供，依赖低阶 **functionstream-api**。安装：`pip install functionstream-api functionstream-api-advanced`。使用时从 `fs_api_advanced` 导入 Codec、ValueState、ListState、MapState 等。

| 库 | 包名 | 内容 |
|----|------|------|
| **functionstream-api**（低阶） | `fs_api` | Context（仅 getOrCreateKVStore、getConfig、emit）、KvStore、KvIterator、ComplexKey、错误类。 |
| **functionstream-api-advanced**（高阶） | `fs_api_advanced` | Codec、ValueState、ListState、MapState、PriorityQueueState、AggregatingState、ReducingState、Keyed\* 工厂与状态类型。 |

---

## 1. 概述

当需要结构化状态（单值、列表、Map、优先队列、聚合、归约）而不想手写字节编码或 key 布局时，可使用高级状态 API。创建方式有两种：通过**运行时的 Context**（如使用 functionstream-runtime 时 `ctx.getOrCreateValueState(...)`）或通过状态类型上的**类型级构造方法**（推荐，便于复用）。

---

## 2. 创建状态的两种方式

### 2.1 通过 Context（getOrCreate\*）

使用 **functionstream-api-advanced** 时，运行时的 Context 实现（如 functionstream-runtime 的 WitContext）会提供 `getOrCreateValueState(store_name, codec)`、`getOrCreateValueStateAutoCodec(store_name)` 以及 ListState、MapState、PriorityQueueState、AggregatingState、ReducingState 与所有 Keyed\* 工厂的对应方法，内部委托给下面所述的类型级 `from_context` / `from_context_auto_codec`。

### 2.2 通过状态类型（推荐）

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
| **key**       | 构造状态时的 `primary_key`（如 `KeyedValueState(factory, primary_key, namespace)`） | 当前记录的**流 key 的值**（如用户 ID、分区 key）。不同 key 对应不同状态。         |
| **namespace** | 创建工厂时的 `namespace`（bytes）                | **有窗口时**为**窗口标识的 bytes**；**无窗口时**传**空 bytes**（如 `b""`）。 |

### 4.2 Keyed 工厂构造方法一览

| 工厂                             | 带 codec                                                                                                   | AutoCodec                                                                                                                |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| KeyedValueStateFactory         | `KeyedValueStateFactory.from_context(ctx, store_name, key_group, value_codec)`                             | `KeyedValueStateFactory.from_context_auto_codec(ctx, store_name, key_group, value_type=None)`                             |
| KeyedListStateFactory          | `KeyedListStateFactory.from_context(ctx, store_name, key_group, value_codec)`                             | `KeyedListStateFactory.from_context_auto_codec(ctx, store_name, key_group, value_type=None)`                              |
| KeyedMapStateFactory           | `KeyedMapStateFactory.from_context(ctx, store_name, key_group, map_key_codec, map_value_codec)`         | `KeyedMapStateFactory.from_context_auto_codec(ctx, store_name, key_group, map_key_type=None, map_value_type=None)`       |
| KeyedPriorityQueueStateFactory | `KeyedPriorityQueueStateFactory.from_context(ctx, store_name, key_group, item_codec)`                     | `KeyedPriorityQueueStateFactory.from_context_auto_codec(ctx, store_name, key_group, item_type=None)`                     |
| KeyedAggregatingStateFactory   | `KeyedAggregatingStateFactory.from_context(ctx, store_name, key_group, acc_codec, agg_func)`               | `KeyedAggregatingStateFactory.from_context_auto_codec(ctx, store_name, key_group, agg_func, acc_type=None)`              |
| KeyedReducingStateFactory      | `KeyedReducingStateFactory.from_context(ctx, store_name, key_group, value_codec, reduce_func)`           | `KeyedReducingStateFactory.from_context_auto_codec(ctx, store_name, key_group, reduce_func, value_type=None)`           |

也可使用 Context 的 `ctx.getOrCreateKeyed*Factory(...)` 方法，其内部会委托给上述构造方法。

### 4.3 KeyedValueState

KeyedValueState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace）。工厂：`KeyedValueStateFactory.from_context(ctx, store_name, key_group, value_codec)` 或 `from_context_auto_codec(ctx, store_name, key_group, value_type=None)`。构造状态：`KeyedValueState(factory, primary_key, namespace)`，其中 namespace 可为 `state_name.encode("utf-8")`。状态方法：`update(value)`、`value()`（返回 `(value, found)`）、`clear()`。

### 4.4 KeyedListState

KeyedListState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace），创建列表时再传入 **key** 与 **namespace**。工厂：`KeyedListStateFactory.from_context(ctx, store_name, key_group, value_codec)` 或 `from_context_auto_codec(ctx, store_name, key_group, value_type=None)`。创建列表：`factory.new_keyed_list(key, namespace)`，得到 `KeyedListState[V]`。状态方法：`add(value)`、`add_all(values)`、`get()`（返回 `List[V]`）、`update(values)`（先清空再整体写入）、`clear()`。

### 4.5 KeyedAggregatingState

KeyedAggregatingState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace）。工厂：`KeyedAggregatingStateFactory.from_context(ctx, store_name, key_group, acc_codec, agg_func)` 或 `from_context_auto_codec(ctx, store_name, key_group, agg_func, acc_type=None)`。创建状态：`factory.new_aggregating_state(primary_key, state_name="")`，得到绑定到该 (primary_key, namespace=state_name) 的 `KeyedAggregatingState[T, ACC, R]`。状态方法：`add(value)`（向当前状态的 accumulator 合并）、`get()`（返回 `(result, found)`）、`clear()`。

### 4.6 KeyedMapState

KeyedMapState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace），且 map key 的 codec 必须有序。工厂：`KeyedMapStateFactory.from_context(ctx, store_name, key_group, map_key_codec, map_value_codec)` 或 `from_context_auto_codec(ctx, store_name, key_group, map_key_type=None, map_value_type=None)`。创建 map：`factory.new_keyed_map(primary_key, map_name)`（map_name 必填，转为 namespace），得到 `KeyedMapState[MK, MV]`。状态方法：`put(map_key, value)`、`get(map_key)`（返回 `(value, found)`）、`delete(map_key)`、`clear()`（按前缀删除本 map 全部条目）、`all()`（迭代 `(map_key, value)`）。

### 4.7 KeyedPriorityQueueState

KeyedPriorityQueueState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace），元素 codec 必须有序。工厂：`KeyedPriorityQueueStateFactory.from_context(ctx, store_name, key_group, item_codec)` 或 `from_context_auto_codec(ctx, store_name, key_group, item_type=None)`。创建队列：`factory.new_keyed_priority_queue(primary_key, namespace)`（primary_key 与 namespace 均必填，bytes），得到 `KeyedPriorityQueueState[V]`。状态方法：`add(value)`、`peek()`（返回 `(min_element, found)`）、`poll()`（取出并返回最小元素）、`clear()`（按前缀删除全部）、`all()`（按序迭代所有元素）。

### 4.8 KeyedReducingState

KeyedReducingState 与 Go SDK 一致：工厂仅需 `key_group`（无 namespace）。工厂：`KeyedReducingStateFactory.from_context(ctx, store_name, key_group, value_codec, reduce_func)` 或 `from_context_auto_codec(ctx, store_name, key_group, reduce_func, value_type=None)`。创建状态：`factory.new_reducing_state(primary_key, namespace)`（两者必填，bytes），得到 `KeyedReducingState[V]`。状态方法：`add(value)`（与当前值经 reduce_func 合并后写入）、`get()`（返回 `(value, found)`）、`clear()`。

---

## 5. 示例

### 5.1 ValueState（from_context_auto_codec）

从 **fs_api_advanced** 导入 ValueState（Codec、ListState、MapState 等同此包）：

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

### 5.2 KeyedValueState（keyed 算子）

流按 key 分区时，在 `init` 中创建工厂，在 `process` 中按当前记录的 `primary_key` 取状态，再 `update(value)` / `value()` / `clear()`：

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

其他状态类型按上表使用 `XxxState.from_context(ctx, store_name, ...)` 或 `XxxState.from_context_auto_codec(ctx, store_name)`。

---

## 6. 参见

- [Python SDK 指南](python-sdk-guide-zh.md) — fs_api、fs_client 及 Context/KvStore 基础用法。
