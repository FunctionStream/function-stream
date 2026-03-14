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

# Go SDK — 高级状态 API

本文档介绍 Function Stream Go SDK 的**带类型高级状态 API**：在底层 `Store` 之上提供的状态抽象（ValueState、ListState、MapState、PriorityQueueState、AggregatingState、ReducingState 及 Keyed* 工厂），通过 **codec** 序列化，并支持按主键的 **keyed state**。当需要结构化状态而不想手写字节编码或 key 布局时使用。

**文档结构：**

- [何时使用哪种 API](#1-何时使用哪种-api) — 按使用场景选择状态类型。
- [包与导入](#2-包与导入) — 类型与 codec 所在包。
- [Codec 约定](#3-codec-约定与默认-codec) — 编码、解码及有序性要求。
- [创建状态](#4-创建状态带-codec-与-autocodec) — 显式 codec 与 AutoCodec 构造方式。
- [非 Keyed 状态参考](#5-非-keyed-状态structures) — 方法与构造方法一览。
- [AggregateFunc 与 ReduceFunc](#6-aggregatefunc-与-reducefunc) — 聚合与归约接口。
- [Keyed 状态](#7-keyed-状态工厂与按-key-实例) — keyGroup、primaryKey、namespace 与工厂方法。
- [错误处理与最佳实践](#8-错误处理与最佳实践) — 生产环境建议。
- [示例](#9-示例) — ValueState、Keyed list、MapState、AggregatingState。

---

## 1. 何时使用哪种 API

高级状态 API 对单个逻辑 store 提供带类型视图。根据访问模式选择对应抽象：

| 使用场景                    | 推荐 API                        | 说明                                           |
|-------------------------|-------------------------------|----------------------------------------------|
| 单一逻辑值（计数、配置块、最新值）       | **ValueState[T]**             | 每个 store 一个值；更新即覆盖。                          |
| 仅追加序列（事件日志、历史）          | **ListState[T]**              | 批量添加、整体读/替换；无 key 迭代。                        |
| 需范围/迭代的键值映射             | **MapState[K,V]**             | 键类型**必须**有有序 codec（如基本类型）。                   |
| 优先队列（最小/最大、Top-K）       | **PriorityQueueState[T]**     | 元素类型**必须**有有序 codec。                         |
| 运行中聚合（sum、count、自定义累加器） | **AggregatingState[T,ACC,R]** | 使用 `AggregateFunc`；累加器可合并。                   |
| 运行中归约（二元合并）             | **ReducingState[V]**          | 使用 `ReduceFunc`；满足结合律的合并。                    |
| **按 key** 的状态（按用户、按分区）  | **Keyed*** 工厂                 | 用于 **keyed 算子**；工厂 + 每条记录的 primaryKey。       |
| 自定义 key 布局、批量扫描、非类型化存储  | 底层 **Store**                  | `Put`/`Get`/`ScanComplex`/`ComplexKey`；完全自控。 |

**Keyed 与非 Keyed**

- **Keyed 状态**用于 **keyed 算子**：流按 key 分区（如 keyBy 之后）。运行时按 key 投递记录；每个 key 应有独立状态。可**一次**获取**工厂**（从 context、store 名称、keyGroup），再按**主键**（流 key）与 namespace 构造对应状态类型。
- **非 Keyed 状态**（ValueState、ListState 等）每个 store 存一个逻辑实体。在无 key 分区或维护单一全局状态时使用。

---

## 2. 包与导入

**两个独立库：** 低阶 **go-sdk**，高阶 **go-sdk-advanced**（依赖 go-sdk）。

| 包                     | 导入路径                                                                      | 职责                                                                                            |
|------------------------|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| **codec**（高阶）      | `github.com/functionstream/function-stream/go-sdk-advanced/codec`          | `Codec[T]` 接口及内置 codec。                                                                       |
| **structures**（高阶） | `github.com/functionstream/function-stream/go-sdk-advanced/structures`     | ValueState、ListState、MapState、PriorityQueueState、AggregatingState、ReducingState。           |
| **keyed**（高阶）      | `github.com/functionstream/function-stream/go-sdk-advanced/keyed`         | Keyed 状态工厂及按 key 的类型（KeyedListStateFactory、KeyedListState 等）。在 keyed 算子中使用。           |

所有状态构造方法均接收 `api.Context`（即 `fssdk.Context`）和 **store 名称**。Store 内部通过 `ctx.GetOrCreateStore(storeName)` 获取。同一 store 名称始终对应同一底层 store（默认实现为 RocksDB）。

---

## 3. Codec 约定与默认 Codec

### 3.1 Codec 接口

`codec.Codec[T]`：

| 方法                                | 说明                                                                                  |
|-----------------------------------|-------------------------------------------------------------------------------------|
| `Encode(value T) ([]byte, error)` | 将值序列化为字节。                                                                           |
| `Decode(data []byte) (T, error)`  | 从字节反序列化。                                                                            |
| `EncodedSize() int`               | 固定大小时返回 `> 0`；变长时为 `<= 0`（用于 list 优化）。                                              |
| `IsOrderedKeyCodec() bool`        | 为 `true` 时，字节编码**全序**：字节字典序与值的顺序一致。**MapState 的 key 与 PriorityQueueState 的元素必须满足**。 |

### 3.2 DefaultCodecFor[T]()

`codec.DefaultCodecFor[T]()` 返回类型 `T` 的默认 codec：

- **基本类型**（`int32`、`int64`、`uint32`、`uint64`、`float32`、`float64`、`string`、`bool`、`int`、`uint` 等）：内置 codec；作为 map key 或 PQ 元素使用时**有序**。
- **结构体、map、slice、数组**：`JSONCodec[T]` — JSON 编码；**无序**（`IsOrderedKeyCodec() == false`）。**不要**在 MapState key 或 PriorityQueueState 元素类型上使用 AutoCodec（依赖有序性的操作可能失败或 panic）。
- **无约束的接口类型**：返回错误；类型参数须为具体类型。

### 3.3 有序性要求

**MapState[K,V]** 与 **PriorityQueueState[T]** 的 key（或元素）类型必须使用 `IsOrderedKeyCodec() == true` 的 codec。对 Map 或 PQ 使用 **AutoCodec** 构造时，请使用基本类型的 key/元素（如 `int64`、`string`），或提供显式有序 codec。

---

## 4. 创建状态：带 Codec 与 AutoCodec

两种构造方式：

1. **显式 codec** — `NewXxxFromContext(ctx, storeName, codec, ...)`  
   由你提供 `Codec[T]`（Map 需 key + value codec；Aggregating/Reducing 需 acc/value codec 及函数）。可完全控制编码与有序性。

2. **AutoCodec** — `NewXxxFromContextAutoCodec(ctx, storeName)` 或 `(ctx, storeName, aggFunc/reduceFunc)`  
   SDK 使用 `codec.DefaultCodecFor[T]()` 作为 value/累加器类型。Map 与 PQ 的 key/元素类型须有**有序**默认 codec；否则创建或操作可能返回 `ErrStoreInternal`。

状态实例是**轻量**的。可在每次调用（如 `Process` 内）创建，或在 Driver 中（如 `Init`）缓存。同一 store 名称始终对应同一底层 store；仅类型视图不同。

---

## 5. 非 Keyed 状态（structures）

### 5.1 语义与方法

| 状态                            | 语义                  | 主要方法                                                                                                                   | 有序 codec？       |
|-------------------------------|---------------------|------------------------------------------------------------------------------------------------------------------------|-----------------|
| **ValueState[T]**             | 单一可替换值。             | `Update(value T) error`；`Value() (T, bool, error)`；`Clear() error`                                                     | 否               |
| **ListState[T]**              | 仅追加列表；批量添加与整体替换。    | `Add(value T) error`；`AddAll(values []T) error`；`Get() ([]T, error)`；`Update(values []T) error`；`Clear() error`        | 否               |
| **MapState[K,V]**             | 键值映射；通过 `All()` 迭代。 | `Put(key K, value V) error`；`Get(key K) (V, bool, error)`；`Delete(key K) error`；`Clear() error`；`All() iter.Seq2[K,V]` | **Key K：是**     |
| **PriorityQueueState[T]**     | 优先队列（按编码顺序最小优先）。    | `Add(value T) error`；`Peek() (T, bool, error)`；`Poll() (T, bool, error)`；`Clear() error`；`All() iter.Seq[T]`           | **元素 T：是**      |
| **AggregatingState[T,ACC,R]** | 可合并累加器的运行中聚合。       | `Add(value T) error`；`Get() (R, bool, error)`；`Clear() error`                                                          | 否（ACC codec 任意） |
| **ReducingState[V]**          | 二元合并的运行中归约。         | `Add(value V) error`；`Get() (V, bool, error)`；`Clear() error`                                                          | 否               |

### 5.2 构造方法一览（非 Keyed）

| 状态                        | 带 codec                                                                                                                           | AutoCodec                                                             |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| ValueState[T]             | `NewValueStateFromContext(ctx, storeName, valueCodec)`                                                                            | `NewValueStateFromContextAutoCodec[T](ctx, storeName)`                |
| ListState[T]              | `NewListStateFromContext(ctx, storeName, itemCodec)`                                                                              | `NewListStateFromContextAutoCodec[T](ctx, storeName)`                 |
| MapState[K,V]             | `NewMapStateFromContext(ctx, storeName, keyCodec, valueCodec)` 或 `NewMapStateAutoKeyCodecFromContext(ctx, storeName, valueCodec)` | `NewMapStateFromContextAutoCodec[K,V](ctx, storeName)`                |
| PriorityQueueState[T]     | `NewPriorityQueueStateFromContext(ctx, storeName, itemCodec)`                                                                     | `NewPriorityQueueStateFromContextAutoCodec[T](ctx, storeName)`        |
| AggregatingState[T,ACC,R] | `NewAggregatingStateFromContext(ctx, storeName, accCodec, aggFunc)`                                                               | `NewAggregatingStateFromContextAutoCodec(ctx, storeName, aggFunc)`    |
| ReducingState[V]          | `NewReducingStateFromContext(ctx, storeName, valueCodec, reduceFunc)`                                                             | `NewReducingStateFromContextAutoCodec[V](ctx, storeName, reduceFunc)` |

---

## 6. AggregateFunc 与 ReduceFunc

### 6.1 AggregateFunc[T, ACC, R]

**AggregatingState** 需要实现 **AggregateFunc[T, ACC, R]**（位于包 `structures`）：

| 方法                                  | 说明                            |
|-------------------------------------|-------------------------------|
| `CreateAccumulator() ACC`           | 空状态时的初始累加器。                   |
| `Add(value T, accumulator ACC) ACC` | 将一条输入折叠进累加器。                  |
| `GetResult(accumulator ACC) R`      | 从累加器得到最终结果。                   |
| `Merge(a, b ACC) ACC`               | 合并两个累加器（如分布式或 checkpoint 合并）。 |

### 6.2 ReduceFunc[V]

**ReducingState** 需要 **ReduceFunc[V]**（函数类型）：`func(value1, value2 V) (V, error)`。须满足**结合律**（最好满足交换律），使多次应用得到确定的归约结果。

---

## 7. Keyed 状态 — 工厂与按 Key 实例

Keyed 状态用于 **keyed 算子**：流按 key 分区（如 keyBy）时，每个 key 在独立状态上处理。可**一次**获取**工厂**（从 context、store 名称与 **keyGroup**），再按**主键**（当前记录的流 key）与 namespace 构造对应状态类型。

状态按 **keyGroup**（[]byte）和 **主键**（primaryKey，[]byte）组织。由 context、store 名称、keyGroup 创建工厂；再通过工厂方法按主键获取状态。

### 7.1 keyGroup、key（主键）与 namespace

Keyed API 对应 store 的 **ComplexKey**，有三个维度：

| 术语            | 出现位置                                                                                          | 含义                                                                                                |
|---------------|-----------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| **keyGroup**  | 创建工厂时的参数                                                                                      | **keyed 组**：标识该状态所属分区/组（如 `[]byte("counters")`、`[]byte("sessions")`）。同一 keyed 组 ⇒ 相同 keyGroup 字节。 |
| **key**       | 工厂方法中的 `primaryKey`（如 `NewKeyedList(primaryKey, namespace)` 等） | **流 key 的值**：分区流所用的 key，序列化为字节（如用户 ID、分区 key）。不同 primaryKey 对应不同状态。                               |
| **namespace** | 工厂方法中的 `namespace`（[]byte）                                                                    | **有窗口时**：**窗口标识的字节**（如序列化的窗口边界或窗口 ID），状态按 key 与窗口隔离。**无窗口时**：传**空字节**（`nil` 或 `[]byte{}`）。        |

**小结**：**keyGroup** = keyed 组标识；**key**（primaryKey）= 流 key 值；**namespace** = 使用窗口时为窗口字节，否则为空。

### 7.2 工厂构造方法一览（Keyed）

| 工厂                                    | 带 codec                                                                                     | AutoCodec                                                                                   |
|---------------------------------------|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| KeyedValueStateFactory[V]             | `NewKeyedValueStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                | `NewKeyedValueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`                |
| KeyedListStateFactory[V]              | `NewKeyedListStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                 | `NewKeyedListStateFactoryAutoCodecFromContext[V](ctx, storeName, keyGroup)`                 |
| KeyedMapStateFactory[MK,MV]           | `NewKeyedMapStateFactoryFromContext(ctx, storeName, keyGroup, keyCodec, valueCodec)`        | `NewKeyedMapStateFactoryFromContextAutoCodec[MK,MV](ctx, storeName, keyGroup)`              |
| KeyedPriorityQueueStateFactory[V]     | `NewKeyedPriorityQueueStateFactoryFromContext(ctx, storeName, keyGroup, itemCodec)`         | `NewKeyedPriorityQueueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`        |
| KeyedAggregatingStateFactory[T,ACC,R] | `NewKeyedAggregatingStateFactoryFromContext(ctx, storeName, keyGroup, accCodec, aggFunc)`   | `NewKeyedAggregatingStateFactoryFromContextAutoCodec(ctx, storeName, keyGroup, aggFunc)`    |
| KeyedReducingStateFactory[V]          | `NewKeyedReducingStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec, reduceFunc)` | `NewKeyedReducingStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup, reduceFunc)` |

### 7.3 从工厂获取按 Key 状态

| 工厂                                | 方法                                                                                                  | 返回                                      |
|-----------------------------------|-----------------------------------------------------------------------------------------------------|-----------------------------------------|
| KeyedValueStateFactory[V]         | `NewKeyedValue(primaryKey []byte, namespace []byte) (*KeyedValueState[V], error)`                   | 每个 (primaryKey, namespace) 一个 value 状态。 |
| KeyedListStateFactory[V]          | `NewKeyedList(primaryKey []byte, namespace []byte) (*KeyedListState[V], error)`                     | 每个 (primaryKey, namespace) 一个 list 状态。  |
| KeyedMapStateFactory[MK,MV]       | `NewKeyedMap(primaryKey []byte, mapName string) (*KeyedMapState[MK,MV], error)`                     | 每个 (primaryKey, mapName) 一个 map 状态。     |
| KeyedPriorityQueueStateFactory[V] | `NewKeyedPriorityQueue(primaryKey []byte, namespace []byte) (*KeyedPriorityQueueState[V], error)`   | 每个 (primaryKey, namespace) 一个 PQ 状态。    |
| KeyedAggregatingStateFactory      | `NewAggregatingState(primaryKey []byte, stateName string) (*KeyedAggregatingState[T,ACC,R], error)` | 每个 (primaryKey, stateName) 一个聚合状态。      |
| KeyedReducingStateFactory[V]      | `NewReducingState(primaryKey []byte, namespace []byte) (*KeyedReducingState[V], error)`             | 每个 (primaryKey, namespace) 一个归约状态。      |

此处 **primaryKey** 为流 key 值；**namespace** 在使用窗口函数时为窗口字节，否则为空。

**设计建议**：每个逻辑状态使用稳定的 keyGroup（如 `[]byte("orders")`）。在工厂方法中，将 keyed 算子收到的**流 key**（如从 key 提取器或消息元数据）作为 primaryKey 传入。

---

## 8. 错误处理与最佳实践

- **状态 API 错误**：创建与方法返回的错误与 `fssdk.SDKError` 兼容（如 `ErrStoreInternal`、`ErrStoreIO`）。Codec 编解码失败会被包装（如 `"encode value state failed"`）。生产环境务必检查并处理错误。
- **Store 命名**：每个逻辑状态使用稳定、唯一的 store 名称（如 `"counters"`、`"user-sessions"`）。同一运行时中同一名称对应同一 store。
- **状态缓存**：可在 `Init` 中创建一次状态实例并在 `Process` 中复用，也可每条消息创建。按消息创建是安全的，在不需要分摊创建成本时能保持代码简单。
- **KeyGroup 设计**：Keyed 状态中，每个“逻辑表”使用一致的 keyGroup。primaryKey 在 keyed 算子中为**流 key** — 使用标识当前记录的 key。使用**窗口函数**时，将窗口标识作为 **namespace** 传入，使状态按 key 与窗口隔离。
- **有序 codec**：MapState 与 PriorityQueueState 使用 AutoCodec 时，请用基本类型作为 key/元素。自定义结构体 key 需实现 `IsOrderedKeyCodec() == true` 的 `Codec[K]` 并使用“带 codec”的构造方法。

---

## 9. 示例

### 9.1 ValueState + AutoCodec（计数器）

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
    // emit 或继续...
    return nil
}
```

### 9.2 Keyed list 工厂（keyed 算子）

当算子在 **keyed 流**上运行时，使用 Keyed list 工厂，并为每条消息将**流 key** 作为 primaryKey 传入：

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
    userID := parseUserID(data)   // []byte — 当前记录的流 key
    list, err := p.listFactory.NewKeyedList(userID, []byte{})  // 无窗口时 namespace 为空
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
    // 使用 items...
    return nil
}
```

### 9.3 MapState 与 AggregatingState（求和）

使用 **go-sdk-advanced** 的 `structures` 包；`ctx` 来自低阶 go-sdk 的 `fssdk.Context`。

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk-advanced/structures"
)

// MapState: string -> int64（两者均有有序默认 codec）
m, err := structures.NewMapStateFromContextAutoCodec[string, int64](ctx, "counts")
if err != nil {
    return err
}
_ = m.Put("a", 1)
v, ok, _ := m.Get("a")

// AggregatingState: int64 求和（ACC = int64, R = int64）
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

## 10. 参见

- [Go SDK 指南](go-sdk-guide-zh.md) — 主文档：Driver、Context、Store、构建与部署。
- [Python SDK — 高级状态 API](../Python-SDK/python-sdk-advanced-state-api-zh.md) — Python SDK 的等价带类型状态 API。
- [examples/go-processor/README.md](../../examples/go-processor/README.md) — 示例算子与构建说明。
