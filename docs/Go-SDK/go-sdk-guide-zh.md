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

# Go SDK 指南

Function Stream 为 Go 开发者提供基于 WebAssembly（WASI P2）的算子开发 SDK。使用 TinyGo 将 Go 代码编译为 WASM 组件，在服务端沙箱中运行，具备 KV 状态存储与数据发射能力。

---

## 一、SDK 核心组件

| 组件           | 定位     | 说明                                                           |
|--------------|--------|--------------------------------------------------------------|
| **fssdk**    | 主包     | 对外入口，提供 `Driver`、`Context`、`Store` 等类型及 `Run(driver)`。       |
| **api**      | 接口定义   | 定义 `Driver`、`Context`、`Store`、`Iterator`、`ComplexKey` 及错误码。  |
| **impl**     | 运行时实现  | 将 Driver 与 WASM 宿主（processor WIT）桥接，内部使用。                    |
| **bindings** | WIT 绑定 | 由 wit-bindgen-go 根据 `wit/processor.wit` 生成的 Go 绑定，供 impl 调用。 |

Go 算子**仅依赖 fssdk**：实现 `Driver`（或嵌入 `BaseDriver`），在 `init()` 中调用 `fssdk.Run(&YourProcessor{})` 即可。

---

## 二、Driver 接口与 BaseDriver

### 2.1 Driver 接口

所有 Go 算子必须实现 `fssdk.Driver` 接口。运行时在对应时机调用以下方法：

| 方法                                           | 触发时机        | 说明                                  |
|----------------------------------------------|-------------|-------------------------------------|
| `Init(ctx, config)`                          | 函数启动时执行一次   | 初始化状态、获取 Store、解析 config。           |
| `Process(ctx, sourceID, data)`               | 每收到一条消息时    | 核心处理逻辑：计算、状态读写、`ctx.Emit()`。        |
| `ProcessWatermark(ctx, sourceID, watermark)` | 收到水位线事件时    | 处理基于时间的窗口或乱序重排，可转发 `EmitWatermark`。 |
| `TakeCheckpoint(ctx, checkpointID)`          | 系统做状态备份时    | 可持久化额外内存状态，保证强一致性。                  |
| `CheckHeartbeat(ctx)`                        | 定期健康检查      | 返回 `false` 会触发算子重启。                 |
| `Close(ctx)`                                 | 函数关闭时       | 释放资源、清空引用。                          |
| `Exec(ctx, className, modules)`              | 扩展能力（可选）    | 动态加载模块等，默认可不实现。                     |
| `Custom(ctx, payload)`                       | 自定义 RPC（可选） | 请求/响应自定义字节，默认返回 payload 副本。         |

### 2.2 BaseDriver

`fssdk.BaseDriver` 为上述所有方法提供空实现。嵌入后只需重写你关心的方法：

```go
type MyProcessor struct {
    fssdk.BaseDriver
    store fssdk.Store
}

func (p *MyProcessor) Init(ctx fssdk.Context, config map[string]string) error {
    store, err := ctx.GetOrCreateStore("my-store")
    if err != nil {
        return err
    }
    p.store = store
    return nil
}

func (p *MyProcessor) Process(ctx fssdk.Context, sourceID uint32, data []byte) error {
    // 业务逻辑，读写 p.store，最后 ctx.Emit(...)
    return ctx.Emit(0, result)
}
```

### 2.3 注册入口

在 `init()` 中调用 `fssdk.Run`，传入你的 Driver 实例（通常为单例）：

```go
func init() {
    fssdk.Run(&MyProcessor{})
}
```

---

## 三、Context 与 Store

### 3.1 Context

`fssdk.Context` 在 Init / Process / ProcessWatermark 等回调中传入，提供：

| 方法                                                       | 说明                                    |
|----------------------------------------------------------|---------------------------------------|
| `Emit(targetID uint32, data []byte) error`               | 将数据发往指定输出通道。                          |
| `EmitWatermark(targetID uint32, watermark uint64) error` | 发射水位线。                                |
| `GetOrCreateStore(name string) (Store, error)`           | 按名称获取或创建 KV Store（基于 RocksDB）。        |
| `Config() map[string]string`                             | 获取启动时下发的配置（对应 config.yaml 中的 init 等）。 |
| `Close() error`                                          | 关闭 Context，一般由运行时管理。                  |

### 3.2 Store（KV 状态）

`fssdk.Store` 提供键值存储与复杂键能力：

**基础 API：**

- `PutState(key, value []byte) error`
- `GetState(key []byte) (value []byte, found bool, err error)`
- `DeleteState(key []byte) error`
- `ListStates(startInclusive, endExclusive []byte) ([][]byte, error)`：按字节序范围列出键。

**ComplexKey（多维键/前缀扫描）：**

- `Put(key ComplexKey, value []byte)` / `Get` / `Delete` / `Merge` / `DeletePrefix`
- `ListComplex(...)`：按 keyGroup、key、namespace 及范围列出 UserKey。
- `ScanComplex(keyGroup, key, namespace []byte) (Iterator, error)`：返回迭代器，适用大范围扫描。

`ComplexKey` 结构包含 `KeyGroup`、`Key`、`Namespace`、`UserKey`，用于多维索引与前缀查询。

### 3.3 Iterator

`Store.ScanComplex` 返回的 `fssdk.Iterator` 接口：

- `HasNext() (bool, error)`
- `Next() (key, value []byte, ok bool, err error)`
- `Close() error`（用毕须调用以释放资源）

---

## 四、生产级示例（词频统计）

```go
package main

import (
    "encoding/json"
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "strconv"
    "strings"
)

func init() {
    fssdk.Run(&CounterProcessor{})
}

type CounterProcessor struct {
    fssdk.BaseDriver
    store          fssdk.Store
    counterMap     map[string]int64
    totalProcessed int64
    keyPrefix      string
}

func (p *CounterProcessor) Init(ctx fssdk.Context, config map[string]string) error {
    store, err := ctx.GetOrCreateStore("counter-store")
    if err != nil {
        return err
    }
    p.store = store
    p.counterMap = make(map[string]int64)
    p.totalProcessed = 0
    p.keyPrefix = strings.TrimSpace(config["key_prefix"])
    return nil
}

func (p *CounterProcessor) Process(ctx fssdk.Context, sourceID uint32, data []byte) error {
    inputStr := strings.TrimSpace(string(data))
    if inputStr == "" {
        return nil
    }
    p.totalProcessed++

    fullKey := p.keyPrefix + inputStr
    existing, found, err := p.store.GetState([]byte(fullKey))
    if err != nil {
        return err
    }
    currentCount := int64(0)
    if found {
        if n, e := strconv.ParseInt(string(existing), 10, 64); e == nil {
            currentCount = n
        }
    }
    newCount := currentCount + 1
    p.counterMap[inputStr] = newCount
    if err = p.store.PutState([]byte(fullKey), []byte(strconv.FormatInt(newCount, 10))); err != nil {
        return err
    }

    out := map[string]interface{}{
        "total_processed": p.totalProcessed,
        "counter_map":     p.counterMap,
    }
    jsonBytes, _ := json.Marshal(out)
    return ctx.Emit(0, jsonBytes)
}

func (p *CounterProcessor) ProcessWatermark(ctx fssdk.Context, sourceID uint32, watermark uint64) error {
    return ctx.EmitWatermark(0, watermark)
}

func (p *CounterProcessor) Close(ctx fssdk.Context) error {
    p.store = nil
    p.counterMap = nil
    return nil
}

func main() {}
```

---

## 五、构建与部署

### 5.1 环境要求

- **Go**：1.23+
- **TinyGo**：0.40+（支持 WASI P2）
- **wit-bindgen-go**：`go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest`
- **wkg**（可选）：用于拉取 WIT 依赖，`cargo install wkg --version 0.10.0`
- **wasm-tools**：用于组件校验等，`cargo install wasm-tools`

### 5.2 生成绑定并构建

在项目根目录下：

```bash
# 安装 Go SDK 工具链（wit-bindgen-go、wkg 等）
make -C go-sdk env

# 生成 WIT 绑定（从 wit/processor.wit 及 deps）
make -C go-sdk bindings

# 运行测试
make -C go-sdk build
```

算子项目（如 `examples/go-processor`）中：

```bash
# 使用 TinyGo 编译为 WASI P2 组件
tinygo build -o build/processor.wasm -target=wasi .
```

具体参数见 `examples/go-processor/build.sh`。

### 5.3 注册与运行

将编译得到的 `processor.wasm` 与 `config.yaml` 通过 SQL CLI 注册为 function：

```sql
create function with (
  'function_path'='/path/to/build/processor.wasm',
  'config_path'='/path/to/config.yaml'
);
```

config.yaml 中需配置 `name`、`type: processor`、`input-groups`、`outputs`（如 Kafka）。详见 [Function 配置](../function-configuration-zh.md) 与 [examples/go-processor/README.md](../../examples/go-processor/README.md)。

---

## 六、错误码与异常处理

SDK 通过 `fssdk.SDKError` 返回错误，可用 `errors.As` 获取 `Code`：

| 错误码                        | 含义             |
|----------------------------|----------------|
| `ErrRuntimeInvalidDriver`  | 传入的 Driver 无效。 |
| `ErrRuntimeNotInitialized` | 运行时未初始化即调用。    |
| `ErrRuntimeClosed`         | 运行时已关闭。        |
| `ErrStoreInvalidName`      | Store 名称不合法。   |
| `ErrStoreInternal`         | Store 内部错误。    |
| `ErrStoreNotFound`         | 未找到指定 Store。   |
| `ErrStoreIO`               | Store 读写异常。    |
| `ErrResultUnexpected`      | 宿主返回了意外结果。     |

处理示例：

```go
if err != nil {
    var sdkErr *fssdk.SDKError
    if errors.As(err, &sdkErr) {
        switch sdkErr.Code {
        case fssdk.ErrStoreNotFound:
            // 按业务决定是否创建或返回
        default:
            // 记录并向上返回
        }
    }
    return err
}
```

---

## 七、高级状态 API

本节说明**高级状态 API**：在底层 `Store` 之上构建的**类型化状态抽象**，通过 **Codec** 序列化，并支持按主键的 **Keyed 状态**。适用于需要结构化状态（单值、列表、Map、优先队列、聚合、归约）且不希望手写字节编码与键布局的场景。完整说明见 [Go SDK — 高级状态 API](go-sdk-advanced-state-api-zh.md)。

### 7.1 何时使用哪种 API

| 使用场景                       | 推荐 API                                                                                                                     |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------|
| 单一逻辑值（如计数器、配置 blob）        | **ValueState[T]** 或单键的裸 `Store`。                                                                                           |
| 仅追加序列（如事件日志）               | **ListState[T]** 或自定义编码的 `Store.Merge`。                                                                                    |
| 需按范围迭代的键值 Map              | **MapState[K,V]**（键类型须为有序 Codec）。                                                                                          |
| 优先队列（最小/最大、Top-K）          | **PriorityQueueState[T]**（元素 Codec 须有序）。                                                                                   |
| 运行中聚合（sum、count、自定义累加器）    | 带 `AggregateFunc` 的 **AggregatingState[T,ACC,R]**。                                                                         |
| 运行中归约（二元合并）                | 带 `ReduceFunc` 的 **ReducingState[V]**。                                                                                     |
| **按 key 维度的状态**（如按用户、按分区键） | **Keyed*** 工厂：先获取工厂，再按 key 调用 `NewKeyedValue(primaryKey, ...)` 等。**供 Keyed 算子使用** — 当流按 key 分区（如经 keyBy 后）时，每个 key 拥有独立状态。 |
| 自定义键布局、批量扫描或非类型化存储         | 底层 **Store**（`Put`/`Get`/`ScanComplex`/`ComplexKey`）。                                                                      |

**Keyed 与非 Keyed：** **Keyed 状态面向 Keyed 算子**：即流已按 key 分区（例如在 DAG 中经 keyBy 后）。此时运行时按 key 投递记录，每个 key 应有独立状态 — 通过 **工厂** 和 **keyGroup** 创建状态，再按 **主键**（即流上的 key）调用 `factory.NewKeyedValue(primaryKey, stateName)` 等。非 Keyed 状态（如 `ValueState`、`ListState`）在每个 store 下存一份逻辑实体，用于无 key 分区或单一全局状态的场景。

### 7.2 包与模块路径

| 包名             | 导入路径                                                                | 职责                                                                                                                  |
|----------------|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| **structures** | `github.com/functionstream/function-stream/go-sdk/state/structures` | 非 Keyed 状态类型：`ValueState`、`ListState`、`MapState`、`PriorityQueueState`、`AggregatingState`、`ReducingState`。           |
| **keyed**      | `github.com/functionstream/function-stream/go-sdk/state/keyed`      | Keyed 状态**工厂**及其按 key 的状态类型（如 `KeyedListStateFactory`、`KeyedListState`）。**供 Keyed 算子使用** — 当流按 key 分区时，按流 key 创建状态。 |
| **codec**      | `github.com/functionstream/function-stream/go-sdk/state/codec`      | `Codec[T]` 接口、`DefaultCodecFor[T]()` 及内置 Codec（基本类型、`JSONCodec`）。                                                   |

所有状态构造函数均接收 `api.Context`（即 `fssdk.Context`）和 **store 名称**；内部通过 `ctx.GetOrCreateStore(storeName)` 获取 Store。同一 store 名称始终对应同一底层存储（默认实现为 RocksDB）。

### 7.3 Codec 约定与默认 Codec

**Codec 接口**（`codec.Codec[T]`）：

- `Encode(value T) ([]byte, error)` — 序列化为字节。
- `Decode(data []byte) (T, error)` — 从字节反序列化。
- `EncodedSize() int` — 固定长度时 `> 0`，变长时 `<= 0`（用于 List 优化）。
- `IsOrderedKeyCodec() bool` — 为 `true` 时表示字节编码**全序**（字节字典序与值的确定顺序一致）。**MapState** 的键与 **PriorityQueueState** 的元素必须使用有序 Codec。

**DefaultCodecFor[T]()** 按类型返回默认 Codec：

- **基本类型**（`int32`、`int64`、`uint32`、`uint64`、`float32`、`float64`、`string`、`bool`、`int`、`uint` 等）：内置定长或变长 Codec；用作 Map 键或 PQ 元素时**有序**。
- **结构体、map、slice、数组**：使用 `JSONCodec[T]`（JSON 编码），**非有序**（`IsOrderedKeyCodec() == false`）。**不要**将其作为 MapState 键或 PriorityQueueState 元素类型与 AutoCodec 一起使用（创建可能成功，但依赖顺序的操作可能失败或 panic）。
- **无类型约束的 interface**：返回错误（类型参数须为具体类型）。

**有序性要求：** `MapState[K,V]` 与 `PriorityQueueState[T]` 的**键**（或**元素**）类型必须使用 `IsOrderedKeyCodec() == true` 的 Codec。使用 Map/PQ 的 **AutoCodec** 构造函数时，请选择基本类型键/元素（如 `int64`、`string`），或显式提供有序 Codec。

### 7.4 创建状态：带 Codec 与 AutoCodec

两类构造函数：

1. **显式 Codec** — `NewXxxFromContext(ctx, storeName, codec, ...)`  
   由调用方传入 `Codec[T]`（Map 需 key+value codec；Aggregating/Reducing 需 acc/value codec 与函数）。可完全控制编码与有序性。

2. **AutoCodec** — `NewXxxFromContextAutoCodec(ctx, storeName)` 或 `(ctx, storeName, aggFunc/reduceFunc)`  
   SDK 对值/累加器类型调用 `codec.DefaultCodecFor[T]()`。Map 与 PQ 的 key/元素类型须有**有序**默认 Codec（基本类型），否则运行时检查可能返回 `ErrStoreInternal`。

状态实例是**轻量**的；可在每次调用时创建（如在 `Process` 内调用 `NewValueStateFromContextAutoCodec[int64](ctx, "store")`），也可在 Driver 中缓存（如在 `Init` 中）。同一 store 名称对应同一底层 store，仅类型化视图不同。

### 7.5 非 Keyed 状态（structures）— 参考

| 状态                            | 语义                        | 方法（签名概要）                                                                                                               | 是否要求有序 Codec |
|-------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------|--------------|
| **ValueState[T]**             | 单一可替换值。                   | `Update(value T) error`；`Value() (T, bool, error)`；`Clear() error`                                                     | 否            |
| **ListState[T]**              | 仅追加列表；支持批量追加与整体替换。        | `Add(value T) error`；`AddAll(values []T) error`；`Get() ([]T, error)`；`Update(values []T) error`；`Clear() error`        | 否            |
| **MapState[K,V]**             | 键值 Map；通过 `All()` 按键范围迭代。 | `Put(key K, value V) error`；`Get(key K) (V, bool, error)`；`Delete(key K) error`；`Clear() error`；`All() iter.Seq2[K,V]` | **键 K：是**    |
| **PriorityQueueState[T]**     | 优先队列（按编码键顺序最小优先）。         | `Add(value T) error`；`Peek() (T, bool, error)`；`Poll() (T, bool, error)`；`Clear() error`；`All() iter.Seq[T]`           | **元素 T：是**   |
| **AggregatingState[T,ACC,R]** | 可合并累加器的运行中聚合。             | `Add(value T) error`；`Get() (R, bool, error)`；`Clear() error`                                                          | 否（ACC 任意）    |
| **ReducingState[V]**          | 二元合并的运行中归约。               | `Add(value V) error`；`Get() (V, bool, error)`；`Clear() error`                                                          | 否            |

**构造函数概要（非 Keyed）：**

| 状态                        | 带 Codec                                                                                                                           | AutoCodec                                                             |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| ValueState[T]             | `NewValueStateFromContext(ctx, storeName, valueCodec)`                                                                            | `NewValueStateFromContextAutoCodec[T](ctx, storeName)`                |
| ListState[T]              | `NewListStateFromContext(ctx, storeName, itemCodec)`                                                                              | `NewListStateFromContextAutoCodec[T](ctx, storeName)`                 |
| MapState[K,V]             | `NewMapStateFromContext(ctx, storeName, keyCodec, valueCodec)` 或 `NewMapStateAutoKeyCodecFromContext(ctx, storeName, valueCodec)` | `NewMapStateFromContextAutoCodec[K,V](ctx, storeName)`                |
| PriorityQueueState[T]     | `NewPriorityQueueStateFromContext(ctx, storeName, itemCodec)`                                                                     | `NewPriorityQueueStateFromContextAutoCodec[T](ctx, storeName)`        |
| AggregatingState[T,ACC,R] | `NewAggregatingStateFromContext(ctx, storeName, accCodec, aggFunc)`                                                               | `NewAggregatingStateFromContextAutoCodec(ctx, storeName, aggFunc)`    |
| ReducingState[V]          | `NewReducingStateFromContext(ctx, storeName, valueCodec, reduceFunc)`                                                             | `NewReducingStateFromContextAutoCodec[V](ctx, storeName, reduceFunc)` |

### 7.6 AggregateFunc 与 ReduceFunc

**AggregatingState** 需要实现 **AggregateFunc[T, ACC, R]**（定义在 `structures`）：

- `CreateAccumulator() ACC` — 空状态下的初始累加器。
- `Add(value T, accumulator ACC) ACC` — 将一条输入折叠进累加器。
- `GetResult(accumulator ACC) R` — 从累加器得到最终结果。
- `Merge(a, b ACC) ACC` — 合并两个累加器（用于分布式/ checkpoint 合并）。

**ReducingState** 需要 **ReduceFunc[V]**（函数类型）：`func(value1, value2 V) (V, error)`。须满足结合律（最好可交换），以便多次应用得到确定的归约结果。

### 7.7 Keyed 状态 — 工厂与按 key 的实例

**Keyed 状态专为 Keyed 算子设计。** 当流已按 key 分区（例如在流水线中经 keyBy 后）时，每个 key 在隔离的状态下被处理。Keyed 状态 API 让你先获取一次**工厂**（通过 context、store 名称和 **keyGroup**），再按**主键**创建状态 — 主键即当前记录所在的流 key（如用户 ID、分区键）。在算子运行于 keyed 流上时使用 Keyed* 工厂；流未按 key 分区或需要单一全局状态时使用非 Keyed 状态。

Keyed 状态由 **keyGroup**（[]byte）和 **primary key**（[]byte）组织。先由 context、store 名称和 keyGroup 创建**工厂**，再通过工厂方法获取某主键下的状态。

#### 7.7.1 keyGroup、key（primaryKey）与 namespace

Keyed 状态 API 对应底层 Store 的 **ComplexKey** 三个维度，含义如下：

| 概念            | API 参数                                                                                        | 含义                                                                                                                                                    |
|---------------|-----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| **keyGroup**  | 创建工厂时的 `keyGroup`                                                                             | **Keyed 的组**：标识该状态属于哪一个 keyed 分区/组。每个逻辑上的“keyed 组”或状态种类对应一个 keyGroup（例如一组用于 “counters”，另一组用于 “sessions”）。同一 keyed 组使用相同 keyGroup 字节；不同组使用不同 keyGroup。 |
| **key**       | 工厂方法中的 `primaryKey`（如 `NewKeyedValue(primaryKey, ...)`、`NewKeyedList(primaryKey, namespace)`） | **对应 Key 的值**：当前记录在流上的 key，以字节形式序列化。即对流进行分区时使用的 key（如用户 ID、分区键）。每个不同的 key 值拥有独立状态。                                                                    |
| **namespace** | 工厂方法中的 `namespace`（[]byte）                                                                    | **若存在窗口函数**，则传入**该窗口对应的 bytes**（例如序列化后的窗口边界或窗口 ID），使状态按 key *且* 按窗口隔离。**无窗口时**传入**空 bytes**（如 `nil` 或 `[]byte{}`）。                                    |

**小结：** **keyGroup** = keyed 的组；**key**（primaryKey）= Key 的值（流 key）；**namespace** = 有窗口函数时为窗口的 bytes，否则为**空 bytes**。

**工厂构造函数概要（Keyed）：**

| 工厂                                    | 带 Codec                                                                                     | AutoCodec                                                                                   |
|---------------------------------------|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| KeyedValueStateFactory[V]             | `NewKeyedValueStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                | `NewKeyedValueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`                |
| KeyedListStateFactory[V]              | `NewKeyedListStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec)`                 | `NewKeyedListStateFactoryAutoCodecFromContext[V](ctx, storeName, keyGroup)`                 |
| KeyedMapStateFactory[MK,MV]           | `NewKeyedMapStateFactoryFromContext(ctx, storeName, keyGroup, keyCodec, valueCodec)`        | `NewKeyedMapStateFactoryFromContextAutoCodec[MK,MV](ctx, storeName, keyGroup)`              |
| KeyedPriorityQueueStateFactory[V]     | `NewKeyedPriorityQueueStateFactoryFromContext(ctx, storeName, keyGroup, itemCodec)`         | `NewKeyedPriorityQueueStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup)`        |
| KeyedAggregatingStateFactory[T,ACC,R] | `NewKeyedAggregatingStateFactoryFromContext(ctx, storeName, keyGroup, accCodec, aggFunc)`   | `NewKeyedAggregatingStateFactoryFromContextAutoCodec(ctx, storeName, keyGroup, aggFunc)`    |
| KeyedReducingStateFactory[V]          | `NewKeyedReducingStateFactoryFromContext(ctx, storeName, keyGroup, valueCodec, reduceFunc)` | `NewKeyedReducingStateFactoryFromContextAutoCodec[V](ctx, storeName, keyGroup, reduceFunc)` |

**从工厂获取按 key 的状态：**

- **KeyedValueStateFactory[V]**：`NewKeyedValue(primaryKey []byte, stateName string) (*KeyedValueState[V], error)` — 每个 (primaryKey, stateName) 对应一个 value 状态。
- **KeyedListStateFactory[V]**：`NewKeyedList(primaryKey []byte, namespace []byte) (*KeyedListState[V], error)`。
- **KeyedMapStateFactory[MK,MV]**：`NewKeyedMap(primaryKey []byte, mapName string) (*KeyedMapState[MK,MV], error)`。
- **KeyedPriorityQueueStateFactory[V]**：`NewKeyedPriorityQueue(primaryKey []byte, namespace []byte) (*KeyedPriorityQueueState[V], error)`。
- **KeyedAggregatingStateFactory**：`NewAggregatingState(primaryKey []byte, stateName string) (*KeyedAggregatingState[T,ACC,R], error)`。
- **KeyedReducingStateFactory[V]**：`NewReducingState(primaryKey []byte, namespace []byte) (*KeyedReducingState[V], error)`。

其中 **primaryKey** 即 key（流 key 的值）；**namespace** 在有窗口函数时为窗口的 bytes，无窗口时为**空 bytes**。

**keyGroup** 用于划分键空间（例如按逻辑状态名或功能维度）。同一逻辑状态应保持 keyGroup 稳定；不同状态实体可共用同一 store 但使用不同 keyGroup。工厂方法中的 **primaryKey** 即 keyed 流中当前记录的 key — 传入你的 Keyed 算子收到的 key（如从 key 提取器或消息元数据）。

### 7.8 错误处理与最佳实践

- **状态 API 返回的错误**：创建与方法返回的错误与 `fssdk.SDKError` 兼容（如 `ErrStoreInternal`、`ErrStoreIO`）。Codec 编解码错误会被包装（如 `"encode value state failed"`）。生产环境应始终检查并处理错误。
- **Store 命名**：按逻辑状态使用稳定、唯一的 store 名称（如 `"counters"`、`"user-sessions"`）。同一运行时内相同名称指向同一 store。
- **状态缓存**：可在 `Init` 中创建一次状态实例并在 `Process` 中复用，也可每条消息创建。按消息创建是安全的，在不需要分摊创建成本时可使代码更简单。
- **KeyGroup 设计**：Keyed 状态下，每个“逻辑表”使用一致的 keyGroup（如 `[]byte("orders")`）。primaryKey 在 Keyed 算子中即**流 key** — 使用标识当前记录的那个 key（如来自 key 提取器或消息 key）。**使用窗口函数时**，将窗口标识作为 **namespace**（如序列化后的窗口边界）传入，使状态按 key 且按窗口隔离。
- **有序 Codec**：MapState 与 PriorityQueueState 使用 AutoCodec 时，键/元素使用基本类型。自定义结构体键时，实现 `IsOrderedKeyCodec() == true` 的 `Codec[K]` 并改用“带 codec”的构造函数。

### 7.9 示例：带 AutoCodec 的 ValueState（计数器）

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk/state/structures"
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
    // ...
}
```

### 7.10 示例：Keyed list 工厂（Keyed 算子）

当算子运行在**按 key 分区的流**上时，使用 Keyed list 工厂，并为每条消息传入**流 key** 作为 primaryKey：

```go
import (
    fssdk "github.com/functionstream/function-stream/go-sdk"
    "github.com/functionstream/function-stream/go-sdk/state/keyed"
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
    userID := parseUserID(data)   // []byte
    list, err := p.listFactory.NewKeyedList(userID, "items")
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
}
```

### 7.11 示例：MapState 与 AggregatingState（求和）

```go
// MapState：string -> int64 计数（二者均有有序默认 codec）
m, err := structures.NewMapStateFromContextAutoCodec[string, int64](ctx, "counts")
if err != nil { return err }
_ = m.Put("a", 1)
v, ok, _ := m.Get("a")

// AggregatingState：int64 求和，AutoCodec（ACC = int64, R = int64）
type sumAgg struct{}
func (sumAgg) CreateAccumulator() int64 { return 0 }
func (sumAgg) Add(v int64, acc int64) int64 { return acc + v }
func (sumAgg) GetResult(acc int64) int64 { return acc }
func (sumAgg) Merge(a, b int64) int64 { return a + b }
agg, err := structures.NewAggregatingStateFromContextAutoCodec[int64, int64, int64](ctx, "sum-store", sumAgg{})
if err != nil { return err }
_ = agg.Add(10)
total, _, _ := agg.Get()
```

---

## 八、目录结构参考

```text
go-sdk/
├── Makefile          # env / wit / bindings / build
├── fssdk.go          # 对外入口与类型重导出
├── go.mod / go.sum
├── api/              # 接口与错误码
│   ├── driver.go     # Driver、BaseDriver
│   ├── context.go    # Context
│   ├── store.go      # Store、Iterator、ComplexKey
│   └── errors.go     # ErrorCode、SDKError
├── impl/             # 与 WASM 宿主桥接
│   ├── runtime.go
│   ├── context.go
│   └── store.go
├── state/            # 高级状态 API
│   ├── structures/   # ValueState、ListState、MapState、PriorityQueue、Aggregating、Reducing
│   ├── keyed/        # Keyed 状态工厂（value、list、map、PQ、aggregating、reducing）
│   ├── codec/        # Codec[T]、DefaultCodecFor、内置与 JSON codec
│   └── common/       # 公共辅助（如 DupBytes）
├── wit/              # processor.wit 及依赖（可由 make wit 生成）
└── bindings/         # wit-bindgen-go 生成的 Go 代码（make bindings）
```

更多示例与 SQL 操作见 [examples/go-processor/README.md](../../examples/go-processor/README.md)、[SQL CLI 指南](../sql-cli-guide-zh.md)。
