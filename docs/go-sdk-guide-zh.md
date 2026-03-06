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

config.yaml 中需配置 `name`、`type: processor`、`input-groups`、`outputs`（如 Kafka）。详见 [Function 配置](function-configuration-zh.md) 与 [examples/go-processor/README.md](../examples/go-processor/README.md)。

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

## 七、目录结构参考

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
├── wit/              # processor.wit 及依赖（可由 make wit 生成）
└── bindings/         # wit-bindgen-go 生成的 Go 代码（make bindings）
```

更多示例与 SQL 操作见 [examples/go-processor/README.md](../examples/go-processor/README.md)、[SQL CLI 指南](sql-cli-guide-zh.md)。
