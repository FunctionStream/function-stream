# Go WASM Processor 示例

这是一个使用 Go 语言编写的 WASM Component 处理器示例，展示了如何使用 `wit-bindgen-go` 生成 Component Model 绑定。

## 目录结构

```
go-processor/
├── main.go                  # Go 源代码
├── go.mod                    # Go 模块定义
├── config.yaml               # 任务配置文件
├── generate-bindings.sh      # 生成 WIT 绑定脚本
├── build.sh                  # 构建脚本
├── bindings/                 # 生成的绑定代码（运行 generate-bindings.sh 后生成）
└── README.md                 # 本文件
```

## 前置要求

1. **Go 1.21+**: 用于编写代码和安装工具
   - 安装方法: https://go.dev/doc/install
   - 验证: `go version`

2. **TinyGo**: 用于编译 Go 代码为 WASM
   - 安装方法: https://tinygo.org/getting-started/install/
   - 验证: `tinygo version`

3. **wit-bindgen-go**: 用于生成 Component Model 绑定
   - 安装: `go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest`
   - 验证: `wit-bindgen-go --version`

4. **wasm-tools** (可选): 用于将普通 WASM 转换为 Component
   - 安装: `cargo install wasm-tools`
   - 或从: https://github.com/bytecodealliance/wasm-tools/releases

## 构建步骤

### 1. 生成 WIT 绑定代码

首先运行绑定生成脚本：

```bash
chmod +x generate-bindings.sh
./generate-bindings.sh
```

这将会：
- 检查并安装 `wit-bindgen-go`（如果未安装）
- 从 `wit/processor.wit` 生成 Go 绑定代码
- 将绑定代码输出到 `bindings/` 目录

**注意**: `bindings/` 目录中的多个文件是**中间产物**（因为 WIT 定义了多个接口：kv, collector, processor），最终编译后只会生成**一个 WASM 文件**。

### 2. 编译 WASM Component

运行构建脚本：

```bash
chmod +x build.sh
./build.sh
```

这将会：
- 生成 WIT 绑定代码（如果尚未生成）→ 多个 Go 文件（中间产物）
- 编译 Go 代码为 WASM → **一个 WASM 文件**（最终产物）
- 尝试转换为 Component Model 格式（如果 `wasm-tools` 可用）
- 最终输出：`build/processor.wasm`（只有一个文件）

### 3. 使用配置文件注册任务

配置文件 `config.yaml` 已经创建好，包含：
- 任务名称: `go-processor-example`
- 输入配置: Kafka 输入源
- 输出配置: Kafka 输出接收器

## 代码说明

### 实现的函数

根据 `wit/processor.wit` 定义，本示例实现了以下导出函数：

- `Init`: 初始化处理器（对应 WIT 的 `init`）
- `Process`: 处理输入数据（对应 WIT 的 `process`）
- `ProcessWatermark`: 处理 watermark（对应 WIT 的 `process-watermark`）
- `TakeCheckpoint`: 创建检查点（对应 WIT 的 `take-checkpoint`）
- `CheckHeartbeat`: 健康检查（对应 WIT 的 `check-heartbeat`）
- `Close`: 清理资源（对应 WIT 的 `close`）
- `ExecCustom`: 执行自定义命令（对应 WIT 的 `exec-custom`）

### 处理逻辑

当前实现了一个简单的 echo 处理器：
- 接收输入数据
- 添加处理标记前缀
- 通过 `collector.emit` 发送到输出（使用生成的绑定）

### WIT 绑定

生成的绑定代码位于 `bindings/` 目录，包含：
- `processor.go`: Processor world 的绑定
- `collector.go`: Collector 接口的绑定（用于调用 host 函数）
- `kv.go`: KV 接口的绑定（用于状态存储）

## 重要提示

### Component Model 支持

⚠️ **技术限制**: TinyGo 目前**不支持直接生成** Component Model 格式的 WASM。

**当前解决方案**：
- 构建脚本会自动处理转换步骤（对用户透明）
- 流程：`Go 代码` → `普通 WASM` → `Component Model`（自动完成）
- 最终输出：`build/processor.wasm`（Component Model 格式）

**为什么需要转换**：
- TinyGo 只能生成普通 WASM 模块（magic: `\0asm`）
- WASMHost 需要 Component Model 格式
- 因此需要 `wasm-tools` 进行转换（脚本自动完成）

**未来**：等待 TinyGo 添加 Component Model 支持后，可以直接生成。

### 函数命名

Go 函数名必须与 WIT 导出名称匹配：
- WIT: `export init: func(...)` → Go: `func Init(...)`
- WIT: `export process: func(...)` → Go: `func Process(...)`
- WIT: `export process-watermark: func(...)` → Go: `func ProcessWatermark(...)`

### 导入接口

`collector` 和 `kv` 接口是由 host 提供的导入接口：
- 使用生成的绑定代码调用这些接口
- 例如: `CollectorEmit(targetID, data)` 调用 host 的 `collector.emit`

## 配置说明

### config.yaml

```yaml
name: "go-processor-example"  # 任务名称
type: processor               # 配置类型

input-groups:
  - inputs:
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic"
        partition: 0
        group_id: "go-processor-group"

outputs:
  - output-type: kafka
    bootstrap_servers: "localhost:9092"
    topic: "output-topic"
    partition: 0
```

## 使用示例

1. **生成绑定并构建**:
   ```bash
   ./generate-bindings.sh
   ./build.sh
   ```

2. **注册任务** (通过 SQL):
   ```sql
   CREATE WASMTASK go-processor-example WITH (
       'wasm-path'='/path/to/examples/go-processor/build/processor.wasm',
       'config-path'='/path/to/examples/go-processor/config.yaml'
   );
   ```

## 故障排除

### 绑定生成失败

- 确保 `wit-bindgen-go` 已安装: `go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest`
- 检查 WIT 文件路径是否正确: `../../wit/processor.wit`

### WASM 不是 Component 格式

- 使用 `wasm-tools` 转换: `wasm-tools component new ...`
- 或等待 TinyGo 的 Component Model 支持

### 函数签名不匹配

- 确保 Go 函数名与 WIT 导出名称匹配（首字母大写）
- 检查参数类型是否与 WIT 定义一致

## 与 Rust 示例的区别

- **Rust**: 使用 `wasmtime-component-macro`，原生支持 Component Model
- **Go**: 使用 `wit-bindgen-go` 生成绑定，但需要额外步骤转换为 Component

推荐：如果可能，优先使用 Rust 示例，因为它对 Component Model 的支持更成熟。
