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

# Function 开发指南

Function 是 Function Stream 的核心计算单元。本指南以 Go (WASM) 模式为例，详细说明 Function 的全生命周期管理，并对比 Python 模式的操作差异。

---

## 一、Function 类型概览

在开始之前，请根据您的业务场景选择合适的算子类型：

| 特性   | WASM Function (type: processor) | Python Function (type: python) |
|------|---------------------------------|--------------------------------|
| 支持语言 | Go, Rust, C++, Python (编译后)     | 原生 Python 脚本                   |
| 性能   | 极高（接近原生，适合高吞吐清洗）                | 中（开发效率高，适合算法逻辑）                |
| 隔离性  | 强沙箱隔离                           | 进程级隔离                          |
| 注册方式 | SQL CLI / Python Client / gRPC  | 仅限 Python Client (gRPC)        |

---

## 二、算子开发示例：Go (WASM)

### 2.1 构建流程 (Build)

Function Stream 采用 WebAssembly 组件模型（Component Model）。Go 代码需编译为满足接口定义的 WASM 二进制文件。

```bash
# 进入示例目录并执行自动化构建
cd examples/go-processor
chmod +x build.sh
./build.sh
```

- **产物**：`build/processor.wasm`
- **依赖**：确保环境已安装 TinyGo、wasm-tools 及 wit-bindgen-go。

### 2.2 任务拓扑配置 (config.yaml)

此文件定义了函数的数据流向与运行时属性。完整字段说明见 [Function 任务配置规范](function-configuration-zh.md)。

```yaml
name: "go-processor-example"   # 任务全局唯一标识
type: processor                # 指定为 WASM 模式

input-groups:                  # 定义输入源拓扑
  - inputs:
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic"
        group_id: "go-processor-group"

outputs:                       # 定义输出通道
  - output-type: kafka
    bootstrap_servers: "localhost:9092"
    topic: "output-topic"
    partition: 0
```

---

## 三、注册与部署管理

### 3.1 方式一：使用 SQL CLI 管理（推荐运维使用）

适合手动部署及线上故障快速排查。

```sql
-- 1. 注册函数 (必须使用绝对路径)
CREATE FUNCTION WITH (
  'function_path'='/opt/fs/examples/go-processor/build/processor.wasm',
  'config_path'='/opt/fs/examples/go-processor/config.yaml'
);

-- 2. 启动处理任务
START FUNCTION go-processor-example;

-- 3. 查看运行状态
SHOW FUNCTIONS;
```

### 3.2 方式二：使用 Python SDK 管理（推荐自动化集成）

适合 CI/CD 流水线或编写自动化调度脚本。

```python
from fs_client import FsClient

# 通过上下文管理器安全连接服务端
with FsClient(host="localhost", port=8080) as client:
    # 注册 WASM 函数
    client.create_function_from_files(
        function_path="examples/go-processor/build/processor.wasm",
        config_path="examples/go-processor/config.yaml",
    )
    # 启动函数
    client.start_function("go-processor-example")
```

---

## 四、生命周期管理矩阵

下表总结了对 Function 进行日常运维的所有标准化操作：

| 运维动作          | SQL 命令                      | Python Client 接口           | 说明                 |
|---------------|-----------------------------|----------------------------|--------------------|
| 注册 (Register) | CREATE FUNCTION WITH        | create_function_from_files | 将逻辑与配置持久化至 Server。 |
| 列表 (List)     | SHOW FUNCTIONS              | show_functions()           | 查看所有任务及其运行状态。      |
| 启动 (Start)    | START FUNCTION &lt;name&gt; | start_function(name)       | 分配执行器并开始消费数据。      |
| 停止 (Stop)     | STOP FUNCTION &lt;name&gt;  | stop_function(name)        | 暂停任务执行。            |
| 删除 (Drop)     | DROP FUNCTION &lt;name&gt;  | drop_function(name)        | 物理删除任务元数据及代码。      |
