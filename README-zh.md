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

# Function Stream

[中文](README-zh.md) | [English](README.md)

**Function Stream** 是一个基于 Rust 构建的高性能、事件驱动的流处理框架。它提供了一个模块化的运行时，用于编排编译为 **WebAssembly (WASM)** 的 Serverless 风格处理函数，支持使用 **Go、Python 和 Rust** 编写函数。

## 目录

- [核心特性](#核心特性)
- [仓库结构](#仓库结构)
- [前置条件](#前置条件)
- [快速开始 (本地开发)](#快速开始-本地开发)
  - [1. 初始化环境](#1-初始化环境)
  - [2. 构建并运行服务端](#2-构建并运行服务端)
  - [3. 运行 CLI](#3-运行-cli)
- [构建与打包](#构建与打包)
  - [构建目标](#构建目标)
  - [分发 (打包)](#分发-打包)
  - [运行分发包](#运行分发包)
  - [使用启动脚本](#使用启动脚本-binstart-serversh)
- [维护](#维护)
- [文档](#文档)
- [配置](#配置)
- [许可证](#许可证)

## 核心特性

- **事件驱动的 WASM 运行时**：以接近原生的性能和沙箱隔离的方式执行多语言函数（Go、Python、Rust）。
- **持久化状态管理**：内置支持基于 RocksDB 的状态存储，用于有状态流处理。
- **SQL 驱动的 CLI**：使用类 SQL 命令进行作业管理和流检测的交互式 REPL。

## 仓库结构

```text
function-stream/
├── src/                     # 核心运行时、协调器、服务器、配置
├── protocol/                # Protocol Buffers 定义
├── cli/                     # SQL REPL 客户端
├── conf/                    # 默认运行时配置
├── docs/                    # 项目文档 (中/英)
├── examples/                # 示例处理器
├── python/                  # Python API、客户端和运行时 (WASM)
├── scripts/                 # 构建和环境自动化脚本
├── Makefile                 # 统一构建系统
└── Cargo.toml               # 工作区清单
```

## 前置条件

- **Rust 工具链**：Stable >= 1.77 (通过 rustup 安装)。
- **Python 3.9+**：构建 Python WASM 运行时所需。
- **Protoc**：Protocol Buffers 编译器（用于生成 gRPC 绑定）。
- **构建工具**：cmake, pkg-config, OpenSSL headers (用于 rdkafka)。

## 快速开始 (本地开发)

### 1. 初始化环境

我们提供了一个自动化脚本来设置 Python 虚拟环境 (`.venv`)、安装依赖项并编译必要的子模块。

```bash
make env
```

### 2. 构建并运行服务端

以 release 模式启动控制平面。如果缺少 Python WASM 运行时，构建系统将自动编译它。

```bash
cargo run --release --bin function-stream
```

### 3. 运行 CLI

在单独的终端中，启动 SQL REPL：

```bash
cargo run -p function-stream-cli -- --host 127.0.0.1 --port 8080
```

## 构建与打包

项目使用标准的 Makefile 来处理编译和分发。产物生成在 `dist/` 目录中。

### 构建目标

| 命令                | 描述                                               |
|-------------------|--------------------------------------------------|
| `make`            | `make build` 的别名。编译 Rust 二进制文件和 Python WASM 运行时。 |
| `make build`      | 构建完整版本（Rust + Python 支持）。                        |
| `make build-lite` | 构建精简版本（仅 Rust，无 Python 依赖）。                      |

### 分发 (打包)

要创建生产就绪的归档文件（`.tar.gz` 和 `.zip`），请使用 dist 目标。

```bash
make dist
```

**输出：**

- `dist/function-stream-<version>.tar.gz`
- `dist/function-stream-<version>.zip`

对于不包含 Python WASM 运行时的轻量级分发：

```bash
make dist-lite
```

**输出：**

- `dist/function-stream-<version>-lite.tar.gz`
- `dist/function-stream-<version>-lite.zip`

### 运行分发包

解压发布包后，您将看到以下结构：

```text
function-stream-<version>/
├── bin/
│   ├── function-stream      # 编译后的二进制文件
│   ├── cli                  # 编译后的 CLI 二进制文件
│   ├── start-server.sh      # 生产环境启动脚本
│   └── start-cli.sh         # CLI 启动脚本
├── conf/
│   └── config.yaml          # 默认配置
├── data/                    # 运行时数据（例如 WASM 缓存）
└── logs/                    # 日志目录（运行时创建）
```

### 使用启动脚本 (bin/start-server.sh)

我们提供了一个强大的 Shell 脚本来管理服务器进程，能够处理环境注入、守护进程化和配置覆盖。

**用法：**

```bash
./bin/start-server.sh [选项]
```

**选项：**

| 选项                      | 描述                              |
|-------------------------|---------------------------------|
| `-d`, `--daemon`        | 在后台运行服务器（守护进程模式）。               |
| `-c`, `--config <path>` | 指定自定义配置文件路径。                    |
| `-D <KEY>=<VALUE>`      | 注入环境变量（例如 `-D RUST_LOG=debug`）。 |

**示例：**

1. **前台模式 (Docker / 调试)** — 在当前 Shell 中运行服务器。适用于容器化环境 (Kubernetes/Docker) 或调试。

    ```bash
    ./bin/start-server.sh
    ```

2. **守护进程模式 (生产)** — 在后台运行服务器，将 stdout/stderr 重定向到 `logs/`。

    ```bash
    ./bin/start-server.sh -d
    ```

3. **自定义配置** — 使用特定配置文件启动。

    ```bash
    ./bin/start-server.sh -d -c config/config.yml
    ```

## 维护

| 命令               | 描述                                          |
|------------------|---------------------------------------------|
| `make clean`     | 深度清理。删除 `target/`、`dist/`、`.venv/` 和所有临时产物。 |
| `make env-clean` | 仅删除 Python 虚拟环境和 Python 产物。                 |
| `make test`      | 运行 Rust 测试套件。                               |

## 文档

| 文档                                                   | 描述            |
|------------------------------------------------------|---------------|
| [服务端配置与运维指南](docs/server-configuration-zh.md)        | 服务端配置与运维操作    |
| [Function 任务配置规范](docs/function-configuration-zh.md) | 任务定义规范        |
| [SQL CLI 交互式管理指南](docs/sql-cli-guide-zh.md)          | 交互式管理指南       |
| [Function 管理与开发指南](docs/function-development-zh.md)  | 管理与开发指南       |
| [Python SDK 开发与交互指南](docs/python-sdk-guide-zh.md)    | Python SDK 指南 |

## 配置

运行时行为由 `conf/config.yaml` 控制。您可以使用环境变量覆盖配置位置：

```bash
export FUNCTION_STREAM_CONF=/path/to/custom/config.yaml
```

## 许可证

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
