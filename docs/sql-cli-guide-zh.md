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

# SQL CLI 使用指南

Function Stream 提供了一套类 SQL 的声明式交互终端（REPL），旨在为运维人员提供低门槛、高效率的任务管控能力。

---

## 一、快速接入

SQL CLI 通过 gRPC 协议与远程 Server 通信。

### 1.1 启动连接

**发布包方式**：CLI 随发布包提供。使用前需执行 `make dist` 生成发布包，解压后在 `function-stream-<version>/` 目录下运行 `bin/start-cli.sh`。脚本依赖同目录下的 `bin/cli` 二进制，并从 `conf/config.yaml` 读取 Server 端口（或通过参数指定）。

```bash
./bin/start-cli.sh [选项]
```

**start-cli.sh 选项：**

| 选项                    | 说明                                                                       |
|-----------------------|--------------------------------------------------------------------------|
| -h, --host \<HOST\>   | Server 地址，默认 127.0.0.1。可通过环境变量 FUNCTION_STREAM_HOST 覆盖。                  |
| -p, --port \<PORT\>   | Server 端口，默认从 conf/config.yaml 解析或 8080。可通过环境变量 FUNCTION_STREAM_PORT 覆盖。 |
| -c, --config \<PATH\> | 指定配置文件路径，用于解析 Server 端口。默认 conf/config.yaml。                             |

**示例**：`./bin/start-cli.sh -h 10.0.0.1 -p 8080`

**开发方式**：

```bash
cargo run -p function-stream-cli -- -h <SERVER_HOST> -p <SERVER_PORT>
```

- **终端提示符**：成功连接后显示 `sql>`。
- **输入规范**：支持多行输入，系统通过 `;`（分号）或括号平衡检测来判定语句结束并提交执行。

---

## 二、命令语法详解

### 2.1 函数注册：CREATE FUNCTION

用于将预编译的 WASM 组件与其拓扑配置关联并注册到服务端。

**语法结构：**

```sql
CREATE FUNCTION WITH (
  'key1' = 'value1',
  'key2' = 'value2'
);
```

**关键属性说明：**

| 属性名           | 必填 | 说明                               |
|---------------|----|----------------------------------|
| function_path | 是  | WASM 组件在 Server 节点上的绝对路径。        |
| config_path   | 是  | 任务配置文件 (YAML) 在 Server 节点上的绝对路径。 |

**操作示例：**

```sql
CREATE FUNCTION WITH (
  'function_path' = '/data/apps/processors/etl_v1.wasm',
  'config_path'   = '/data/apps/configs/etl_v1.yaml'
);
```

**注意**：函数名称（Name）是自动从 config_path 指向的 YAML 文件中读取的，无需在 SQL 中显式指定。

### 2.2 状态观测：SHOW FUNCTIONS

检索当前 Server 托管的所有函数元数据及其运行快照。

```sql
SHOW FUNCTIONS;
```

**输出字段解析：**

- **name**：函数唯一标识名。
- **task_type**：算子类型（如 processor 代表 WASM）。
- **status**：当前运行状态（Running, Stopped, Failed, Created）。

### 2.3 生命周期管控：START / STOP / DROP

这组命令直接控制计算资源的分配与回收。

**启动任务**：将函数加载至运行时引擎并开始消费上游数据。

```sql
START FUNCTION go_processor_demo;
```

**停止任务**：暂停任务执行。

```sql
STOP FUNCTION go_processor_demo;
```

**物理卸载**：从元数据存储中彻底抹除该函数信息。

```sql
DROP FUNCTION go_processor_demo;
```

---

## 三、REPL 内建辅助指令

在 `sql>` 提示符下，除了标准 SQL 语句，还支持以下便捷指令：

| 指令    | 简写  | 说明                              |
|-------|-----|---------------------------------|
| help  | h   | 打印当前版本支持的所有语法模版。                |
| clear | cls | 清除屏幕输出缓存。                       |
| exit  | q   | 安全断开 gRPC 连接并退出终端。支持 quit 或 :q。 |

---

## 四、技术约束与注意事项

- **路径隔离**：SQL CLI 本身不负责上传文件。function_path 指向的文件必须预先存在于**服务端机器**的磁盘上。若需远程上传打包，请使用 Python SDK。
- **Python 函数限制**：由于 Python 函数涉及动态依赖分析与代码打包，目前**不支持**通过 SQL CLI 创建，仅能通过 CLI 进行 START / STOP / SHOW 等生命周期管理。
