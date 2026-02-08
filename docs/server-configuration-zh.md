# 服务端配置与运维指南

Function Stream 是一个云原生流处理平台，支持 WASM (Go/Rust) 与 Python 编写的轻量级计算任务。

---

## 一、服务端配置 (Server Configuration)

服务端（Server）由 `conf/config.yaml` 控制，控制着网络、日志、运行引擎以及持久化存储。

### 1.1 基础服务 (Service)

| 参数名        | 类型     | 必填 | 默认值             | 说明与建议                             |
|------------|--------|----|-----------------|-----------------------------------|
| service_id | string | 否  | default-service | 实例唯一标识。在集群部署中建议设为 hostname。       |
| host       | string | 是  | 127.0.0.1       | 监听地址。若需对外提供服务或在容器中运行，请设为 0.0.0.0。 |
| port       | int    | 是  | 8080            | gRPC 服务端口。                        |
| workers    | int    | 否  | CPU核心 × 4       | 线程池深度。I/O 密集型任务建议适当调高倍率。          |
| debug      | bool   | 否  | false           | 开启后将记录 Trace 级别日志。生产环境建议关闭。       |

### 1.2 日志系统 (Logging)

| 参数名           | 类型     | 默认值          | 说明与建议                                 |
|---------------|--------|--------------|---------------------------------------|
| level         | string | info         | 日志级别：trace, debug, info, warn, error。 |
| format        | string | json         | 建议设为 json 以配合 ELK 或 Loki 进行结构化采集。     |
| file_path     | string | logs/app.log | 日志存储路径。                               |
| max_file_size | int    | 100          | 单个文件最大大小 (MB)。                        |
| max_files     | int    | 5            | 滚动保留的历史日志文件数量。                        |

---

## 二、运行时配置 (Runtimes)

Python 配置关注解释器环境，WASM 配置关注组件编译优化。

### 2.1 Python 运行时 (python)

| 参数名          | 类型     | 默认值           | 说明与建议                                |
|--------------|--------|---------------|--------------------------------------|
| wasm_path    | string | (内置路径)        | 关键。指向 Python 运行时 WASM 引擎。若路径错误将无法启动。 |
| cache_dir    | string | data/cache/py | AOT (Ahead-of-Time) 编译产物存放地。需确保读写权限。 |
| enable_cache | bool   | true          | 核心设置。开启后可消除 Python 任务冷启动的秒级延迟。       |

### 2.2 WASM 运行时 (wasm)

| 参数名            | 类型     | 默认值         | 说明与建议                         |
|----------------|--------|-------------|-------------------------------|
| cache_dir      | string | .cache/wasm | 存储 WASM 组件增量编译后的中间机器码。        |
| enable_cache   | bool   | true        | 启用增量编译缓存，用于加速函数逻辑热更新。         |
| max_cache_size | int    | 100MB       | 缓存上限。若部署函数超过 50 个，建议调至 512MB。 |

---

## 三、存储配置 (Storage)

系统基于 RocksDB 实现任务元数据与状态的持久化。

| 配置项 (RocksDB)         | 默认值     | 说明与建议                                  |
|-----------------------|---------|----------------------------------------|
| storage_type          | rocksdb | 生产环境必须使用 rocksdb，memory 模式仅供临时测试。      |
| base_dir              | data    | 数据根目录。建议挂载到 SSD 存储卷以保证 I/O 吞吐。         |
| max_open_files        | 1000    | RocksDB 允许打开的文件句柄数。需配合系统 ulimit -n 调整。 |
| write_buffer_size     | 64MB    | 写缓冲区大小。写入频率极高时，建议调大至 128MB。            |
| target_file_size_base | 64MB    | L0 层 SST 文件目标大小。                       |

---

## 四、任务开发与管理

### 4.1 任务 YAML 配置规范

每个 Function 需定义一套 I/O 拓扑：

- **name / type**：任务名称及类型 (processor / python)。
- **input-groups**：定义输入源（如 Kafka 的 bootstrap_servers, topic, group_id）。
- **outputs**：定义输出通道（如 Kafka 的 topic, partition）。

详见 [Function 任务配置规范](function-configuration-zh.md)。

### 4.2 管理命令 (SQL CLI)

通过 SQL REPL 进行函数全生命周期管理：

- **创建**：`CREATE FUNCTION WITH ('function_path'='...', 'config_path'='...');`
- **启动/停止**：`START FUNCTION <name>;` / `STOP FUNCTION <name>;`
- **查看/删除**：`SHOW FUNCTIONS;` / `DROP FUNCTION <name>;`

---

## 五、运维操作 (Operations)

### 5.1 启动脚本 (bin/start-server.sh)

该脚本随发布包提供。使用前需执行 `make dist` 生成发布包，解压后在 `function-stream-<version>/` 目录下运行。脚本依赖同目录下的 `bin/function-stream` 二进制及 `conf/config.yaml`。

- **后台运行**：`./bin/start-server.sh -d`
- **自定义配置**：`./bin/start-server.sh -c /path/to/config.yaml`
