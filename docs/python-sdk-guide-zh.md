# Python SDK 开发与交互指南

Function Stream 为 Python 开发者提供了一套完整的工具链，涵盖了从流算子开发 (fs_api) 到管理 (fs_client) 的全流程。

---

## 一、SDK 核心组件定义

| 包名 | 定位 | 核心功能 |
|------|------|----------|
| fs_api | 算子开发接口 | 定义 Processor 逻辑，提供 KV 状态存取及数据发射 (Emit) 能力。 |
| fs_client | 集群控制客户端 | 基于 gRPC，实现函数的远程注册、状态控制及拓扑配置。 |
| fs_runtime | 内建运行时（内部使用） | 封装了在 WASM 隔离环境中的 Python 解释器行为。 |

---

## 二、fs_api：算子开发深度解析

### 2.1 核心基类 FSProcessorDriver

所有 Python 算子必须继承此类。它是逻辑执行的入口点，由服务端自动触发其生命周期方法。

| 钩子方法                       | 触发时机        | 业务逻辑建议                   |
|----------------------------|-------------|--------------------------|
| init(ctx, config)          | 函数启动时执行一次   | 自定义 init_config。         |
| process(ctx, source, data) | 每接收到一条消息时触发 | 核心逻辑所在。执行计算、状态读写及结果下发。   |
| process_watermark(...)     | 接收到水位线事件时触发 | 处理基于时间的窗口触发或乱序重排逻辑。      |
| take_checkpoint(ctx, id)   | 系统进行状态备份时回调 | 返回额外需要持久化的内存状态，实现强一致性保障。 |
| check_heartbeat(ctx)       | 定期健康检查      | 检查内部算子，返回 False 将触发算子重启。 |

### 2.2 有状态计算：Context 与 KvStore

Function Stream 的强大之处在于其内建的本地状态管理。

**Context 交互：**

- `ctx.emit(bytes, channel=0)`：将处理结果推向指定的输出通道。
- `ctx.getOrCreateKVStore("name")`：访问基于 RocksDB 的本地状态存储。

**KvStore 接口：**

- 支持基础的 `put_state` / `get_state`。
- 进阶支持 ComplexKey（复杂键）操作，适用于多维索引或前缀扫描场景。

### 2.3 生产级代码示例

```python
from fs_api import FSProcessorDriver, Context
import json

class MetricProcessor(FSProcessorDriver):
    def init(self, ctx: Context, config: dict):
        self.metric_name = config.get("metric_name", "default_event")

    def process(self, ctx: Context, source_id: int, data: bytes):
        # 1. 解析输入
        event = json.loads(data.decode())
        
        # 2. 状态原子操作：累加计数
        store = ctx.getOrCreateKVStore("stats_db")
        key = f"count:{self.metric_name}".encode()
        current_val = int(store.get_state(key) or 0)
        
        new_val = current_val + event.get("value", 1)
        store.put_state(key, str(new_val).encode())
        
        # 3. 发射处理结果
        result = {"metric": self.metric_name, "total": new_val}
        ctx.emit(json.dumps(result).encode(), 0)
```

---

## 三、fs_client：function 管理

### 3.1 function 创建流程：自动化依赖打包

不同于 WASM 模式需手动编译，fs_client 注册 Python 函数时会执行以下操作：

1. **静态分析**：自动扫描 ProcessorClass 所在的模块及其引用的本地依赖。
2. **资源打包**：将代码及其依赖项封装为特定格式。
3. **远程注册**：通过 gRPC 流式上传至 Server 并在沙箱中拉起运行。

### 3.2 链式配置构建 (WasmTaskBuilder)

使用 Builder 模式可以清晰地定义函数的 I/O 拓扑：

```python
from fs_client.config import WasmTaskBuilder, KafkaInput, KafkaOutput

task_config = (
    WasmTaskBuilder()
    .set_name("python-etl-job")
    .add_init_config("metric_name", "user_click")
    .add_input_group([
        KafkaInput(bootstrap_servers="kafka:9092", topic="raw-data", group_id="fs-group")
    ])
    .add_output(KafkaOutput(bootstrap_servers="kafka:9092", topic="clean-data", partition=0))
    .build()
)
```

### 3.3 客户端交互全流程

```python
from fs_client import FsClient

with FsClient(host="10.0.0.1", port=8080) as client:
    # 1. 注册 Python 算子
    client.create_python_function_from_config(task_config, MetricProcessor)
    
    # 2. 启动函数
    client.start_function("python-etl-job")
    
    # 3. 监控状态
    status = client.show_functions(filter_pattern="python-etl-job")
    print(f"Task Status: {status.functions[0].status}")
```

---

## 四、运维与异常处理矩阵

| 异常类                   | 触发原因                       | 推荐处理策略                         |
|-----------------------|----------------------------|--------------------------------|
| ConflictError (409)   | 尝试注册已存在的函数名                | 先调用 drop_function 或修改任务名称。     |
| BadRequestError (400) | YAML 配置不满足规范或 Kafka 参数错误   | 检查 WasmTaskBuilder 中的配置项。      |
| ServerError (500)     | Server 侧运行时环境（如 RocksDB）异常 | 检查服务端 conf/config.yaml 存储路径权限。 |
| NotFoundError (404)   | 操作了不存在的函数或无效的 Checkpoint   | 确认函数名是否输入正确。                   |
