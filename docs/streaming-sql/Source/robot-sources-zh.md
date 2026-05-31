# 机器人场景 Source 指南

面向移动机器人、机械臂、AGV 等设备的流式接入说明。配置风格参考 **Apache Flink** 连接器（`url`、`format`、`scan.interval`、`lookup-method` 等）。

## 已支持 Source

| Connector | 典型场景 | 文档 |
|-----------|----------|------|
| **kafka** | 经 ROS2/网关桥接后的汇聚 topic、平台总线 | [kafka-source-zh.md](kafka-source-zh.md) |
| **mqtt** | 设备直连、边缘 MQTT Broker、IoT 规则引擎 | [mqtt-source-zh.md](mqtt-source-zh.md) |
| **http** | REST 轮询机器人 API；Webhook 接收 POST 遥测 | [http-source-zh.md](http-source-zh.md) |
| **ros** | ROS1 topic（rosbridge WebSocket） | [ros-source-zh.md](ros-source-zh.md) |
| **ros2** | ROS2 topic（rosbridge_server） | [ros2-source-zh.md](ros2-source-zh.md) |
| **robot-bag** | MCAP / ROS1 bag / ROS2 bag / PCD / JSONL / CSV 离线回放 | [robot-bag-source-zh.md](robot-bag-source-zh.md) |

## 推荐选型

```text
机器人 ──MQTT──► Broker ──► function-stream (mqtt source)
机器人 ──HTTP POST──► function-stream (http webhook)
调度系统 ──HTTP GET API──► function-stream (http poll)
ROS/ROS2 ──rosbridge──► function-stream (ros / ros2 source)
离线录制 ──bag/MCAP/PCD──► function-stream (robot-bag source)
```

| 数据形态 | 推荐 Connector | 说明 |
|----------|----------------|------|
| 高频遥测、弱网重传 | `mqtt` | QoS 1/2、`client_id` 稳定 |
| 云平台/第三方 REST 回调 | `http` webhook | `http.listen.port` + `http.webhook.path` |
| 定时查询机器人状态 | `http` poll | `url` + `scan.interval` |
| ROS/ROS2 实时 topic | `ros` / `ros2` | rosbridge WebSocket |
| 离线 bag / MCAP / PCD / 日志 | `robot-bag` | `path` + `bag.format` |
| 已有 Kafka 管线 | `kafka` | `group.id`、`scan.startup.mode` |

## robot-bag 格式对照

| `bag.format` | 输入 | topic 过滤 | 推荐 `format` |
|--------------|------|------------|---------------|
| `ros1` | `.bag` 文件 | 支持 | 二进制 ROS 消息用 `raw_bytes` |
| `ros2` | `.db3` 或目录 | 支持 | `raw_bytes` |
| `mcap` | `.mcap` 文件 | 支持 | `json` 或 `raw_bytes` |
| `pcd` | `.pcd` 或目录 | 不支持 | `json`（`pcd.emit.mode=point` 或 `cloud`） |
| `jsonl` | `.jsonl` | 不支持 | `json` |
| `csv` | `.csv` | 不支持 | `json` 或 `raw_string` |

英文文档：[robot-sources.md](robot-sources.md) · [robot-bag-source.md](robot-bag-source.md)

## 规划中（常量已预留，待实现）

| Connector | 机器人场景 | Flink / 业界参考 |
|-----------|------------|------------------|
| `websocket` | 实时关节流、仿真器推送 | WebSocket Table Source |
| `nats` | 边缘集群、JetStream | NATS connector |
| `pulsar` | 多租户机器人云平台 | Pulsar source |
| `redis` | 设备在线状态、Stream | Redis Streams |
| `sse` | 浏览器/轻量网关 Server-Sent Events | — |

工业现场 **Modbus / OPC-UA** 建议经边缘网关转为 MQTT/HTTP 后接入，不在引擎内直接实现现场总线。

## 通用约定

1. **`format='json'`** 最常用；单字段二进制可用 `raw_bytes`。
2. **`parallelism = 1`**：MQTT 订阅、HTTP Webhook 端口、HTTP 单 URL 轮询均避免重复消费。
3. **吞吐**：Source 默认 batch=4096、linger=20ms；Kafka **无限速**；Delta 缓冲默认 256MB 并合并小 batch 写 Parquet。
4. **Checkpoint**：Kafka 支持分区 offset；MQTT/HTTP v1 为至少一次，依赖 broker 会话或轮询间隔。

## 端到端示例（MQTT + Delta）

```sql
-- 1. 机器人经 MQTT 上报
CREATE TABLE robot_events (
  robot_id STRING,
  event_type STRING,
  payload STRING,
  ts TIMESTAMP
) WITH (
  connector='mqtt',
  type='source',
  topic='factory/robot/+/telemetry',
  'mqtt.host'='127.0.0.1',
  'mqtt.port'='1883',
  format='json'
);

-- 2. 流式作业（示意）
CREATE STREAMING TABLE robot_sink
WITH (
  connector='delta',
  type='sink',
  path='/data/robot',
  format='parquet'
) AS
SELECT * FROM robot_events;
```

## 添加新 Source 的开发清单

见项目内 MQTT/HTTP 实现路径：Proto `XxxSourceConfig` → Planner `SourceProvider` → Runtime `SourceOperator` → `ConnectorSourceDispatcher` 注册。
