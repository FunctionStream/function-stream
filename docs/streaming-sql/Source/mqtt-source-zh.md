# MQTT Source

`mqtt` source 用于从 MQTT broker 订阅 topic 并持续消费消息。

## 支持格式

- `json`
- `raw_string`
- `raw_bytes`

## 常用 WITH 参数

- `connector='mqtt'`
- `type='source'`（默认）
- `topic='sensors/#'`
- `mqtt.host='127.0.0.1'`（或 `host`）
- `mqtt.port='1883'`（或 `port`，默认 1883）
- `format='json'|'raw_string'|'raw_bytes'`
- `mqtt.client.id`（可选；未设置时运行时生成 `fs-{job_id}-{subtask}-mqtt`）
- `mqtt.username` / `mqtt.password`（可选）
- `mqtt.qos`（可选，0/1/2，默认 1）
- `mqtt.clean.session`（可选，默认 `true`）
- `mqtt.keep.alive.secs`（可选，默认 60）
- `rate_limit.messages_per_second`（可选）：**默认不配置 = 不限速**

## 并行度

MQTT 订阅在多个 subtask 上会重复消费同一 topic，**建议 pipeline `parallelism = 1`**。

## Checkpoint

v1 不向 catalog 持久化 MQTT 偏移；故障恢复依赖 broker 会话与 `client_id` / `clean_session` 配置（至少一次语义）。

## 示例

```sql
CREATE TABLE src_mqtt_json (
  device_id STRING,
  value DOUBLE,
  ts TIMESTAMP
) WITH (
  connector='mqtt',
  type='source',
  topic='sensors/data',
  'mqtt.host'='127.0.0.1',
  'mqtt.port'='1883',
  format='json'
);
```
