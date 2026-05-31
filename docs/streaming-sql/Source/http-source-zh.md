# HTTP Source（机器人 / REST）

`http` source 面向机器人遥测与 REST 接口，配置命名对齐 Flink HTTP / `rest-lookup` 连接器习惯。

## 模式

| 模式 | 场景 | 必填参数 |
|------|------|----------|
| **poll**（默认，有 `url` 时） | 定时拉取机器人 HTTP API | `url`、`format` |
| **webhook**（有 `http.listen.port` 或 `mode='webhook'`） | 机器人/边缘网关 **POST** 上报 | `http.listen.port`、`format` |

## 支持格式

- `json`
- `raw_string`
- `raw_bytes`

## Flink 对齐参数

| 参数 | Flink 对应 | 说明 |
|------|------------|------|
| `connector='http'` | `connector` | 固定为 `http` |
| `url` | `url` | Poll 模式请求地址 |
| `format` | `format` | 如 `json` |
| `lookup-method` / `http.method` | `lookup-method` | `GET`（默认）/ `POST` / `PUT` |
| `scan.interval` | — | 轮询周期，如 `INTERVAL '1' SECOND` |
| `scan.interval.ms` | — | 轮询周期毫秒（默认 1000） |
| `http.request.body` / `body` | `http.request.body-template` | POST 请求体 |
| `header.<name>` | 自定义 Header | 如 `'header.Authorization'='Bearer xxx'` |
| `http.request.timeout.ms` | `gid.connector.http.source.lookup.request.timeout` | 请求超时（默认 30000） |
| `http.response.split` | — | `single`（默认）/ `ndjson` / `json-array` |
| `rate_limit.messages_per_second` | — | 默认不限速 |

## Webhook 参数（机器人上报）

| 参数 | 说明 | 默认 |
|------|------|------|
| `http.listen.host` | 监听地址 | `0.0.0.0` |
| `http.listen.port` | 监听端口 | 必填 |
| `http.webhook.path` | POST 路径 | `/` |
| `mode` | `poll` / `webhook` | 按 `url` / 端口自动推断 |

## 示例：轮询机器人状态 API

```sql
CREATE TABLE robot_status (
  robot_id STRING,
  battery DOUBLE,
  pose STRING,
  ts TIMESTAMP
) WITH (
  connector='http',
  type='source',
  url='http://127.0.0.1:8080/api/v1/robots/status',
  'lookup-method'='GET',
  'scan.interval'='INTERVAL ''2'' SECOND',
  format='json'
);
```

## 示例：Webhook 接收机器人上报

```sql
CREATE TABLE robot_telemetry (
  robot_id STRING,
  joint_states STRING,
  ts TIMESTAMP
) WITH (
  connector='http',
  type='source',
  mode='webhook',
  'http.listen.host'='0.0.0.0',
  'http.listen.port'='18080',
  'http.webhook.path'='/ingest/telemetry',
  format='json'
);
```

机器人向 `POST http://<fs-host>:18080/ingest/telemetry` 发送 JSON 即可。

## 并行度

Poll / Webhook 均建议 **`parallelism = 1`**。Webhook 多 subtask 会重复绑定端口。

## Checkpoint

v1 不持久化 HTTP 偏移；Poll 模式按 `scan.interval` 重复拉取；Webhook 依赖上游重试。
