# Kafka Source

`kafka` source 用于从 Kafka topic 持续消费数据。

## 支持格式

- `json`
- `raw_string`
- `raw_bytes`

## 常用 WITH 参数

- `connector='kafka'`
- `type='source'`（默认）
- `topic='topic_in'`
- `bootstrap.servers='host:9092'`
- `group.id='consumer_group'`（可选；未指定时自动生成为 `fs-<表名>-consumer` 并写入 catalog）
- `format='json'|'raw_string'|'raw_bytes'`
- `scan.startup.mode='earliest'|'latest'|'group-offsets'`（可选）

## Consumer Group

创建 Kafka Source 表时若未指定 `group.id` 且未指定 `group.id.prefix`，系统会在保存 catalog 前自动分配：

```text
group.id = fs-<表名>-consumer
```

`SHOW CREATE TABLE` 与重启后 catalog 恢复均使用该值，保证 offset 按固定 consumer group 提交。若需每个 subtask 独立 group，可设置 `group.id.prefix`，由 runtime 追加 job/subtask 后缀。

## 吞吐说明

Kafka Source **不限速**；默认 batch=4096、linger=20ms、fetch 高吞吐参数已内置。

## 示例

```sql
CREATE TABLE src_kafka_json (
  user_id BIGINT,
  event STRING,
  ts TIMESTAMP
) WITH (
  connector='kafka',
  type='source',
  topic='topic_in',
  'bootstrap.servers'='127.0.0.1:9092',
  format='json'
);
```

上例未写 `group.id`，catalog 中会自动保存 `'group.id'='fs-src_kafka_json-consumer'`。
