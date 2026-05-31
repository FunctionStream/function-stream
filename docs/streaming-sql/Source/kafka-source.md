# Kafka Source

`kafka` source is used to ingest streaming records from Kafka topics.

## Supported formats

- `json`
- `raw_string`
- `raw_bytes`

## Common `WITH` options

- `connector='kafka'`
- `topic='topic_name'`
- `bootstrap.servers='host:9092'`
- `group.id='consumer_group'` (optional; if omitted, defaults to `fs-<table_name>-consumer` and is persisted in the catalog)
- `format='json'|'raw_string'|'raw_bytes'`
- `scan.startup.mode='earliest'|'latest'|'group-offsets'` (optional)

## Consumer group

When you create a Kafka source table without `group.id` and without `group.id.prefix`, the planner assigns before catalog save:

```text
group.id = fs-<table_name>-consumer
```

`SHOW CREATE TABLE` and catalog reload after restart use this value so offsets commit under a stable consumer group. For per-subtask groups, set `group.id.prefix` instead; the runtime appends job/subtask suffixes.

## Example

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

If `group.id` is omitted, the catalog stores `'group.id'='fs-src_kafka_json-consumer'`.

