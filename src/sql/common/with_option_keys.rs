// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL `WITH` 子句中的选项名，以及部分连接器序列化 JSON 的字段名（单一来源）。

// ── 通用 / 表级 ─────────────────────────────────────────────────────────────

pub const CONNECTOR: &str = "connector";
pub const TYPE: &str = "type";
pub const FORMAT: &str = "format";
/// 未指定 `format` 选项时的默认格式名（值，非键）。
pub const DEFAULT_FORMAT_VALUE: &str = "json";
pub const BAD_DATA: &str = "bad_data";
pub const PARTITION_BY: &str = "partition_by";

pub const EVENT_TIME_FIELD: &str = "event_time_field";
pub const WATERMARK_FIELD: &str = "watermark_field";

pub const IDLE_MICROS: &str = "idle_micros";
pub const IDLE_TIME: &str = "idle_time";

pub const LOOKUP_CACHE_MAX_BYTES: &str = "lookup.cache.max_bytes";
pub const LOOKUP_CACHE_TTL: &str = "lookup.cache.ttl";

// ── 非 Kafka 连接器的 opaque JSON（`CONNECTOR` 与 WITH 选项同名）────────────

pub const CONNECTION_SCHEMA: &str = "connection_schema";

// ── 后端参数序列化（如 lookup）──────────────────────────────────────────────

pub const ADAPTER: &str = "adapter";

// ── Kafka ─────────────────────────────────────────────────────────────────

pub const KAFKA_BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const KAFKA_BOOTSTRAP_SERVERS_LEGACY: &str = "bootstrap_servers";
pub const KAFKA_TOPIC: &str = "topic";
pub const KAFKA_RATE_LIMIT_MESSAGES_PER_SECOND: &str = "rate_limit.messages_per_second";
pub const KAFKA_VALUE_SUBJECT: &str = "value.subject";
pub const KAFKA_SCAN_STARTUP_MODE: &str = "scan.startup.mode";
pub const KAFKA_ISOLATION_LEVEL: &str = "isolation.level";
pub const KAFKA_GROUP_ID: &str = "group.id";
pub const KAFKA_GROUP_ID_LEGACY: &str = "group_id";
pub const KAFKA_GROUP_ID_PREFIX: &str = "group.id.prefix";
pub const KAFKA_SINK_COMMIT_MODE: &str = "sink.commit.mode";
pub const KAFKA_SINK_KEY_FIELD: &str = "sink.key.field";
pub const KAFKA_KEY_FIELD_LEGACY: &str = "key.field";
pub const KAFKA_SINK_TIMESTAMP_FIELD: &str = "sink.timestamp.field";
pub const KAFKA_TIMESTAMP_FIELD_LEGACY: &str = "timestamp.field";

// ── JSON format ───────────────────────────────────────────────────────────

pub const JSON_CONFLUENT_SCHEMA_REGISTRY: &str = "json.confluent_schema_registry";
pub const JSON_CONFLUENT_SCHEMA_VERSION: &str = "json.confluent_schema_version";
pub const JSON_INCLUDE_SCHEMA: &str = "json.include_schema";
pub const JSON_DEBEZIUM: &str = "json.debezium";
pub const JSON_UNSTRUCTURED: &str = "json.unstructured";
pub const JSON_TIMESTAMP_FORMAT: &str = "json.timestamp_format";
pub const JSON_DECIMAL_ENCODING: &str = "json.decimal_encoding";
pub const JSON_COMPRESSION: &str = "json.compression";

// ── Avro ──────────────────────────────────────────────────────────────────

pub const AVRO_CONFLUENT_SCHEMA_REGISTRY: &str = "avro.confluent_schema_registry";
pub const AVRO_RAW_DATUMS: &str = "avro.raw_datums";
pub const AVRO_INTO_UNSTRUCTURED_JSON: &str = "avro.into_unstructured_json";
pub const AVRO_SCHEMA_ID: &str = "avro.schema_id";

// ── Parquet ───────────────────────────────────────────────────────────────

pub const PARQUET_COMPRESSION: &str = "parquet.compression";
pub const PARQUET_ROW_GROUP_BYTES: &str = "parquet.row_group_bytes";

// ── Protobuf ────────────────────────────────────────────────────────────────

pub const PROTOBUF_INTO_UNSTRUCTURED_JSON: &str = "protobuf.into_unstructured_json";
pub const PROTOBUF_MESSAGE_NAME: &str = "protobuf.message_name";
pub const PROTOBUF_CONFLUENT_SCHEMA_REGISTRY: &str = "protobuf.confluent_schema_registry";
pub const PROTOBUF_LENGTH_DELIMITED: &str = "protobuf.length_delimited";

// ── Framing ─────────────────────────────────────────────────────────────────

pub const FRAMING_METHOD: &str = "framing.method";
pub const FRAMING_MAX_LINE_LENGTH: &str = "framing.max_line_length";

// ── 从字符串 map 推断编码（catalog 等）──────────────────────────────────────

pub const FORMAT_DEBEZIUM_FLAG: &str = "format.debezium";
