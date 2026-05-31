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

pub const CONNECTOR: &str = "connector";
pub const TYPE: &str = "type";
pub const FORMAT: &str = "format";
pub const DEFAULT_FORMAT_VALUE: &str = "json";
pub const BAD_DATA: &str = "bad_data";
pub const PARTITION_BY: &str = "partition_by";
pub const PATH: &str = "path";
pub const SINK_PATH: &str = "sink.path";

pub const EVENT_TIME_FIELD: &str = "event_time_field";
pub const WATERMARK_FIELD: &str = "watermark_field";

pub const IDLE_MICROS: &str = "idle_micros";
pub const IDLE_TIME: &str = "idle_time";

pub const LOOKUP_CACHE_MAX_BYTES: &str = "lookup.cache.max_bytes";
pub const LOOKUP_CACHE_TTL: &str = "lookup.cache.ttl";

pub const CONNECTION_SCHEMA: &str = "connection_schema";

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

// ── MQTT ──────────────────────────────────────────────────────────────────

pub const MQTT_HOST: &str = "mqtt.host";
pub const MQTT_HOST_LEGACY: &str = "host";
pub const MQTT_PORT: &str = "mqtt.port";
pub const MQTT_PORT_LEGACY: &str = "port";
pub const MQTT_TOPIC: &str = "topic";
pub const MQTT_CLIENT_ID: &str = "mqtt.client.id";
pub const MQTT_CLIENT_ID_LEGACY: &str = "client.id";
pub const MQTT_USERNAME: &str = "mqtt.username";
pub const MQTT_USERNAME_LEGACY: &str = "username";
pub const MQTT_PASSWORD: &str = "mqtt.password";
pub const MQTT_PASSWORD_LEGACY: &str = "password";
pub const MQTT_QOS: &str = "mqtt.qos";
pub const MQTT_QOS_LEGACY: &str = "qos";
pub const MQTT_CLEAN_SESSION: &str = "mqtt.clean.session";
pub const MQTT_CLEAN_SESSION_LEGACY: &str = "clean.session";
pub const MQTT_KEEP_ALIVE_SECS: &str = "mqtt.keep.alive.secs";
pub const MQTT_KEEP_ALIVE_SECS_LEGACY: &str = "keep.alive.secs";
pub const MQTT_RATE_LIMIT_MESSAGES_PER_SECOND: &str = "rate_limit.messages_per_second";

// ── HTTP (Flink rest-lookup / robot REST aligned) ─────────────────────────

pub const HTTP_MODE: &str = "mode";
pub const HTTP_URL: &str = "url";
pub const HTTP_METHOD: &str = "http.method";
pub const HTTP_LOOKUP_METHOD: &str = "lookup-method";
pub const HTTP_SCAN_INTERVAL: &str = "scan.interval";
pub const HTTP_SCAN_INTERVAL_MS: &str = "scan.interval.ms";
pub const HTTP_POLL_INTERVAL_MS: &str = "http.poll.interval.ms";
pub const HTTP_REQUEST_BODY: &str = "http.request.body";
pub const HTTP_REQUEST_BODY_LEGACY: &str = "body";
pub const HTTP_LISTEN_HOST: &str = "http.listen.host";
pub const HTTP_LISTEN_PORT: &str = "http.listen.port";
pub const HTTP_WEBHOOK_PATH: &str = "http.webhook.path";
pub const HTTP_REQUEST_TIMEOUT_MS: &str = "http.request.timeout.ms";
pub const HTTP_RESPONSE_SPLIT: &str = "http.response.split";
pub const HTTP_HEADER_PREFIX: &str = "header.";

// ── ROS / rosbridge ───────────────────────────────────────────────────────

pub const ROS_URL: &str = "url";
pub const ROS_URL_LEGACY: &str = "ros.url";
pub const ROS_TOPIC: &str = "topic";
pub const ROS_TOPIC_LEGACY: &str = "ros.topic";
pub const ROS_MESSAGE_FIELD: &str = "ros.message.field";
pub const ROS_MESSAGE_FIELD_LEGACY: &str = "message.field";
pub const ROS_VERSION: &str = "ros.version";

// ── Robot bag / file formats ──────────────────────────────────────────────

pub const ROBOT_BAG_PATH: &str = "path";
pub const ROBOT_BAG_FORMAT: &str = "bag.format";
pub const ROBOT_BAG_TOPIC: &str = "topic";
pub const ROBOT_BAG_REPLAY_INTERVAL_MS: &str = "replay.interval.ms";
pub const ROBOT_BAG_LOOP: &str = "loop";
pub const ROBOT_BAG_PCD_EMIT_MODE: &str = "pcd.emit.mode";

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

// ── S3 ────────────────────────────────────────────────────────────────────

pub const S3_BUCKET: &str = "s3.bucket";
pub const S3_REGION: &str = "s3.region";
pub const S3_ENDPOINT: &str = "s3.endpoint";
pub const S3_ACCESS_KEY_ID: &str = "s3.access_key_id";
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret_access_key";
pub const S3_SESSION_TOKEN: &str = "s3.session_token";

// ── Delta Lake commit coordination (S3-compatible backends) ───────────────
//
// `delta.commit.strategy` selects how concurrent-writer safety is enforced
// when committing to `_delta_log` on S3-compatible object stores:
//
//   - `single-writer` (default): rely on Function-Stream's pipeline scheduler
//     that guarantees a single writer per Delta table. Commits use direct
//     PUT and are atomic per object. Suitable for AWS S3, MinIO, R2, GCS-S3.
//   - `dynamodb`: use deltalake-aws DynamoDB lock client for multi-writer
//     safety. Requires `delta.dynamodb.table` and AWS credentials/region.
//
pub const DELTA_COMMIT_STRATEGY: &str = "delta.commit.strategy";
pub const DELTA_DYNAMODB_TABLE: &str = "delta.dynamodb.table";
pub const DELTA_DYNAMODB_REGION: &str = "delta.dynamodb.region";
pub const DELTA_DYNAMODB_ENDPOINT: &str = "delta.dynamodb.endpoint";
pub const DELTA_DYNAMODB_ACCESS_KEY_ID: &str = "delta.dynamodb.access_key_id";
pub const DELTA_DYNAMODB_SECRET_ACCESS_KEY: &str = "delta.dynamodb.secret_access_key";

// ── Protobuf ────────────────────────────────────────────────────────────────

pub const PROTOBUF_INTO_UNSTRUCTURED_JSON: &str = "protobuf.into_unstructured_json";
pub const PROTOBUF_MESSAGE_NAME: &str = "protobuf.message_name";
pub const PROTOBUF_CONFLUENT_SCHEMA_REGISTRY: &str = "protobuf.confluent_schema_registry";
pub const PROTOBUF_LENGTH_DELIMITED: &str = "protobuf.length_delimited";

// ── Framing ─────────────────────────────────────────────────────────────────

pub const FRAMING_METHOD: &str = "framing.method";
pub const FRAMING_MAX_LINE_LENGTH: &str = "framing.max_line_length";

pub const FORMAT_DEBEZIUM_FLAG: &str = "format.debezium";

// ── Streaming runtime common options ───────────────────────────────────────

pub const CHECKPOINT_INTERVAL_MS: &str = "checkpoint.interval.ms";
pub const PIPELINE_PARALLELISM: &str = "pipeline.parallelism";
pub const KEY_BY_PARALLELISM: &str = "key_by.parallelism";
pub const OPERATOR_MEMORY_BYTES: &str = "operator.memory.bytes";
pub const SINK_MEMORY_BYTES: &str = "sink.memory.bytes";
