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

pub mod scalar_fn {
    pub const GET_FIRST_JSON_OBJECT: &str = "get_first_json_object";
    pub const EXTRACT_JSON: &str = "extract_json";
    pub const EXTRACT_JSON_STRING: &str = "extract_json_string";
    pub const SERIALIZE_JSON_UNION: &str = "serialize_json_union";
    pub const MULTI_HASH: &str = "multi_hash";
}

pub mod window_fn {
    pub const HOP: &str = "hop";
    pub const TUMBLE: &str = "tumble";
    pub const SESSION: &str = "session";
}

pub mod planning_placeholder_udf {
    pub const UNNEST: &str = "unnest";
    pub const ROW_TIME: &str = "row_time";
    pub const LIST_ELEMENT_FIELD: &str = "field";
}

pub mod operator_feature {
    pub const ASYNC_UDF: &str = "async-udf";
    pub const JOIN_WITH_EXPIRATION: &str = "join-with-expiration";
    pub const WINDOWED_JOIN: &str = "windowed-join";
    pub const SQL_WINDOW_FUNCTION: &str = "sql-window-function";
    pub const LOOKUP_JOIN: &str = "lookup-join";
    pub const SQL_TUMBLING_WINDOW_AGGREGATE: &str = "sql-tumbling-window-aggregate";
    pub const SQL_SLIDING_WINDOW_AGGREGATE: &str = "sql-sliding-window-aggregate";
    pub const SQL_SESSION_WINDOW_AGGREGATE: &str = "sql-session-window-aggregate";
    pub const SQL_UPDATING_AGGREGATE: &str = "sql-updating-aggregate";
    pub const KEY_BY_ROUTING: &str = "key-by-routing";
    pub const CONNECTOR_SOURCE: &str = "connector-source";
    pub const CONNECTOR_SINK: &str = "connector-sink";
}

pub mod extension_node {
    pub const STREAM_WINDOW_AGGREGATE: &str = "StreamWindowAggregateNode";
    pub const STREAMING_WINDOW_FUNCTION: &str = "StreamingWindowFunctionNode";
    pub const EVENT_TIME_WATERMARK: &str = "EventTimeWatermarkNode";
    pub const CONTINUOUS_AGGREGATE: &str = "ContinuousAggregateNode";
    pub const SYSTEM_TIMESTAMP_INJECTOR: &str = "SystemTimestampInjectorNode";
    pub const STREAM_INGESTION: &str = "StreamIngestionNode";
    pub const STREAM_EGRESS: &str = "StreamEgressNode";
    pub const STREAM_PROJECTION: &str = "StreamProjectionNode";
    pub const REMOTE_TABLE_BOUNDARY: &str = "RemoteTableBoundaryNode";
    pub const REFERENCE_TABLE_SOURCE: &str = "ReferenceTableSource";
    pub const STREAM_REFERENCE_JOIN: &str = "StreamReferenceJoin";
    pub const KEY_EXTRACTION: &str = "KeyExtractionNode";
    pub const STREAMING_JOIN: &str = "StreamingJoinNode";
    pub const ASYNC_FUNCTION_EXECUTION: &str = "AsyncFunctionExecutionNode";
    pub const UNROLL_DEBEZIUM_PAYLOAD: &str = "UnrollDebeziumPayloadNode";
    pub const PACK_DEBEZIUM_ENVELOPE: &str = "PackDebeziumEnvelopeNode";
}

pub mod proto_operator_name {
    pub const TUMBLING_WINDOW: &str = "TumblingWindow";
    pub const UPDATING_AGGREGATE: &str = "UpdatingAggregate";
    pub const WINDOW_FUNCTION: &str = "WindowFunction";
    pub const SLIDING_WINDOW_LABEL: &str = "sliding window";
    pub const INSTANT_WINDOW: &str = "InstantWindow";
    pub const INSTANT_WINDOW_LABEL: &str = "instant window";
}

pub mod runtime_operator_kind {
    pub const STREAMING_JOIN: &str = "streaming_join";
    pub const WATERMARK_GENERATOR: &str = "watermark_generator";
    pub const STREAMING_WINDOW_EVALUATOR: &str = "streaming_window_evaluator";
}

pub mod factory_operator_name {
    pub const CONNECTOR_SOURCE: &str = "ConnectorSource";
    pub const CONNECTOR_SINK: &str = "ConnectorSink";
    pub const KAFKA_SOURCE: &str = "KafkaSource";
    pub const KAFKA_SINK: &str = "KafkaSink";
}

pub mod cdc {
    pub const BEFORE: &str = "before";
    pub const AFTER: &str = "after";
    pub const OP: &str = "op";
}

pub mod updating_state_field {
    pub const IS_RETRACT: &str = "is_retract";
    pub const ID: &str = "id";
}

pub mod sql_field {
    pub const ASYNC_RESULT: &str = "__async_result";
    pub const DEFAULT_KEY_LABEL: &str = "key";
    pub const DEFAULT_PROJECTION_LABEL: &str = "projection";
    pub const COMPUTED_WATERMARK: &str = "__watermark";
    pub const TIMESTAMP_FIELD: &str = "_timestamp";
    pub const UPDATING_META_FIELD: &str = "_updating_meta";
}

pub mod sql_planning_default {
    pub const DEFAULT_PARALLELISM: usize = 1;
    /// Parallelism for aggregations that run after `KeyBy` / shuffle on non-empty routing keys.
    pub const KEYED_AGGREGATE_DEFAULT_PARALLELISM: usize = 8;
    pub const PLANNING_TTL_SECS: u64 = 24 * 60 * 60;
}

pub mod with_opt_bool_str {
    pub const TRUE: &str = "true";
    pub const YES: &str = "yes";
    pub const FALSE: &str = "false";
    pub const NO: &str = "no";
}

pub mod interval_duration_unit {
    pub const SECOND: &str = "second";
    pub const SECONDS: &str = "seconds";
    pub const S: &str = "s";
    pub const MINUTE: &str = "minute";
    pub const MINUTES: &str = "minutes";
    pub const MIN: &str = "min";
    pub const HOUR: &str = "hour";
    pub const HOURS: &str = "hours";
    pub const H: &str = "h";
    pub const DAY: &str = "day";
    pub const DAYS: &str = "days";
    pub const D: &str = "d";
}

pub mod connection_format_value {
    pub const JSON: &str = "json";
    pub const DEBEZIUM_JSON: &str = "debezium_json";
    pub const AVRO: &str = "avro";
    pub const PARQUET: &str = "parquet";
    pub const PROTOBUF: &str = "protobuf";
    pub const RAW_STRING: &str = "raw_string";
    pub const RAW_BYTES: &str = "raw_bytes";
}

pub mod framing_method_value {
    pub const NEWLINE: &str = "newline";
    pub const NEWLINE_DELIMITED: &str = "newline_delimited";
}

pub mod bad_data_value {
    pub const FAIL: &str = "fail";
    pub const DROP: &str = "drop";
}

pub mod timestamp_format_value {
    pub const RFC3339_SNAKE: &str = "rfc3339";
    pub const RFC3339_UPPER: &str = "RFC3339";
    pub const UNIX_MILLIS_SNAKE: &str = "unix_millis";
    pub const UNIX_MILLIS_PASCAL: &str = "UnixMillis";
}

pub mod decimal_encoding_value {
    pub const NUMBER: &str = "number";
    pub const STRING: &str = "string";
    pub const BYTES: &str = "bytes";
}

pub mod json_compression_value {
    pub const UNCOMPRESSED: &str = "uncompressed";
    pub const GZIP: &str = "gzip";
}

pub mod parquet_compression_value {
    pub const UNCOMPRESSED: &str = "uncompressed";
    pub const SNAPPY: &str = "snappy";
    pub const GZIP: &str = "gzip";
    pub const ZSTD: &str = "zstd";
    pub const LZ4: &str = "lz4";
    pub const LZ4_RAW: &str = "lz4_raw";
}

pub mod date_part_keyword {
    pub const YEAR: &str = "year";
    pub const MONTH: &str = "month";
    pub const WEEK: &str = "week";
    pub const DAY: &str = "day";
    pub const HOUR: &str = "hour";
    pub const MINUTE: &str = "minute";
    pub const SECOND: &str = "second";
    pub const MILLISECOND: &str = "millisecond";
    pub const MICROSECOND: &str = "microsecond";
    pub const NANOSECOND: &str = "nanosecond";
    pub const DOW: &str = "dow";
    pub const DOY: &str = "doy";
}

pub mod date_trunc_keyword {
    pub const YEAR: &str = "year";
    pub const QUARTER: &str = "quarter";
    pub const MONTH: &str = "month";
    pub const WEEK: &str = "week";
    pub const DAY: &str = "day";
    pub const HOUR: &str = "hour";
    pub const MINUTE: &str = "minute";
    pub const SECOND: &str = "second";
}

pub mod mem_exec_join_side {
    pub const LEFT: &str = "left";
    pub const RIGHT: &str = "right";
}

pub mod physical_plan_node_name {
    pub const RW_LOCK_READER: &str = "rw_lock_reader";
    pub const UNBOUNDED_READER: &str = "unbounded_reader";
    pub const VEC_READER: &str = "vec_reader";
    pub const MEM_EXEC: &str = "mem_exec";
    pub const DEBEZIUM_UNROLLING_EXEC: &str = "debezium_unrolling_exec";
    pub const TO_DEBEZIUM_EXEC: &str = "to_debezium_exec";
}

pub mod window_function_udf {
    pub const NAME: &str = "window";
}

pub mod window_interval_field {
    pub const START: &str = "start";
    pub const END: &str = "end";
}

pub mod debezium_op_short {
    pub const CREATE: &str = "c";
    pub const READ: &str = "r";
    pub const UPDATE: &str = "u";
    pub const DELETE: &str = "d";
}

pub mod connector_type {
    pub const KAFKA: &str = "kafka";
    pub const KINESIS: &str = "kinesis";
    pub const FILESYSTEM: &str = "filesystem";
    pub const DELTA: &str = "delta";
    pub const ICEBERG: &str = "iceberg";
    pub const PULSAR: &str = "pulsar";
    pub const NATS: &str = "nats";
    pub const REDIS: &str = "redis";
    pub const MQTT: &str = "mqtt";
    pub const WEBSOCKET: &str = "websocket";
    pub const SSE: &str = "sse";
    pub const NEXMARK: &str = "nexmark";
    pub const BLACKHOLE: &str = "blackhole";
    pub const MEMORY: &str = "memory";
    pub const POSTGRES: &str = "postgres";
}

pub mod connection_table_role {
    pub const SOURCE: &str = "source";
    pub const SINK: &str = "sink";
    pub const LOOKUP: &str = "lookup";
}

pub const SUPPORTED_CONNECTOR_ADAPTERS: &[&str] = &[connector_type::KAFKA];

pub mod kafka_with_value {
    pub const SCAN_LATEST: &str = "latest";
    pub const SCAN_EARLIEST: &str = "earliest";
    pub const SCAN_GROUP_OFFSETS: &str = "group-offsets";
    pub const SCAN_GROUP: &str = "group";
    pub const ISOLATION_READ_COMMITTED: &str = "read_committed";
    pub const ISOLATION_READ_UNCOMMITTED: &str = "read_uncommitted";
    pub const SINK_COMMIT_EXACTLY_ONCE_HYPHEN: &str = "exactly-once";
    pub const SINK_COMMIT_EXACTLY_ONCE_UNDERSCORE: &str = "exactly_once";
    pub const SINK_COMMIT_AT_LEAST_ONCE_HYPHEN: &str = "at-least-once";
    pub const SINK_COMMIT_AT_LEAST_ONCE_UNDERSCORE: &str = "at_least_once";
}
