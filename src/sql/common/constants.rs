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

//! SQL / 流算子相关的**名称与标识符常量**（标量函数名、窗口 TVF、逻辑扩展节点名、CDC 字段、
//! 运行时 blueprint 字符串、`OperatorName` 特性标签等）；与 [`super::with_option_keys`]（WITH 选项键）分工。

// ── 内置标量 UDF（`register_all` / `ScalarUDFImpl::name`）──────────────────────

pub mod scalar_fn {
    pub const GET_FIRST_JSON_OBJECT: &str = "get_first_json_object";
    pub const EXTRACT_JSON: &str = "extract_json";
    pub const EXTRACT_JSON_STRING: &str = "extract_json_string";
    pub const SERIALIZE_JSON_UNION: &str = "serialize_json_union";
    pub const MULTI_HASH: &str = "multi_hash";
}

// ── 窗口 TVF（`hop` / `tumble` / `session` 等，与 DataFusion 解析一致）──────────

pub mod window_fn {
    pub const HOP: &str = "hop";
    pub const TUMBLE: &str = "tumble";
    pub const SESSION: &str = "session";
}

// ── 流规划期占位标量 UDF（`StreamPlanningContextBuilder::with_streaming_extensions`）──

pub mod planning_placeholder_udf {
    pub const UNNEST: &str = "unnest";
    pub const ROW_TIME: &str = "row_time";
    /// `List` 内元素字段名，仅用于占位签名的 Arrow 形态
    pub const LIST_ELEMENT_FIELD: &str = "field";
}

// ── `OperatorName` 在指标 / 特性集合中使用的 kebab-case 标签 ─────────────────

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

// ── 逻辑计划扩展节点的 `UserDefinedLogicalNodeCore::name` / 类型字符串 ────────

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

// ── gRPC / proto 算子配置里的 `name` 字段（与 `OperatorName` 展示相关）──────────

pub mod proto_operator_name {
    pub const TUMBLING_WINDOW: &str = "TumblingWindow";
    pub const UPDATING_AGGREGATE: &str = "UpdatingAggregate";
    pub const WINDOW_FUNCTION: &str = "WindowFunction";
    /// 滑动窗口 human-readable 描述片段（非固定 id）
    pub const SLIDING_WINDOW_LABEL: &str = "sliding window";
    pub const INSTANT_WINDOW: &str = "InstantWindow";
    pub const INSTANT_WINDOW_LABEL: &str = "instant window";
}

// ── 下发到运行时的 blueprint / 算子种类字符串 ──────────────────────────────────

pub mod runtime_operator_kind {
    pub const STREAMING_JOIN: &str = "streaming_join";
    pub const WATERMARK_GENERATOR: &str = "watermark_generator";
    pub const STREAMING_WINDOW_EVALUATOR: &str = "streaming_window_evaluator";
}

// ── Debezium CDC 信封字段 ───────────────────────────────────────────────────

pub mod cdc {
    pub const BEFORE: &str = "before";
    pub const AFTER: &str = "after";
    pub const OP: &str = "op";
}

// ── updating aggregate 状态元数据 struct 字段 ────────────────────────────────

pub mod updating_state_field {
    pub const IS_RETRACT: &str = "is_retract";
    pub const ID: &str = "id";
}

// ── 计划里常用的列名 / 别名 ───────────────────────────────────────────────────

pub mod sql_field {
    /// 异步 UDF 重写后的结果列名。
    pub const ASYNC_RESULT: &str = "__async_result";
    pub const DEFAULT_KEY_LABEL: &str = "key";
    pub const DEFAULT_PROJECTION_LABEL: &str = "projection";
    /// `WATERMARK FOR … AS expr` 生成的计算列名（与 `TemporalPipelineConfig` 一致）。
    pub const COMPUTED_WATERMARK: &str = "__watermark";
    /// 流表事件时间物理列名（与 DataFusion 计划注入列一致）。
    pub const TIMESTAMP_FIELD: &str = "_timestamp";
    /// Changelog / updating 模式下的元数据列名。
    pub const UPDATING_META_FIELD: &str = "_updating_meta";
}

// ── `SqlConfig` / `PlanningOptions` 默认值 ────────────────────────────────────

pub mod sql_planning_default {
    pub const DEFAULT_PARALLELISM: usize = 4;
    /// [`PlanningOptions::default`] 的 TTL（秒）：24h。
    pub const PLANNING_TTL_SECS: u64 = 24 * 60 * 60;
}

// ── `ConnectorOptions` / WITH 解析用到的字面量 ────────────────────────────────

/// 单引号字符串形式的布尔取值（见 [`super::connector_options::ConnectorOptions::pull_opt_bool`]）。
pub mod with_opt_bool_str {
    pub const TRUE: &str = "true";
    pub const YES: &str = "yes";
    pub const FALSE: &str = "false";
    pub const NO: &str = "no";
}

/// `INTERVAL '…'` / 间隔字符串解析中的单位 token（小写；解析前会对单位做 `to_lowercase`）。
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

// ── `format` / `framing.method` / `bad_data` 的 WITH 取值（见 `format_from_opts`）──────

/// `format = '…'` 的名称（小写；`Format::from_opts` 会对值做 `to_lowercase`）。
pub mod connection_format_value {
    pub const JSON: &str = "json";
    pub const DEBEZIUM_JSON: &str = "debezium_json";
    pub const AVRO: &str = "avro";
    pub const PARQUET: &str = "parquet";
    pub const PROTOBUF: &str = "protobuf";
    pub const RAW_STRING: &str = "raw_string";
    pub const RAW_BYTES: &str = "raw_bytes";
}

/// `framing.method` 合法取值（与 `Framing::from_opts` 一致；当前不做大小写折叠）。
pub mod framing_method_value {
    pub const NEWLINE: &str = "newline";
    pub const NEWLINE_DELIMITED: &str = "newline_delimited";
}

/// `bad_data = '…'`（小写；解析前 `to_lowercase`）。
pub mod bad_data_value {
    pub const FAIL: &str = "fail";
    pub const DROP: &str = "drop";
}

// ── `formats.rs` 里枚举的 wire 名（与 serde `snake_case` / `TryFrom` / `FromStr` 一致）────

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

// ── `date_part` / `date_trunc` SQL 关键字（小写；解析前对输入做 `to_lowercase`）────────

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

// ── `logical_planner/mod.rs` 物理计划与 Debezium 流水线 ───────────────────────

/// `FsMemExec` / codec 里表示 join 左右输入的 `table_name`。
pub mod mem_exec_join_side {
    pub const LEFT: &str = "left";
    pub const RIGHT: &str = "right";
}

/// 自定义 `ExecutionPlan::name()`（与 DataFusion explain / 调试一致）。
pub mod physical_plan_node_name {
    pub const RW_LOCK_READER: &str = "rw_lock_reader";
    pub const UNBOUNDED_READER: &str = "unbounded_reader";
    pub const VEC_READER: &str = "vec_reader";
    pub const MEM_EXEC: &str = "mem_exec";
    pub const DEBEZIUM_UNROLLING_EXEC: &str = "debezium_unrolling_exec";
    pub const TO_DEBEZIUM_EXEC: &str = "to_debezium_exec";
}

/// 流式 `window(start, end)` 标量 UDF 的注册名。
pub mod window_function_udf {
    pub const NAME: &str = "window";
}

/// `window()` UDF 返回 struct 的字段名（与 `window_arrow_struct` 一致）。
pub mod window_interval_field {
    pub const START: &str = "start";
    pub const END: &str = "end";
}

/// Debezium `op` 列中的单字母取值（unroll / pack 路径）。
pub mod debezium_op_short {
    pub const CREATE: &str = "c";
    pub const READ: &str = "r";
    pub const UPDATE: &str = "u";
    pub const DELETE: &str = "d";
}

// ── 连接器类型短名（工厂注册等）──────────────────────────────────────────────

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

// ── 连接表 `WITH type = 'source'|'sink'|'lookup'`（`SourceTable::from_options` / `deduce_role`）──

pub mod connection_table_role {
    pub const SOURCE: &str = "source";
    pub const SINK: &str = "sink";
    /// 与虚拟 `lookup` 连接器短名相同（亦在 [`SUPPORTED_CONNECTOR_ADAPTERS`] 中）。
    pub const LOOKUP: &str = "lookup";
}

/// [`crate::sql::schema::table_role::validate_adapter_availability`] 白名单（与 SQL `connector = '…'` 短名一致）。
pub const SUPPORTED_CONNECTOR_ADAPTERS: &[&str] = &[
    connector_type::KAFKA,
];

// ── Kafka 连接器 WITH 选项取值（`wire_kafka_operator_config`）────────────────

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
