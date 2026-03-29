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
    /// 异步 UDF 重写后的结果列（与历史 `extensions::constants` 对齐）。
    pub const ASYNC_RESULT: &str = "__async_result";
    pub const DEFAULT_KEY_LABEL: &str = "key";
    pub const DEFAULT_PROJECTION_LABEL: &str = "projection";
}

// ── 连接器类型短名（工厂注册等）──────────────────────────────────────────────

pub mod connector_type {
    pub const KAFKA: &str = "kafka";
    pub const REDIS: &str = "redis";
}
