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

//! Shared core types and constants for FunctionStream (`crate::sql::common`).
//!
//! Used by the runtime, SQL planner, coordinator, and other subsystems —
//! analogous to `arroyo-types` + `arroyo-rpc` in Arroyo.

pub mod arrow_ext;
pub mod connector_options;
pub mod with_option_keys;
pub mod constants;
pub mod control;
pub mod date;
pub mod debezium;
pub mod fs_schema;
pub mod errors;
pub mod format_from_opts;
pub mod formats;
pub mod hash;
pub mod kafka_catalog;
pub mod message;
pub mod operator_config;
pub mod task_info;
pub mod time_utils;
pub mod worker;
pub mod converter;

// ── Re-exports from existing modules ──
pub use arrow_ext::{DisplayAsSql, FsExtensionType, GetArrowSchema, GetArrowType};
pub use date::{DatePart, DateTruncPrecision};
pub use debezium::{Debezium, DebeziumOp, UpdatingData};
pub use hash::{range_for_server, server_for_hash, HASH_SEEDS};
pub use message::{ArrowMessage, CheckpointBarrier, SignalMessage, Watermark};
pub use task_info::{ChainInfo, TaskInfo};
pub use time_utils::{from_micros, from_millis, from_nanos, to_micros, to_millis, to_nanos};
pub use worker::{MachineId, WorkerId};

// ── Re-exports from new modules ──
pub use control::{
    CheckpointCompleted, CheckpointEvent, CompactionResult, ControlMessage, ControlResp,
    ErrorDomain, RetryHint, StopMode, TableConfig, TaskCheckpointEventType, TaskError,
};
pub use fs_schema::{FsSchema, FsSchemaRef};
pub use connector_options::{ConnectorOptions, FromOpts};
pub use kafka_catalog::{
    KafkaConfig, KafkaConfigAuthentication, KafkaTable, KafkaTableSourceOffset, ReadMode,
    SchemaRegistryConfig, SinkCommitMode, TableType,
};
pub use errors::{DataflowError, DataflowResult};
pub use formats::{BadData, Format, Framing, JsonCompression, JsonFormat};
pub use operator_config::MetadataField;

// ── Well-known column names ──
pub use constants::sql_field::{TIMESTAMP_FIELD, UPDATING_META_FIELD};

// ── Environment variables ──
pub const JOB_ID_ENV: &str = "JOB_ID";
pub const RUN_ID_ENV: &str = "RUN_ID";

// ── Metric names ──
pub const MESSAGES_RECV: &str = "fs_worker_messages_recv";
pub const MESSAGES_SENT: &str = "fs_worker_messages_sent";
pub const BYTES_RECV: &str = "fs_worker_bytes_recv";
pub const BYTES_SENT: &str = "fs_worker_bytes_sent";
pub const BATCHES_RECV: &str = "fs_worker_batches_recv";
pub const BATCHES_SENT: &str = "fs_worker_batches_sent";
pub const TX_QUEUE_SIZE: &str = "fs_worker_tx_queue_size";
pub const TX_QUEUE_REM: &str = "fs_worker_tx_queue_rem";
pub const DESERIALIZATION_ERRORS: &str = "fs_worker_deserialization_errors";

pub const LOOKUP_KEY_INDEX_FIELD: &str = "__lookup_key_index";
