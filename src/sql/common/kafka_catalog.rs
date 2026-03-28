//! Kafka 表级与连接级配置（与 JSON Schema / Catalog 对齐）。
//!
//! 放在 [`crate::sql::common`] 而非 `runtime::streaming`，以便 **SQL 规划、Coordinator、连接配置存储**
//! 与 **运行时工厂**（如 `ConnectorSourceDispatcher`）共用同一套类型，避免循环依赖。
//!
//! 与 [`crate::runtime::streaming::api::source::SourceOffset`] 语义相同但独立定义，运行时可用 `From`/`match` 做映射。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── KafkaTable：单表 Source/Sink ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KafkaTable {
    pub topic: String,
    /// Source / Sink 判别及各自字段；与顶层 JSON 扁平字段共用 `type` 标签。
    #[serde(flatten)]
    pub kind: TableType,
    #[serde(default)]
    pub client_configs: HashMap<String, String>,
    pub value_subject: Option<String>,
}

impl KafkaTable {
    /// Schema Registry subject；未配置时与常见约定一致：`{topic}-value`。
    pub fn subject(&self) -> String {
        self.value_subject
            .clone()
            .unwrap_or_else(|| format!("{}-value", self.topic))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableType {
    Source {
        offset: KafkaTableSourceOffset,
        read_mode: Option<ReadMode>,
        group_id: Option<String>,
        group_id_prefix: Option<String>,
    },
    Sink {
        commit_mode: SinkCommitMode,
        key_field: Option<String>,
        timestamp_field: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaTableSourceOffset {
    Latest,
    Earliest,
    #[default]
    Group,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkCommitMode {
    #[default]
    AtLeastOnce,
    ExactlyOnce,
}

// ── KafkaConfig：集群 / 鉴权 / Schema Registry ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default)]
    pub authentication: KafkaConfigAuthentication,
    #[serde(default)]
    pub schema_registry_enum: Option<SchemaRegistryConfig>,
    #[serde(default)]
    pub connection_properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KafkaConfigAuthentication {
    #[serde(rename = "None")]
    None,
    #[serde(rename = "AWS_MSK_IAM")]
    AwsMskIam { region: String },
    #[serde(rename = "SASL")]
    Sasl {
        protocol: String,
        mechanism: String,
        username: String,
        password: String,
    },
}

impl Default for KafkaConfigAuthentication {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SchemaRegistryConfig {
    #[serde(rename = "None")]
    None,
    #[serde(rename = "Confluent Schema Registry")]
    ConfluentSchemaRegistry {
        endpoint: String,
        #[serde(rename = "apiKey")]
        api_key: Option<String>,
        #[serde(rename = "apiSecret")]
        api_secret: Option<String>,
    },
}
