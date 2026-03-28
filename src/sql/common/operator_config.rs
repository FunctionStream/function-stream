use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::formats::{BadData, Format, Framing};
use super::fs_schema::FsSchema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub messages_per_second: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataField {
    pub field_name: String,
    pub key: String,
    /// JSON-encoded Arrow DataType string, e.g. `"Utf8"`, `"Int64"`.
    #[serde(default)]
    pub data_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorConfig {
    pub connection: Value,
    pub table: Value,
    pub format: Option<Format>,
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub metadata_fields: Vec<MetadataField>,
    /// Arrow 行 schema（Kafka Source/Sink 反序列化、序列化必需）。
    #[serde(default)]
    pub input_schema: Option<FsSchema>,
}
