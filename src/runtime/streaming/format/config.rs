use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimestampFormat {
    RFC3339,
    UnixMillis,
    UnixSeconds,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DecimalEncoding {
    String,
    Number,
    Bytes,
}

/// 数据容错策略
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BadDataPolicy {
    /// 遇到脏数据直接报错，导致算子 Panic 和重启
    Fail,
    /// 丢弃脏数据，并记录监控 Metrics
    Drop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonFormat {
    pub timestamp_format: TimestampFormat,
    pub decimal_encoding: DecimalEncoding,
    pub include_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Format {
    Json(JsonFormat),
    RawString,
    RawBytes,
}
