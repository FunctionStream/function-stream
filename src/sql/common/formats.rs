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

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    #[default]
    #[serde(rename = "rfc3339")]
    RFC3339,
    UnixMillis,
}

impl TryFrom<&str> for TimestampFormat {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "RFC3339" | "rfc3339" => Ok(TimestampFormat::RFC3339),
            "UnixMillis" | "unix_millis" => Ok(TimestampFormat::UnixMillis),
            _ => Err(()),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum DecimalEncoding {
    #[default]
    Number,
    String,
    Bytes,
}

impl TryFrom<&str> for DecimalEncoding {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "number" => Ok(Self::Number),
            "string" => Ok(Self::String),
            "bytes" => Ok(Self::Bytes),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Default, Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum JsonCompression {
    #[default]
    Uncompressed,
    Gzip,
}

impl FromStr for JsonCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "uncompressed" => Ok(JsonCompression::Uncompressed),
            "gzip" => Ok(JsonCompression::Gzip),
            _ => Err(format!("invalid json compression '{s}'")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct JsonFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,
    #[serde(default, alias = "confluent_schema_version")]
    pub schema_id: Option<u32>,
    #[serde(default)]
    pub include_schema: bool,
    #[serde(default)]
    pub debezium: bool,
    #[serde(default)]
    pub unstructured: bool,
    #[serde(default)]
    pub timestamp_format: TimestampFormat,
    #[serde(default)]
    pub decimal_encoding: DecimalEncoding,
    #[serde(default)]
    pub compression: JsonCompression,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct RawStringFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct RawBytesFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct AvroFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,
    #[serde(default)]
    pub raw_datums: bool,
    #[serde(default)]
    pub into_unstructured_json: bool,
    #[serde(default)]
    pub schema_id: Option<u32>,
}

impl AvroFormat {
    pub fn new(
        confluent_schema_registry: bool,
        raw_datums: bool,
        into_unstructured_json: bool,
    ) -> Self {
        Self {
            confluent_schema_registry,
            raw_datums,
            into_unstructured_json,
            schema_id: None,
        }
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Default)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
    Gzip,
    #[default]
    Zstd,
    Lz4,
    Lz4Raw,
}

impl FromStr for ParquetCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "uncompressed" => Ok(ParquetCompression::Uncompressed),
            "snappy" => Ok(ParquetCompression::Snappy),
            "gzip" => Ok(ParquetCompression::Gzip),
            "zstd" => Ok(ParquetCompression::Zstd),
            "lz4" => Ok(ParquetCompression::Lz4),
            "lz4_raw" => Ok(ParquetCompression::Lz4Raw),
            _ => Err(format!("invalid parquet compression '{s}'")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Default)]
#[serde(rename_all = "snake_case")]
pub struct ParquetFormat {
    #[serde(default)]
    pub compression: ParquetCompression,
    #[serde(default)]
    pub row_group_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct ProtobufFormat {
    #[serde(default)]
    pub into_unstructured_json: bool,
    #[serde(default)]
    pub message_name: Option<String>,
    #[serde(default)]
    pub compiled_schema: Option<Vec<u8>>,
    #[serde(default)]
    pub confluent_schema_registry: bool,
    #[serde(default)]
    pub length_delimited: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Format {
    Json(JsonFormat),
    Avro(AvroFormat),
    Protobuf(ProtobufFormat),
    Parquet(ParquetFormat),
    RawString(RawStringFormat),
    RawBytes(RawBytesFormat),
}

impl Display for Format {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Format {
    pub fn name(&self) -> &'static str {
        match self {
            Format::Json(_) => "json",
            Format::Avro(_) => "avro",
            Format::Protobuf(_) => "protobuf",
            Format::Parquet(_) => "parquet",
            Format::RawString(_) => "raw_string",
            Format::RawBytes(_) => "raw_bytes",
        }
    }

    pub fn is_updating(&self) -> bool {
        matches!(self, Format::Json(JsonFormat { debezium: true, .. }))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case", tag = "behavior")]
pub enum BadData {
    Fail {},
    Drop {},
}

impl Default for BadData {
    fn default() -> Self {
        BadData::Fail {}
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case", tag = "method")]
pub enum Framing {
    Newline(NewlineDelimitedFraming),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct NewlineDelimitedFraming {
    pub max_line_length: Option<u64>,
}
