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
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use super::constants::{
    connection_format_value, decimal_encoding_value, json_compression_value,
    parquet_compression_value, timestamp_format_value,
};

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
            timestamp_format_value::RFC3339_UPPER | timestamp_format_value::RFC3339_SNAKE => {
                Ok(TimestampFormat::RFC3339)
            }
            timestamp_format_value::UNIX_MILLIS_PASCAL
            | timestamp_format_value::UNIX_MILLIS_SNAKE => Ok(TimestampFormat::UnixMillis),
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
            decimal_encoding_value::NUMBER => Ok(Self::Number),
            decimal_encoding_value::STRING => Ok(Self::String),
            decimal_encoding_value::BYTES => Ok(Self::Bytes),
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
            json_compression_value::UNCOMPRESSED => Ok(JsonCompression::Uncompressed),
            json_compression_value::GZIP => Ok(JsonCompression::Gzip),
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
            parquet_compression_value::UNCOMPRESSED => Ok(ParquetCompression::Uncompressed),
            parquet_compression_value::SNAPPY => Ok(ParquetCompression::Snappy),
            parquet_compression_value::GZIP => Ok(ParquetCompression::Gzip),
            parquet_compression_value::ZSTD => Ok(ParquetCompression::Zstd),
            parquet_compression_value::LZ4 => Ok(ParquetCompression::Lz4),
            parquet_compression_value::LZ4_RAW => Ok(ParquetCompression::Lz4Raw),
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
            Format::Json(_) => connection_format_value::JSON,
            Format::Avro(_) => connection_format_value::AVRO,
            Format::Protobuf(_) => connection_format_value::PROTOBUF,
            Format::Parquet(_) => connection_format_value::PARQUET,
            Format::RawString(_) => connection_format_value::RAW_STRING,
            Format::RawBytes(_) => connection_format_value::RAW_BYTES,
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
