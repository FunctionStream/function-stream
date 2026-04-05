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

//! Parse `WITH` clause format / framing / bad-data options (Arroyo-compatible keys).

use std::str::FromStr;

use datafusion::common::{Result as DFResult, plan_datafusion_err, plan_err};

use super::connector_options::ConnectorOptions;
use super::constants::{bad_data_value, connection_format_value, framing_method_value};
use super::formats::{
    AvroFormat, BadData, DecimalEncoding, Format, Framing, JsonCompression, JsonFormat,
    NewlineDelimitedFraming, ParquetCompression, ParquetFormat, ProtobufFormat, RawBytesFormat,
    RawStringFormat, TimestampFormat,
};
use super::with_option_keys as opt;

impl JsonFormat {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut j = JsonFormat::default();
        if let Some(v) = opts.pull_opt_bool(opt::JSON_CONFLUENT_SCHEMA_REGISTRY)? {
            j.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_u64(opt::JSON_CONFLUENT_SCHEMA_VERSION)? {
            j.schema_id = Some(v as u32);
        }
        if let Some(v) = opts.pull_opt_bool(opt::JSON_INCLUDE_SCHEMA)? {
            j.include_schema = v;
        }
        if let Some(v) = opts.pull_opt_bool(opt::JSON_DEBEZIUM)? {
            j.debezium = v;
        }
        if let Some(v) = opts.pull_opt_bool(opt::JSON_UNSTRUCTURED)? {
            j.unstructured = v;
        }
        if let Some(s) = opts.pull_opt_str(opt::JSON_TIMESTAMP_FORMAT)? {
            j.timestamp_format = TimestampFormat::try_from(s.as_str())
                .map_err(|_| plan_datafusion_err!("invalid json.timestamp_format '{}'", s))?;
        }
        if let Some(s) = opts.pull_opt_str(opt::JSON_DECIMAL_ENCODING)? {
            j.decimal_encoding = DecimalEncoding::try_from(s.as_str())
                .map_err(|_| plan_datafusion_err!("invalid json.decimal_encoding '{s}'"))?;
        }
        if let Some(s) = opts.pull_opt_str(opt::JSON_COMPRESSION)? {
            j.compression = JsonCompression::from_str(&s)
                .map_err(|e| plan_datafusion_err!("invalid json.compression: {e}"))?;
        }
        Ok(j)
    }
}

impl Format {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let Some(name) = opts.pull_opt_str(opt::FORMAT)? else {
            return Ok(None);
        };
        let n = name.to_lowercase();
        match n.as_str() {
            connection_format_value::JSON => Ok(Some(Format::Json(JsonFormat::from_opts(opts)?))),
            connection_format_value::DEBEZIUM_JSON => {
                let mut j = JsonFormat::from_opts(opts)?;
                j.debezium = true;
                Ok(Some(Format::Json(j)))
            }
            connection_format_value::AVRO => Ok(Some(Format::Avro(AvroFormat::from_opts(opts)?))),
            connection_format_value::PARQUET => {
                Ok(Some(Format::Parquet(ParquetFormat::from_opts(opts)?)))
            }
            connection_format_value::PROTOBUF => {
                Ok(Some(Format::Protobuf(ProtobufFormat::from_opts(opts)?)))
            }
            connection_format_value::RAW_STRING => Ok(Some(Format::RawString(RawStringFormat {}))),
            connection_format_value::RAW_BYTES => Ok(Some(Format::RawBytes(RawBytesFormat {}))),
            _ => plan_err!("unknown format '{name}'"),
        }
    }
}

impl AvroFormat {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut a = AvroFormat {
            confluent_schema_registry: false,
            raw_datums: false,
            into_unstructured_json: false,
            schema_id: None,
        };
        if let Some(v) = opts.pull_opt_bool(opt::AVRO_CONFLUENT_SCHEMA_REGISTRY)? {
            a.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_bool(opt::AVRO_RAW_DATUMS)? {
            a.raw_datums = v;
        }
        if let Some(v) = opts.pull_opt_bool(opt::AVRO_INTO_UNSTRUCTURED_JSON)? {
            a.into_unstructured_json = v;
        }
        if let Some(v) = opts.pull_opt_u64(opt::AVRO_SCHEMA_ID)? {
            a.schema_id = Some(v as u32);
        }
        Ok(a)
    }
}

impl ParquetFormat {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut p = ParquetFormat::default();
        if let Some(s) = opts.pull_opt_str(opt::PARQUET_COMPRESSION)? {
            p.compression = ParquetCompression::from_str(&s)
                .map_err(|e| plan_datafusion_err!("invalid parquet.compression: {e}"))?;
        }
        if let Some(v) = opts.pull_opt_u64(opt::PARQUET_ROW_GROUP_BYTES)? {
            p.row_group_bytes = Some(v);
        }
        Ok(p)
    }
}

impl ProtobufFormat {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut p = ProtobufFormat {
            into_unstructured_json: false,
            message_name: None,
            compiled_schema: None,
            confluent_schema_registry: false,
            length_delimited: false,
        };
        if let Some(v) = opts.pull_opt_bool(opt::PROTOBUF_INTO_UNSTRUCTURED_JSON)? {
            p.into_unstructured_json = v;
        }
        if let Some(s) = opts.pull_opt_str(opt::PROTOBUF_MESSAGE_NAME)? {
            p.message_name = Some(s);
        }
        if let Some(v) = opts.pull_opt_bool(opt::PROTOBUF_CONFLUENT_SCHEMA_REGISTRY)? {
            p.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_bool(opt::PROTOBUF_LENGTH_DELIMITED)? {
            p.length_delimited = v;
        }
        Ok(p)
    }
}

impl Framing {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let method = opts.pull_opt_str(opt::FRAMING_METHOD)?;
        match method.as_deref() {
            None => Ok(None),
            Some(framing_method_value::NEWLINE) | Some(framing_method_value::NEWLINE_DELIMITED) => {
                let max = opts.pull_opt_u64(opt::FRAMING_MAX_LINE_LENGTH)?;
                Ok(Some(Framing::Newline(NewlineDelimitedFraming {
                    max_line_length: max,
                })))
            }
            Some(other) => plan_err!("unknown framing.method '{other}'"),
        }
    }
}

impl BadData {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let Some(s) = opts.pull_opt_str(opt::BAD_DATA)? else {
            return Ok(BadData::Fail {});
        };
        let v = s.to_lowercase();
        match v.as_str() {
            bad_data_value::FAIL => Ok(BadData::Fail {}),
            bad_data_value::DROP => Ok(BadData::Drop {}),
            _ => plan_err!("invalid bad_data '{s}'"),
        }
    }
}
