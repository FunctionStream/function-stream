//! Parse `WITH` clause format / framing / bad-data options (Arroyo-compatible keys).

use std::str::FromStr;

use datafusion::common::{Result as DFResult, plan_datafusion_err, plan_err};

use super::connector_options::ConnectorOptions;
use super::formats::{
    AvroFormat, BadData, DecimalEncoding, Format, Framing, JsonCompression, JsonFormat,
    NewlineDelimitedFraming, ParquetCompression, ParquetFormat, ProtobufFormat, RawBytesFormat,
    RawStringFormat, TimestampFormat,
};

impl JsonFormat {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut j = JsonFormat::default();
        if let Some(v) = opts.pull_opt_bool("json.confluent_schema_registry")? {
            j.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_u64("json.confluent_schema_version")? {
            j.schema_id = Some(v as u32);
        }
        if let Some(v) = opts.pull_opt_bool("json.include_schema")? {
            j.include_schema = v;
        }
        if let Some(v) = opts.pull_opt_bool("json.debezium")? {
            j.debezium = v;
        }
        if let Some(v) = opts.pull_opt_bool("json.unstructured")? {
            j.unstructured = v;
        }
        if let Some(s) = opts.pull_opt_str("json.timestamp_format")? {
            j.timestamp_format = TimestampFormat::try_from(s.as_str()).map_err(|_| {
                plan_datafusion_err!("invalid json.timestamp_format '{}'", s)
            })?;
        }
        if let Some(s) = opts.pull_opt_str("json.decimal_encoding")? {
            j.decimal_encoding = DecimalEncoding::try_from(s.as_str()).map_err(|_| {
                plan_datafusion_err!("invalid json.decimal_encoding '{s}'")
            })?;
        }
        if let Some(s) = opts.pull_opt_str("json.compression")? {
            j.compression = JsonCompression::from_str(&s)
                .map_err(|e| plan_datafusion_err!("invalid json.compression: {e}"))?;
        }
        Ok(j)
    }
}

impl Format {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let Some(name) = opts.pull_opt_str("format")? else {
            return Ok(None);
        };
        match name.to_lowercase().as_str() {
            "json" => Ok(Some(Format::Json(JsonFormat::from_opts(opts)?))),
            "debezium_json" => {
                let mut j = JsonFormat::from_opts(opts)?;
                j.debezium = true;
                Ok(Some(Format::Json(j)))
            }
            "avro" => Ok(Some(Format::Avro(AvroFormat::from_opts(opts)?))),
            "parquet" => Ok(Some(Format::Parquet(ParquetFormat::from_opts(opts)?))),
            "protobuf" => Ok(Some(Format::Protobuf(ProtobufFormat::from_opts(opts)?))),
            "raw_string" => Ok(Some(Format::RawString(RawStringFormat {}))),
            "raw_bytes" => Ok(Some(Format::RawBytes(RawBytesFormat {}))),
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
        if let Some(v) = opts.pull_opt_bool("avro.confluent_schema_registry")? {
            a.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_bool("avro.raw_datums")? {
            a.raw_datums = v;
        }
        if let Some(v) = opts.pull_opt_bool("avro.into_unstructured_json")? {
            a.into_unstructured_json = v;
        }
        if let Some(v) = opts.pull_opt_u64("avro.schema_id")? {
            a.schema_id = Some(v as u32);
        }
        Ok(a)
    }
}

impl ParquetFormat {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let mut p = ParquetFormat::default();
        if let Some(s) = opts.pull_opt_str("parquet.compression")? {
            p.compression = ParquetCompression::from_str(&s)
                .map_err(|e| plan_datafusion_err!("invalid parquet.compression: {e}"))?;
        }
        if let Some(v) = opts.pull_opt_u64("parquet.row_group_bytes")? {
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
        if let Some(v) = opts.pull_opt_bool("protobuf.into_unstructured_json")? {
            p.into_unstructured_json = v;
        }
        if let Some(s) = opts.pull_opt_str("protobuf.message_name")? {
            p.message_name = Some(s);
        }
        if let Some(v) = opts.pull_opt_bool("protobuf.confluent_schema_registry")? {
            p.confluent_schema_registry = v;
        }
        if let Some(v) = opts.pull_opt_bool("protobuf.length_delimited")? {
            p.length_delimited = v;
        }
        Ok(p)
    }
}

impl Framing {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let method = opts.pull_opt_str("framing.method")?;
        match method.as_deref() {
            None => Ok(None),
            Some("newline") | Some("newline_delimited") => {
                let max = opts.pull_opt_u64("framing.max_line_length")?;
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
        let Some(s) = opts.pull_opt_str("bad_data")? else {
            return Ok(BadData::Fail {});
        };
        match s.to_lowercase().as_str() {
            "fail" => Ok(BadData::Fail {}),
            "drop" => Ok(BadData::Drop {}),
            _ => plan_err!("invalid bad_data '{s}'"),
        }
    }
}
