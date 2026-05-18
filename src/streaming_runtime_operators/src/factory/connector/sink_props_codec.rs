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

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use protocol::function_stream_graph::{ParquetCompressionProto, SinkFormatProto};

use crate::config::global_config::DEFAULT_SINK_BUFFER_MEMORY_BYTES;
use crate::sql::common::constants::{connection_format_value, parquet_compression_value};
use crate::sql::common::with_option_keys as opt;

pub fn normalized_props(raw: HashMap<String, String>) -> HashMap<String, String> {
    raw.into_iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), normalize_value(&v)))
        .collect()
}

pub fn apply_common_sink_fields(
    props: &mut HashMap<String, String>,
    path: String,
    format: i32,
    parquet_compression: Option<i32>,
) {
    if !path.is_empty() {
        props.insert(opt::PATH.to_string(), path);
    }
    if let Some(fmt) = SinkFormatProto::try_from(format)
        .ok()
        .and_then(sink_format_as_str)
    {
        props.insert(opt::FORMAT.to_string(), fmt.to_string());
    }
    if let Some(comp) = parquet_compression
        .and_then(|c| ParquetCompressionProto::try_from(c).ok())
        .and_then(parquet_compression_as_str)
    {
        props.insert(opt::PARQUET_COMPRESSION.to_string(), comp.to_string());
    }
}

pub fn sink_format_as_str(v: SinkFormatProto) -> Option<&'static str> {
    use SinkFormatProto as F;
    match v {
        F::SinkFormatCsv => Some(connection_format_value::CSV),
        F::SinkFormatJsonl => Some(connection_format_value::JSONL),
        F::SinkFormatAvro => Some(connection_format_value::AVRO),
        F::SinkFormatParquet => Some(connection_format_value::PARQUET),
        F::SinkFormatOrc => Some(connection_format_value::ORC),
        F::SinkFormatLance => Some(connection_format_value::LANCE),
        F::SinkFormatUnspecified => None,
    }
}

pub fn parquet_compression_as_str(v: ParquetCompressionProto) -> Option<&'static str> {
    use ParquetCompressionProto as C;
    match v {
        C::ParquetCompressionUncompressed => Some(parquet_compression_value::UNCOMPRESSED),
        C::ParquetCompressionSnappy => Some(parquet_compression_value::SNAPPY),
        C::ParquetCompressionGzip => Some(parquet_compression_value::GZIP),
        C::ParquetCompressionZstd => Some(parquet_compression_value::ZSTD),
        C::ParquetCompressionLz4 => Some(parquet_compression_value::LZ4),
        C::ParquetCompressionLz4Raw => Some(parquet_compression_value::LZ4_RAW),
        C::ParquetCompressionUnspecified => None,
    }
}

pub fn parse_sink_memory_bytes(props: &HashMap<String, String>) -> Result<u64> {
    let parsed = match props.get(opt::SINK_MEMORY_BYTES) {
        Some(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid '{}' value '{}'", opt::SINK_MEMORY_BYTES, raw))?,
        None => DEFAULT_SINK_BUFFER_MEMORY_BYTES,
    };
    if parsed == 0 {
        bail!("'{}' must be > 0", opt::SINK_MEMORY_BYTES);
    }
    Ok(parsed)
}

fn normalize_value(v: &str) -> String {
    let s = v.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}
