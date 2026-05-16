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

use datafusion::common::{DataFusionError, Result, plan_err};
use protocol::function_stream_graph::{ParquetCompressionProto, SinkFormatProto};

use crate::common::Format;
use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::parquet_compression_value;
use crate::common::with_option_keys as opt;

pub struct SinkUtils;

impl SinkUtils {
    pub fn require_path(options: &mut ConnectorOptions) -> Result<String> {
        if let Some(v) = options.pull_opt_str(opt::PATH)? {
            return Ok(v);
        }
        if let Some(v) = options.pull_opt_str(opt::SINK_PATH)? {
            return Ok(v);
        }
        plan_err!("Missing required WITH option 'path' (or 'sink.path')")
    }

    pub fn extract_parquet_compression(options: &mut ConnectorOptions) -> Result<Option<i32>> {
        let Some(v) = options.pull_opt_str(opt::PARQUET_COMPRESSION)? else {
            return Ok(None);
        };
        let parsed = match v.to_ascii_lowercase().as_str() {
            parquet_compression_value::UNCOMPRESSED => {
                ParquetCompressionProto::ParquetCompressionUncompressed
            }
            parquet_compression_value::SNAPPY => ParquetCompressionProto::ParquetCompressionSnappy,
            parquet_compression_value::GZIP => ParquetCompressionProto::ParquetCompressionGzip,
            parquet_compression_value::ZSTD => ParquetCompressionProto::ParquetCompressionZstd,
            parquet_compression_value::LZ4 => ParquetCompressionProto::ParquetCompressionLz4,
            parquet_compression_value::LZ4_RAW => ParquetCompressionProto::ParquetCompressionLz4Raw,
            other => return plan_err!("Unsupported parquet.compression '{other}'"),
        };
        Ok(Some(parsed as i32))
    }

    pub fn require_str(
        options: &mut ConnectorOptions,
        key: &str,
        connector: &str,
    ) -> Result<String> {
        options.pull_opt_str(key)?.ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Connector '{connector}' requires WITH option '{key}'"
            ))
        })
    }

    pub fn resolve_sink_format(
        format: &Option<Format>,
        connector_name: &str,
        supported_formats: &[SinkFormatProto],
    ) -> Result<i32> {
        let proto_format = match format {
            Some(Format::Csv(_)) => SinkFormatProto::SinkFormatCsv,
            Some(Format::Json(_)) => SinkFormatProto::SinkFormatJsonl,
            Some(Format::Avro(_)) => SinkFormatProto::SinkFormatAvro,
            Some(Format::Parquet(_)) => SinkFormatProto::SinkFormatParquet,
            Some(Format::Lance(_)) => SinkFormatProto::SinkFormatLance,
            Some(f) => {
                return plan_err!("Format '{f:?}' cannot be mapped to a sink format");
            }
            None => {
                return plan_err!("Connector '{connector_name}' requires a format to be specified");
            }
        };

        if !supported_formats.contains(&proto_format) {
            return plan_err!(
                "Format {proto_format:?} is not supported by connector '{connector_name}'"
            );
        }

        Ok(proto_format as i32)
    }
}
