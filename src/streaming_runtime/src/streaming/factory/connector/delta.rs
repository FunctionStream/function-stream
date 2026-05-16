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
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use prost::Message;
use protocol::function_stream_graph::ConnectorOp;
use protocol::function_stream_graph::connector_op::Config;

use crate::sql::common::constants::connection_format_value;
use crate::sql::common::with_option_keys as opt;
use crate::streaming::api::operator::ConstructedOperator;
use crate::streaming::factory::connector::sink_props_codec::{
    apply_common_sink_fields, normalized_props, parse_sink_memory_bytes,
};
use crate::streaming::factory::global::Registry;
use crate::streaming::factory::operator_constructor::OperatorConstructor;
use crate::streaming::operators::sink::delta::{DeltaFormat, DeltaSinkOperator};
use crate::streaming::operators::sink::filesystem::compression_from_str;

pub struct DeltaSinkDispatcher;

impl OperatorConstructor for DeltaSinkDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload).context("failed to decode connector op")?;
        let props = match op.config {
            Some(Config::DeltaSink(cfg)) => delta_props(cfg),
            _ => bail!("delta sink expects DeltaSinkConfig"),
        };
        let props = normalized_props(props);

        let format = props
            .get(opt::FORMAT)
            .map(String::as_str)
            .unwrap_or(connection_format_value::PARQUET)
            .to_ascii_lowercase();
        let path = props
            .get(opt::PATH)
            .cloned()
            .or_else(|| props.get(opt::SINK_PATH).cloned())
            .unwrap_or_else(|| ".".to_string());
        let compression =
            compression_from_str(props.get(opt::PARQUET_COMPRESSION).map(String::as_str))?;
        let sink_memory_bytes = parse_sink_memory_bytes(&props)?;
        let format = match format.as_str() {
            connection_format_value::CSV => DeltaFormat::Csv,
            connection_format_value::PARQUET => DeltaFormat::Parquet,
            connection_format_value::JSON => DeltaFormat::JsonL,
            connection_format_value::AVRO => DeltaFormat::Avro,
            connection_format_value::ORC => DeltaFormat::Orc,
            other => bail!("unsupported delta sink format '{other}'"),
        };
        Ok(ConstructedOperator::Operator(Box::new(
            DeltaSinkOperator::try_new(
                op.name,
                path,
                format,
                compression,
                sink_memory_bytes,
                props,
            )?,
        )))
    }
}

fn delta_props(cfg: protocol::function_stream_graph::DeltaSinkConfig) -> HashMap<String, String> {
    let mut props = cfg.extra_properties;
    props.extend(cfg.runtime_properties);
    apply_common_sink_fields(&mut props, cfg.path, cfg.format, cfg.parquet_compression);
    props
}
