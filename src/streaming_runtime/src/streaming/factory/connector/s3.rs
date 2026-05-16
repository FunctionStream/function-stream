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
    apply_common_sink_fields, normalized_props,
};
use crate::streaming::factory::global::Registry;
use crate::streaming::factory::operator_constructor::OperatorConstructor;
use crate::streaming::operators::sink::s3::{S3Format, S3SinkOperator, compression_from_str};

pub struct S3SinkDispatcher;

impl OperatorConstructor for S3SinkDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload).context("failed to decode connector op")?;
        let props = match op.config {
            Some(Config::S3Sink(cfg)) => s3_props(cfg),
            _ => bail!("s3 sink expects S3SinkConfig"),
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
            .unwrap_or_default();
        let compression =
            compression_from_str(props.get(opt::PARQUET_COMPRESSION).map(String::as_str))?;
        let format = match format.as_str() {
            connection_format_value::CSV => S3Format::Csv,
            connection_format_value::PARQUET => S3Format::Parquet,
            other => bail!("unsupported s3 sink format '{other}'"),
        };
        Ok(ConstructedOperator::Operator(Box::new(
            S3SinkOperator::try_new(op.name, path, format, compression, props)?,
        )))
    }
}

fn s3_props(cfg: protocol::function_stream_graph::S3SinkConfig) -> HashMap<String, String> {
    let mut props = cfg.extra_properties;
    props.extend(cfg.runtime_properties);
    apply_common_sink_fields(&mut props, cfg.path, cfg.format, cfg.parquet_compression);
    if !cfg.bucket.is_empty() {
        props.insert(opt::S3_BUCKET.to_string(), cfg.bucket);
    }
    if !cfg.region.is_empty() {
        props.insert(opt::S3_REGION.to_string(), cfg.region);
    }
    if let Some(v) = cfg.endpoint {
        props.insert(opt::S3_ENDPOINT.to_string(), v);
    }
    if let Some(v) = cfg.access_key_id {
        props.insert(opt::S3_ACCESS_KEY_ID.to_string(), v);
    }
    if let Some(v) = cfg.secret_access_key {
        props.insert(opt::S3_SECRET_ACCESS_KEY.to_string(), v);
    }
    if let Some(v) = cfg.session_token {
        props.insert(opt::S3_SESSION_TOKEN.to_string(), v);
    }
    props
}
