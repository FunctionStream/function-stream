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

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use prost::Message;
use protocol::function_stream_graph::ConnectorOp;

use crate::sql::common::constants::connector_type;
use crate::streaming::api::operator::ConstructedOperator;
use crate::streaming::factory::global::Registry;
use crate::streaming::factory::operator_constructor::OperatorConstructor;

use super::{
    DeltaSinkDispatcher, FilesystemSinkDispatcher, IcebergSinkDispatcher, LanceDbSinkDispatcher,
    S3SinkDispatcher, kafka::KafkaConnectorDispatcher,
};

pub struct ConnectorSourceDispatcher;

impl OperatorConstructor for ConnectorSourceDispatcher {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(config)
            .context("failed decoding connector op for source dispatch")?;
        match op.connector.to_ascii_lowercase().as_str() {
            connector_type::KAFKA => KafkaConnectorDispatcher.with_config(config, registry),
            _ => bail!("unsupported source connector '{}'", op.connector),
        }
    }
}

pub struct ConnectorSinkDispatcher;

impl OperatorConstructor for ConnectorSinkDispatcher {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(config)
            .context("failed decoding connector op for sink dispatch")?;
        match op.connector.to_ascii_lowercase().as_str() {
            connector_type::KAFKA => KafkaConnectorDispatcher.with_config(config, registry),
            connector_type::FILESYSTEM => FilesystemSinkDispatcher.with_config(config, registry),
            connector_type::DELTA => DeltaSinkDispatcher.with_config(config, registry),
            connector_type::ICEBERG => IcebergSinkDispatcher.with_config(config, registry),
            connector_type::S3 => S3SinkDispatcher.with_config(config, registry),
            "lancedb" => LanceDbSinkDispatcher.with_config(config, registry),
            _ => bail!("unsupported sink connector '{}'", op.connector),
        }
    }
}
