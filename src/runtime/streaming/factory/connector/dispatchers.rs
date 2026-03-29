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

//! Source / Sink 连接器协议：按 [`ConnectorOp::connector`] 分发到具体实现。

use anyhow::{anyhow, Result};
use prost::Message;
use std::sync::Arc;

use protocol::grpc::api::ConnectorOp;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::factory::operator_constructor::OperatorConstructor;
use crate::sql::common::constants::connector_type;

use super::kafka::{KafkaSinkDispatcher, KafkaSourceDispatcher};

pub struct ConnectorSourceDispatcher;

impl OperatorConstructor for ConnectorSourceDispatcher {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(config)
            .map_err(|e| anyhow!("decode ConnectorOp (source): {e}"))?;

        match op.connector.as_str() {
            ct if ct == connector_type::KAFKA => KafkaSourceDispatcher.with_config(config, registry),
            ct if ct == connector_type::REDIS => Err(anyhow!(
                "ConnectorSource '{}' factory wiring not yet implemented",
                op.connector
            )),
            other => Err(anyhow!("Unsupported source connector type: {}", other)),
        }
    }
}

pub struct ConnectorSinkDispatcher;

impl OperatorConstructor for ConnectorSinkDispatcher {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(config)
            .map_err(|e| anyhow!("decode ConnectorOp (sink): {e}"))?;

        match op.connector.as_str() {
            ct if ct == connector_type::KAFKA => KafkaSinkDispatcher.with_config(config, registry),
            other => Err(anyhow!("Unsupported sink connector type: {}", other)),
        }
    }
}
