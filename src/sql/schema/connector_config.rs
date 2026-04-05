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

use protocol::function_stream_graph::{
    GenericConnectorConfig, KafkaSinkConfig, KafkaSourceConfig, connector_op,
};

#[derive(Debug, Clone)]
pub enum ConnectorConfig {
    KafkaSource(KafkaSourceConfig),
    KafkaSink(KafkaSinkConfig),
    Generic(HashMap<String, String>),
}

impl ConnectorConfig {
    pub fn to_proto_config(&self) -> connector_op::Config {
        match self {
            ConnectorConfig::KafkaSource(cfg) => connector_op::Config::KafkaSource(cfg.clone()),
            ConnectorConfig::KafkaSink(cfg) => connector_op::Config::KafkaSink(cfg.clone()),
            ConnectorConfig::Generic(props) => {
                connector_op::Config::Generic(GenericConnectorConfig {
                    properties: props.clone(),
                })
            }
        }
    }
}

impl PartialEq for ConnectorConfig {
    fn eq(&self, other: &Self) -> bool {
        use prost::Message;
        match (self, other) {
            (ConnectorConfig::KafkaSource(a), ConnectorConfig::KafkaSource(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::KafkaSink(a), ConnectorConfig::KafkaSink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::Generic(a), ConnectorConfig::Generic(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ConnectorConfig {}

impl std::hash::Hash for ConnectorConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use prost::Message;
        std::mem::discriminant(self).hash(state);
        match self {
            ConnectorConfig::KafkaSource(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::KafkaSink(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::Generic(m) => {
                let mut pairs: Vec<_> = m.iter().collect();
                pairs.sort_by_key(|(k, _)| (*k).clone());
                pairs.hash(state);
            }
        }
    }
}
