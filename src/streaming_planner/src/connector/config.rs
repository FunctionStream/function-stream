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

use protocol::function_stream_graph::{
    DeltaSinkConfig, FilesystemSinkConfig, IcebergSinkConfig, KafkaSinkConfig, KafkaSourceConfig,
    LanceDbSinkConfig, S3SinkConfig, connector_op,
};

#[derive(Debug, Clone)]
pub enum ConnectorConfig {
    KafkaSource(KafkaSourceConfig),
    KafkaSink(KafkaSinkConfig),
    FilesystemSink(FilesystemSinkConfig),
    DeltaSink(DeltaSinkConfig),
    IcebergSink(IcebergSinkConfig),
    S3Sink(S3SinkConfig),
    LanceDbSink(LanceDbSinkConfig),
}

impl ConnectorConfig {
    pub fn to_proto_config(&self) -> connector_op::Config {
        match self {
            ConnectorConfig::KafkaSource(cfg) => connector_op::Config::KafkaSource(cfg.clone()),
            ConnectorConfig::KafkaSink(cfg) => connector_op::Config::KafkaSink(cfg.clone()),
            ConnectorConfig::FilesystemSink(cfg) => {
                connector_op::Config::FilesystemSink(cfg.clone())
            }
            ConnectorConfig::DeltaSink(cfg) => connector_op::Config::DeltaSink(cfg.clone()),
            ConnectorConfig::IcebergSink(cfg) => connector_op::Config::IcebergSink(cfg.clone()),
            ConnectorConfig::S3Sink(cfg) => connector_op::Config::S3Sink(cfg.clone()),
            ConnectorConfig::LanceDbSink(cfg) => connector_op::Config::LancedbSink(cfg.clone()),
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
            (ConnectorConfig::FilesystemSink(a), ConnectorConfig::FilesystemSink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::DeltaSink(a), ConnectorConfig::DeltaSink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::IcebergSink(a), ConnectorConfig::IcebergSink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::S3Sink(a), ConnectorConfig::S3Sink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
            (ConnectorConfig::LanceDbSink(a), ConnectorConfig::LanceDbSink(b)) => {
                a.encode_to_vec() == b.encode_to_vec()
            }
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
            ConnectorConfig::FilesystemSink(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::DeltaSink(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::IcebergSink(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::S3Sink(cfg) => cfg.encode_to_vec().hash(state),
            ConnectorConfig::LanceDbSink(cfg) => cfg.encode_to_vec().hash(state),
        }
    }
}
