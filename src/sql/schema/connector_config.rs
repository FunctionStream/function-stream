// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// Strongly-typed in-memory connector configuration for the SQL catalog layer.
// Maps 1:1 to the `ConnectorOp.oneof config` proto variants.

use std::collections::HashMap;

use protocol::grpc::api::{
    connector_op, GenericConnectorConfig, KafkaSinkConfig, KafkaSourceConfig,
};

/// Strongly-typed connector configuration stored in [`super::SourceTable`].
///
/// Each variant corresponds directly to a proto `ConnectorOp.oneof config` branch.
/// Adding a new connector (e.g. MySQL CDC) means adding a variant here and a proto message —
/// the Rust compiler will then guide you to every call-site that needs updating.
#[derive(Debug, Clone)]
pub enum ConnectorConfig {
    KafkaSource(KafkaSourceConfig),
    KafkaSink(KafkaSinkConfig),
    /// Fallback for connectors not yet strongly typed (e.g. future Redis, JDBC).
    Generic(HashMap<String, String>),
}

impl ConnectorConfig {
    /// Convert to the proto `ConnectorOp.oneof config` representation — zero JSON involved.
    pub fn to_proto_config(&self) -> connector_op::Config {
        match self {
            ConnectorConfig::KafkaSource(cfg) => {
                connector_op::Config::KafkaSource(cfg.clone())
            }
            ConnectorConfig::KafkaSink(cfg) => {
                connector_op::Config::KafkaSink(cfg.clone())
            }
            ConnectorConfig::Generic(props) => {
                connector_op::Config::Generic(GenericConnectorConfig {
                    properties: props.clone(),
                })
            }
        }
    }
}

// Proto-generated types do not derive Eq/Hash/PartialEq since they contain f32/f64
// in the general case. For our subset (Kafka configs) all fields are integers, strings,
// and maps — logically hashable. We impl the traits via serialized proto bytes so the
// SourceTable derive chain stays intact.

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
