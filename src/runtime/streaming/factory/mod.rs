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


pub mod connector;
pub mod global;

mod operator_constructor;
mod operator_factory;

use tracing::info;

use crate::sql::common::constants::factory_operator_name;

#[allow(unused_imports)]
pub use connector::{
    ConnectorSinkDispatcher, ConnectorSourceDispatcher, KafkaSinkDispatcher, KafkaSourceDispatcher,
};
pub use global::Registry;
pub use operator_constructor::OperatorConstructor;
pub use operator_factory::OperatorFactory;

fn register_builtin_connectors(factory: &mut OperatorFactory) {
    factory.register(
        factory_operator_name::CONNECTOR_SOURCE,
        Box::new(connector::ConnectorSourceDispatcher),
    );
    factory.register(
        factory_operator_name::CONNECTOR_SINK,
        Box::new(connector::ConnectorSinkDispatcher),
    );
}

fn register_kafka_connector_plugins(factory: &mut OperatorFactory) {
    factory.register(
        factory_operator_name::KAFKA_SOURCE,
        Box::new(connector::kafka::ConnectorDispatcher),
    );
    factory.register(
        factory_operator_name::KAFKA_SINK,
        Box::new(connector::kafka::ConnectorDispatcher),
    );
    info!(
        "Registered Kafka connector plugins ({}, {})",
        factory_operator_name::KAFKA_SOURCE,
        factory_operator_name::KAFKA_SINK
    );
}