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

//! 流算子工厂：[`global`] 为共享注册表；[`connector`] 为 Source/Sink 协议与实现；
//! [`OperatorFactory`]、[`OperatorConstructor`] 在根模块，避免与 `connector` 循环依赖。

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
#[allow(unused_imports)]
pub use operator_factory::PassthroughConstructor;

/// 注册 `ConnectorSource` / `ConnectorSink` 分发器（打破 `operator_factory` ↔ `connector` 依赖环）。
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

/// 注册直连 Kafka 算子（名称见 [`crate::sql::common::constants::factory_operator_name`]）。
fn register_kafka_connector_plugins(factory: &mut OperatorFactory) {
    factory.register(
        factory_operator_name::KAFKA_SOURCE,
        Box::new(connector::KafkaSourceDispatcher),
    );
    factory.register(
        factory_operator_name::KAFKA_SINK,
        Box::new(connector::KafkaSinkDispatcher),
    );
    info!(
        "Registered Kafka connector plugins ({}, {})",
        factory_operator_name::KAFKA_SOURCE,
        factory_operator_name::KAFKA_SINK
    );
}
