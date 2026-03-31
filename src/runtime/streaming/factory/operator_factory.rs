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


use anyhow::{anyhow, Result};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use protocol::grpc::api::ProjectionOperator as ProjectionOperatorProto;
use super::operator_constructor::OperatorConstructor;
use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::connector::{
    ConnectorSinkDispatcher, ConnectorSourceDispatcher,
};
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::operators::grouping::IncrementalAggregatingConstructor;
use crate::runtime::streaming::operators::joins::{
    InstantJoinConstructor, JoinWithExpirationConstructor,
};
use crate::runtime::streaming::operators::key_by::KeyByConstructor;
use crate::runtime::streaming::operators::watermark::WatermarkGeneratorConstructor;
use crate::runtime::streaming::operators::windows::{
    SessionAggregatingWindowConstructor, SlidingAggregatingWindowConstructor,
    TumblingAggregateWindowConstructor, WindowFunctionConstructor,
};
use crate::runtime::streaming::operators::{ProjectionOperator, StatelessPhysicalExecutor, ValueExecutionOperator};
use protocol::grpc::api::{
    ExpressionWatermarkConfig, JoinOperator as JoinOperatorProto,
    KeyPlanOperator as KeyByProto,
    SessionWindowAggregateOperator, SlidingWindowAggregateOperator, TumblingWindowAggregateOperator,
    UpdatingAggregateOperator, ValuePlanOperator, WindowFunctionOperator as WindowFunctionProto,
};

use crate::sql::logical_node::logical::OperatorName;

///
pub struct OperatorFactory {
    constructors: HashMap<String, Box<dyn OperatorConstructor>>,
    registry: Arc<Registry>,
}

impl OperatorFactory {
    pub fn new(registry: Arc<Registry>) -> Self {
        let mut factory = Self {
            constructors: HashMap::new(),
            registry,
        };
        factory.register_builtins();
        factory
    }

    pub fn register(&mut self, name: &str, constructor: Box<dyn OperatorConstructor>) {
        self.constructors.insert(name.to_string(), constructor);
    }

    pub fn register_named(&mut self, name: OperatorName, constructor: Box<dyn OperatorConstructor>) {
        self.register(name.as_registry_key(), constructor);
    }

    pub fn create_operator(&self, name: &str, payload: &[u8]) -> Result<ConstructedOperator> {
        let ctor = self
            .constructors
            .get(name)
            .ok_or_else(|| {
                anyhow!(
                    "FATAL: Operator '{}' not found in Factory Registry. \
                     Ensure the worker is compiled with the correct plugins.",
                    name
                )
            })?;

        ctor.with_config(payload, self.registry.clone())
    }

    pub fn registered_operators(&self) -> Vec<&str> {
        self.constructors.keys().map(|s| s.as_str()).collect()
    }

    fn register_builtins(&mut self) {
        self.register_named(OperatorName::TumblingWindowAggregate, Box::new(TumblingWindowBridge));
        self.register_named(OperatorName::SlidingWindowAggregate, Box::new(SlidingWindowBridge));
        self.register_named(OperatorName::SessionWindowAggregate, Box::new(SessionWindowBridge));

        self.register_named(OperatorName::ExpressionWatermark, Box::new(WatermarkBridge));

        // ─── SQL Window Function ───
        self.register_named(OperatorName::WindowFunction, Box::new(WindowFunctionBridge));

        // ─── Join ───
        self.register_named(OperatorName::Join, Box::new(JoinWithExpirationBridge));
        self.register_named(OperatorName::InstantJoin, Box::new(InstantJoinBridge));
        self.register_named(OperatorName::LookupJoin, Box::new(LookupJoinBridge));

        self.register_named(OperatorName::UpdatingAggregate, Box::new(IncrementalAggregateBridge));

        self.register_named(OperatorName::KeyBy, Box::new(KeyByBridge));

        self.register_named(OperatorName::Projection, Box::new(ProjectionConstructor));
        self.register_named(OperatorName::Value, Box::new(ValueBridge));
        self.register_named(OperatorName::ConnectorSource, Box::new(ConnectorSourceBridge));
        self.register_named(OperatorName::ConnectorSink, Box::new(ConnectorSinkBridge));

        crate::runtime::streaming::factory::register_kafka_connector_plugins(self);
    }
}

struct TumblingWindowBridge;
impl OperatorConstructor for TumblingWindowBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = TumblingWindowAggregateOperator::decode(config)
            .map_err(|e| anyhow!("Decode TumblingWindowAggregateOperator failed: {e}"))?;
        let op = TumblingAggregateWindowConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct SlidingWindowBridge;
impl OperatorConstructor for SlidingWindowBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = SlidingWindowAggregateOperator::decode(config)
            .map_err(|e| anyhow!("Decode SlidingWindowAggregateOperator failed: {e}"))?;
        let op = SlidingAggregatingWindowConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct SessionWindowBridge;
impl OperatorConstructor for SessionWindowBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = SessionWindowAggregateOperator::decode(config)
            .map_err(|e| anyhow!("Decode SessionWindowAggregateOperator failed: {e}"))?;
        let op = SessionAggregatingWindowConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct WatermarkBridge;
impl OperatorConstructor for WatermarkBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = ExpressionWatermarkConfig::decode(config)
            .map_err(|e| anyhow!("Decode ExpressionWatermarkConfig failed: {e}"))?;
        let op = WatermarkGeneratorConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct WindowFunctionBridge;
impl OperatorConstructor for WindowFunctionBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = WindowFunctionProto::decode(config)
            .map_err(|e| anyhow!("Decode WindowFunctionOperator failed: {e}"))?;
        let op = WindowFunctionConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct JoinWithExpirationBridge;
impl OperatorConstructor for JoinWithExpirationBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = JoinOperatorProto::decode(config)
            .map_err(|e| anyhow!("Decode JoinOperator (expiration) failed: {e}"))?;
        let op = JoinWithExpirationConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct InstantJoinBridge;
impl OperatorConstructor for InstantJoinBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = JoinOperatorProto::decode(config)
            .map_err(|e| anyhow!("Decode JoinOperator (instant) failed: {e}"))?;
        let op = InstantJoinConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct LookupJoinBridge;
impl OperatorConstructor for LookupJoinBridge {
    fn with_config(&self, _config: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        Err(anyhow!("LookupJoin is not supported in the current runtime"))
    }
}

struct IncrementalAggregateBridge;
impl OperatorConstructor for IncrementalAggregateBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = UpdatingAggregateOperator::decode(config)
            .map_err(|e| anyhow!("Decode UpdatingAggregateOperator failed: {e}"))?;
        let op = IncrementalAggregatingConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct KeyByBridge;
impl OperatorConstructor for KeyByBridge {
    fn with_config(&self, config: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = KeyByProto::decode(config)
            .map_err(|e| anyhow!("Decode KeyPlanOperator failed: {e}"))?;
        let op = KeyByConstructor.with_config(proto)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

pub struct ProjectionConstructor;

impl OperatorConstructor for ProjectionConstructor {
    fn with_config(&self, payload: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = ProjectionOperatorProto::decode(payload)?;
        let op = ProjectionOperator::from_proto(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

struct ValueBridge;
impl OperatorConstructor for ValueBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let proto = ValuePlanOperator::decode(config)
            .map_err(|e| anyhow!("Decode ValuePlanOperator failed: {e}"))?;
        let op = ValueExecutionConstructor.with_config(proto, registry)?;
        Ok(ConstructedOperator::Operator(Box::new(op)))
    }
}

/// Generic connector source constructor: decodes `ConnectorOp` and dispatches by connector type.
struct ConnectorSourceBridge;
impl OperatorConstructor for ConnectorSourceBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        ConnectorSourceDispatcher.with_config(config, registry)
    }
}

/// Generic connector sink constructor: decodes `ConnectorOp` and dispatches by connector type.
struct ConnectorSinkBridge;
impl OperatorConstructor for ConnectorSinkBridge {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator> {
        ConnectorSinkDispatcher.with_config(config, registry)
    }
}


struct ValueExecutionConstructor;
impl ValueExecutionConstructor {
    fn with_config(
        &self,
        config: ValuePlanOperator,
        registry: Arc<Registry>,
    ) -> Result<ValueExecutionOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, registry.as_ref())
            .map_err(|e| anyhow!("build value execution plan '{}': {e}", config.name))?;
        Ok(ValueExecutionOperator::new(config.name, executor))
    }
}