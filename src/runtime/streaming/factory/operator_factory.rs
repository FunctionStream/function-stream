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

use super::operator_constructor::OperatorConstructor;
use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::operators::PassthroughOperator;
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

use protocol::grpc::api::{
    ExpressionWatermarkConfig, JoinOperator as JoinOperatorProto,
    KeyPlanOperator as KeyByProto, SessionWindowAggregateOperator, SlidingWindowAggregateOperator,
    TumblingWindowAggregateOperator, UpdatingAggregateOperator,
    WindowFunctionOperator as WindowFunctionProto,
};

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
        self.register("TumblingWindowAggregate", Box::new(TumblingWindowBridge));
        self.register("SlidingWindowAggregate", Box::new(SlidingWindowBridge));
        self.register("SessionWindowAggregate", Box::new(SessionWindowBridge));

        self.register("ExpressionWatermark", Box::new(WatermarkBridge));

        // ─── SQL Window Function ───
        self.register("WindowFunction", Box::new(WindowFunctionBridge));

        // ─── Join ───
        self.register("Join", Box::new(JoinWithExpirationBridge));
        self.register("InstantJoin", Box::new(InstantJoinBridge));
        self.register("LookupJoin", Box::new(LookupJoinBridge));

        self.register("UpdatingAggregate", Box::new(IncrementalAggregateBridge));

        self.register("KeyBy", Box::new(KeyByBridge));

        self.register("Projection", Box::new(PassthroughConstructor("Projection")));
        self.register("ArrowValue", Box::new(PassthroughConstructor("ArrowValue")));
        self.register("ArrowKey", Box::new(PassthroughConstructor("ArrowKey")));

        crate::runtime::streaming::factory::register_builtin_connectors(self);
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

pub struct PassthroughConstructor(pub &'static str);

impl OperatorConstructor for PassthroughConstructor {
    fn with_config(&self, _config: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        Ok(ConstructedOperator::Operator(Box::new(
            PassthroughOperator::new(self.0),
        )))
    }
}
