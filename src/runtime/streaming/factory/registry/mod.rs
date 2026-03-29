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

use crate::sql::common::constants::connector_type;
use crate::runtime::streaming::api::operator::Registry;

use crate::runtime::streaming::api::operator::ConstructedOperator;
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

pub mod kafka_factory;

use kafka_factory::{register_kafka_plugins, KafkaSinkDispatcher, KafkaSourceDispatcher};

use protocol::grpc::api::{
    ConnectorOp, ExpressionWatermarkConfig,
    JoinOperator as JoinOperatorProto,
    KeyPlanOperator as KeyByProto,
    SessionWindowAggregateOperator, SlidingWindowAggregateOperator,
    TumblingWindowAggregateOperator, UpdatingAggregateOperator,
    WindowFunctionOperator as WindowFunctionProto,
};

// ---------------------------------------------------------------------------
// 1. Core Trait (工厂契约)
// ---------------------------------------------------------------------------

/// 算子构造器 trait：每个实现者负责从 protobuf 字节流反序列化配置并构造 [`ConstructedOperator`]。
///
/// 外部插件可实现此 trait 并通过 [`OperatorFactory::register`] 注入。
pub trait OperatorConstructor: Send + Sync {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator>;
}

// ---------------------------------------------------------------------------
// 2. 工业级工厂注册表
// ---------------------------------------------------------------------------

/// 持有 `name → OperatorConstructor` 映射与共享 [`Registry`]。
///
/// `JobManager` 在部署任务时调用 [`create_operator`]，完成从字节流到运行时算子的
/// 反射式实例化。
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

    /// 反射与实例化：从 TDD 的字节流中拉起运行时的业务算子
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

    /// 列出已注册的所有算子名称（调试用）。
    pub fn registered_operators(&self) -> Vec<&str> {
        self.constructors.keys().map(|s| s.as_str()).collect()
    }

    fn register_builtins(&mut self) {
        // ─── 窗口聚合 ───
        self.register("TumblingWindowAggregate", Box::new(TumblingWindowBridge));
        self.register("SlidingWindowAggregate", Box::new(SlidingWindowBridge));
        self.register("SessionWindowAggregate", Box::new(SessionWindowBridge));

        // ─── 水位 ───
        self.register("ExpressionWatermark", Box::new(WatermarkBridge));

        // ─── SQL Window Function ───
        self.register("WindowFunction", Box::new(WindowFunctionBridge));

        // ─── Join ───
        self.register("Join", Box::new(JoinWithExpirationBridge));
        self.register("InstantJoin", Box::new(InstantJoinBridge));
        self.register("LookupJoin", Box::new(LookupJoinBridge));

        // ─── 增量聚合 ───
        self.register("UpdatingAggregate", Box::new(IncrementalAggregateBridge));

        // ─── 物理网络路由 ───
        self.register("KeyBy", Box::new(KeyByBridge));

        // ─── 连接器 Source / Sink（分发器模式，不硬编码具体连接器） ───
        self.register("ConnectorSource", Box::new(ConnectorSourceDispatcher));
        self.register("ConnectorSink", Box::new(ConnectorSinkDispatcher));

        // ─── 透传类算子 ───
        self.register("Projection", Box::new(PassthroughConstructor("Projection")));
        self.register("ArrowValue", Box::new(PassthroughConstructor("ArrowValue")));
        self.register("ArrowKey", Box::new(PassthroughConstructor("ArrowKey")));

        register_kafka_plugins(self);
    }
}

// ---------------------------------------------------------------------------
// 3. 构造器适配 — 解码 protobuf 后委托给各算子模块的 Constructor
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 4. 连接器分发抽象 (Connector Dispatcher) — 不硬编码具体连接器
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 5. 透传类算子
// ---------------------------------------------------------------------------

pub struct PassthroughConstructor(pub &'static str);

impl OperatorConstructor for PassthroughConstructor {
    fn with_config(&self, _config: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        Ok(ConstructedOperator::Operator(Box::new(
            PassthroughOperator::new(self.0),
        )))
    }
}
