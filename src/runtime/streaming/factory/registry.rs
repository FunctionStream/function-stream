use anyhow::{anyhow, Result};
use crate::runtime::streaming::api::operator::ConstructedOperator;
use std::collections::HashMap;


/// 工业级算子注册表与工厂
pub struct OperatorFactory {
    constructors: HashMap<String, Box<dyn OperatorConstructor>>,
}

impl OperatorFactory {
    pub fn new() -> Self {
        let factory = Self {
            constructors: HashMap::new(),
        };

        // TODO: 在此注册具体算子构造器
        factory.register("TumblingWindowAggregate", Box::new(TumblingWindowAggregateConstructor));
        factory.register("ExpressionWatermark", Box::new(WatermarkGeneratorConstructor));
        factory.register("KafkaSource", Box::new(KafkaSourceConstructor));

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

        ctor.with_config(payload)
    }
}
