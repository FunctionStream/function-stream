use anyhow::Result;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::common::OperatorConfig;

/// 维表查询接口：由具体 Connector（如 Redis、MySQL）实现。
#[async_trait]
pub trait LookupConnector: Send {
    fn name(&self) -> &str;

    /// 根据 key 列批量查询外部系统，返回结果 batch（含 `_lookup_key_index` 列）。
    /// 返回 `None` 表示无匹配行。
    async fn lookup(&self, keys: &[ArrayRef]) -> Option<Result<RecordBatch>>;
}

/// Connector 工厂 trait：每种外部系统实现此 trait 提供 Source / Sink / Lookup 构建能力。
pub trait Connector: Send + Sync {
    fn name(&self) -> &str;

    fn make_lookup(
        &self,
        config: OperatorConfig,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn LookupConnector>>;
}

/// 全局 Connector 注册表。
pub struct ConnectorRegistry {
    connectors: HashMap<String, Box<dyn Connector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            connectors: HashMap::new(),
        }
    }

    pub fn register(&mut self, connector: Box<dyn Connector>) {
        self.connectors
            .insert(connector.name().to_string(), connector);
    }

    pub fn get(&self, name: &str) -> Option<&dyn Connector> {
        self.connectors.get(name).map(|c| c.as_ref())
    }
}

/// 返回当前已注册的所有 Connector。
///
/// 目前返回空注册表，后续接入 Kafka / Redis 等时在此处注册。
pub fn connectors() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    // TODO: registry.register(Box::new(KafkaConnector));
    // TODO: registry.register(Box::new(RedisConnector));
    registry
}
