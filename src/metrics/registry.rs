// Metrics registry for managing all metrics

use crate::metrics::types::{
    MetricDefinition, MetricEntry, MetricId, MetricLabels, MetricName, MetricType,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Metrics registry
pub struct MetricsRegistry {
    /// Registered metrics: metric_id -> metric_entry
    metrics: Arc<RwLock<HashMap<MetricId, Arc<MetricEntry>>>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new metric
    pub async fn register(&self, definition: MetricDefinition) -> Result<Arc<MetricEntry>> {
        let id = definition.id.clone();
        let mut metrics = self.metrics.write().await;

        if metrics.contains_key(&id) {
            return Err(anyhow::anyhow!("Metric with ID '{}' already exists", id));
        }

        let entry = Arc::new(MetricEntry::new(definition));
        metrics.insert(id.clone(), entry.clone());

        log::info!("Registering metric: {}", id);
        Ok(entry)
    }

    /// Register a counter metric
    pub async fn register_counter(
        &self,
        id: MetricId,
        name: MetricName,
        description: String,
        labels: MetricLabels,
    ) -> Result<Arc<MetricEntry>> {
        let definition = MetricDefinition {
            id: id.clone(),
            name,
            metric_type: MetricType::Counter,
            description,
            default_labels: labels,
        };
        self.register(definition).await
    }

    /// Register a gauge metric
    pub async fn register_gauge(
        &self,
        id: MetricId,
        name: MetricName,
        description: String,
        labels: MetricLabels,
    ) -> Result<Arc<MetricEntry>> {
        let definition = MetricDefinition {
            id: id.clone(),
            name,
            metric_type: MetricType::Gauge,
            description,
            default_labels: labels,
        };
        self.register(definition).await
    }

    /// Register a histogram metric
    pub async fn register_histogram(
        &self,
        id: MetricId,
        name: MetricName,
        description: String,
        labels: MetricLabels,
    ) -> Result<Arc<MetricEntry>> {
        let definition = MetricDefinition {
            id: id.clone(),
            name,
            metric_type: MetricType::Histogram,
            description,
            default_labels: labels,
        };
        self.register(definition).await
    }

    /// Get a metric by ID
    pub async fn get(&self, id: &MetricId) -> Option<Arc<MetricEntry>> {
        let metrics = self.metrics.read().await;
        metrics.get(id).cloned()
    }

    /// Check if a metric exists
    pub async fn exists(&self, id: &MetricId) -> bool {
        let metrics = self.metrics.read().await;
        metrics.contains_key(id)
    }

    /// List all metric IDs
    pub async fn list_ids(&self) -> Vec<MetricId> {
        let metrics = self.metrics.read().await;
        metrics.keys().cloned().collect()
    }

    /// Get all metrics
    pub async fn get_all(&self) -> Vec<Arc<MetricEntry>> {
        let metrics = self.metrics.read().await;
        metrics.values().cloned().collect()
    }

    /// Unregister a metric
    pub async fn unregister(&self, id: &MetricId) -> Result<()> {
        let mut metrics = self.metrics.write().await;

        if metrics.remove(id).is_some() {
            log::info!("Unregistering metric: {}", id);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Metric with ID '{}' not found", id))
        }
    }

    /// Get metric count
    pub async fn count(&self) -> usize {
        let metrics = self.metrics.read().await;
        metrics.len()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

