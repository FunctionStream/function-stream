// Metrics types and definitions

use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// Metric identifier
pub type MetricId = String;

/// Metric name
pub type MetricName = String;

/// Metric value type
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Counter metric (monotonically increasing)
    Counter(i64),
    /// Gauge metric (can go up or down)
    Gauge(f64),
    /// Histogram metric (distribution of values)
    Histogram(Vec<f64>),
}

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// Metric labels (key-value pairs for filtering and grouping)
pub type MetricLabels = std::collections::HashMap<String, String>;

/// Metric data point
#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    /// Metric name
    pub name: MetricName,
    /// Metric value
    pub value: MetricValue,
    /// Metric labels
    pub labels: MetricLabels,
    /// Timestamp
    pub timestamp: SystemTime,
}

/// Metric definition
#[derive(Debug, Clone)]
pub struct MetricDefinition {
    /// Metric ID
    pub id: MetricId,
    /// Metric name
    pub name: MetricName,
    /// Metric type
    pub metric_type: MetricType,
    /// Metric description
    pub description: String,
    /// Default labels
    pub default_labels: MetricLabels,
}

/// Metric entry
pub struct MetricEntry {
    /// Metric definition
    pub definition: MetricDefinition,
    /// Current value
    pub value: Arc<RwLock<MetricValue>>,
    /// Last update time
    pub last_updated: Arc<RwLock<SystemTime>>,
}

impl MetricEntry {
    /// Create a new metric entry
    pub fn new(definition: MetricDefinition) -> Self {
        let initial_value = match definition.metric_type {
            MetricType::Counter => MetricValue::Counter(0),
            MetricType::Gauge => MetricValue::Gauge(0.0),
            MetricType::Histogram => MetricValue::Histogram(Vec::new()),
        };

        Self {
            definition,
            value: Arc::new(RwLock::new(initial_value)),
            last_updated: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    /// Update metric value
    pub async fn update(&self, value: MetricValue) {
        let mut v = self.value.write().await;
        *v = value;
        let mut t = self.last_updated.write().await;
        *t = SystemTime::now();
    }

    /// Increment counter
    pub async fn increment(&self, delta: i64) {
        let mut v = self.value.write().await;
        if let MetricValue::Counter(ref mut count) = *v {
            *count += delta;
        }
        let mut t = self.last_updated.write().await;
        *t = SystemTime::now();
    }

    /// Set gauge value
    pub async fn set_gauge(&self, value: f64) {
        let mut v = self.value.write().await;
        *v = MetricValue::Gauge(value);
        let mut t = self.last_updated.write().await;
        *t = SystemTime::now();
    }

    /// Add histogram sample
    pub async fn add_histogram_sample(&self, sample: f64) {
        let mut v = self.value.write().await;
        if let MetricValue::Histogram(ref mut samples) = *v {
            samples.push(sample);
            // Keep only last 1000 samples to avoid memory issues
            if samples.len() > 1000 {
                samples.remove(0);
            }
        }
        let mut t = self.last_updated.write().await;
        *t = SystemTime::now();
    }

    /// Get current value
    pub async fn get_value(&self) -> MetricValue {
        let v = self.value.read().await;
        v.clone()
    }

    /// Get metric data point
    pub async fn get_data_point(&self) -> MetricDataPoint {
        let value = self.get_value().await;
        let timestamp = {
            let t = self.last_updated.read().await;
            *t
        };

        MetricDataPoint {
            name: self.definition.name.clone(),
            value,
            labels: self.definition.default_labels.clone(),
            timestamp,
        }
    }
}
