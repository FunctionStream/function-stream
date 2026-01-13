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

// Metrics collector for collecting and exporting metrics

use crate::metrics::registry::MetricsRegistry;
use crate::metrics::types::{MetricDataPoint, MetricValue};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Metrics snapshot
pub type MetricsSnapshot = Vec<MetricDataPoint>;

/// Metrics collector
pub struct MetricsCollector {
    registry: Arc<MetricsRegistry>,
    /// Collected snapshots (for history)
    snapshots: Arc<RwLock<Vec<MetricsSnapshot>>>,
    /// Maximum number of snapshots to keep
    max_snapshots: usize,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self {
            registry,
            snapshots: Arc::new(RwLock::new(Vec::new())),
            max_snapshots: 100,
        }
    }

    /// Create with custom max snapshots
    pub fn with_max_snapshots(registry: Arc<MetricsRegistry>, max_snapshots: usize) -> Self {
        Self {
            registry,
            snapshots: Arc::new(RwLock::new(Vec::new())),
            max_snapshots,
        }
    }

    /// Collect current metrics snapshot
    pub async fn collect(&self) -> MetricsSnapshot {
        let metrics = self.registry.get_all().await;
        let mut snapshot = Vec::new();

        for metric in metrics {
            let data_point = metric.get_data_point().await;
            snapshot.push(data_point);
        }

        snapshot
    }

    /// Collect and store snapshot
    pub async fn collect_and_store(&self) -> Result<MetricsSnapshot> {
        let snapshot = self.collect().await;

        let mut snapshots = self.snapshots.write().await;
        snapshots.push(snapshot.clone());

        // Keep only last N snapshots
        if snapshots.len() > self.max_snapshots {
            snapshots.remove(0);
        }

        Ok(snapshot)
    }

    /// Get latest snapshot
    pub async fn get_latest_snapshot(&self) -> Option<MetricsSnapshot> {
        let snapshots = self.snapshots.read().await;
        snapshots.last().cloned()
    }

    /// Get all snapshots
    pub async fn get_all_snapshots(&self) -> Vec<MetricsSnapshot> {
        let snapshots = self.snapshots.read().await;
        snapshots.clone()
    }

    /// Export metrics as Prometheus format
    pub async fn export_prometheus(&self) -> String {
        let snapshot = self.collect().await;
        let mut output = String::new();

        for data_point in snapshot {
            let metric_name = data_point.name.replace("-", "_").replace(".", "_");
            let value_str = match &data_point.value {
                MetricValue::Counter(v) => v.to_string(),
                MetricValue::Gauge(v) => v.to_string(),
                MetricValue::Histogram(samples) => {
                    // Calculate average for histogram
                    if samples.is_empty() {
                        "0".to_string()
                    } else {
                        let sum: f64 = samples.iter().sum();
                        (sum / samples.len() as f64).to_string()
                    }
                }
            };

            // Build labels string
            let labels_str = if data_point.labels.is_empty() {
                String::new()
            } else {
                let labels: Vec<String> = data_point
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect();
                format!("{{{}}}", labels.join(","))
            };

            output.push_str(&format!(
                "# HELP {} {}\n",
                metric_name, "Metric description"
            ));
            output.push_str(&format!(
                "# TYPE {} {}\n",
                metric_name,
                match data_point.value {
                    MetricValue::Counter(_) => "counter",
                    MetricValue::Gauge(_) => "gauge",
                    MetricValue::Histogram(_) => "histogram",
                }
            ));
            output.push_str(&format!("{}{} {}\n", metric_name, labels_str, value_str));
        }

        output
    }

    /// Export metrics as JSON
    pub async fn export_json(&self) -> Result<String> {
        let snapshot = self.collect().await;
        let json_data: Vec<serde_json::Value> = snapshot
            .into_iter()
            .map(|dp| {
                let value = match &dp.value {
                    MetricValue::Counter(v) => serde_json::json!({ "counter": v }),
                    MetricValue::Gauge(v) => serde_json::json!({ "gauge": v }),
                    MetricValue::Histogram(samples) => {
                        serde_json::json!({ "histogram": samples })
                    }
                };

                serde_json::json!({
                    "name": dp.name,
                    "value": value,
                    "labels": dp.labels,
                    "timestamp": dp.timestamp.duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                })
            })
            .collect();

        serde_json::to_string_pretty(&json_data)
            .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))
    }

    /// Get metrics summary
    pub async fn get_summary(&self) -> HashMap<String, String> {
        let snapshot = self.collect().await;
        let total = snapshot.len();
        let mut summary = HashMap::new();

        let mut counters = 0;
        let mut gauges = 0;
        let mut histograms = 0;

        for data_point in snapshot {
            match data_point.value {
                MetricValue::Counter(_) => counters += 1,
                MetricValue::Gauge(_) => gauges += 1,
                MetricValue::Histogram(_) => histograms += 1,
            }
        }

        summary.insert("total_metrics".to_string(), total.to_string());
        summary.insert("counters".to_string(), counters.to_string());
        summary.insert("gauges".to_string(), gauges.to_string());
        summary.insert("histograms".to_string(), histograms.to_string());

        summary
    }
}
