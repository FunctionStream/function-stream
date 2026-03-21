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

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;

use super::column_descriptor::ColumnDescriptor;
use super::connection_type::ConnectionType;

/// Role of a connector-backed table in the pipeline (ingest / egress / lookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableRole {
    Ingestion,
    Egress,
    Reference,
}

impl From<TableRole> for ConnectionType {
    fn from(r: TableRole) -> Self {
        match r {
            TableRole::Ingestion => ConnectionType::Source,
            TableRole::Egress => ConnectionType::Sink,
            TableRole::Reference => ConnectionType::Lookup,
        }
    }
}

impl From<ConnectionType> for TableRole {
    fn from(c: ConnectionType) -> Self {
        match c {
            ConnectionType::Source => TableRole::Ingestion,
            ConnectionType::Sink => TableRole::Egress,
            ConnectionType::Lookup => TableRole::Reference,
        }
    }
}

pub fn validate_adapter_availability(adapter: &str) -> Result<()> {
    let supported = [
        "kafka",
        "kinesis",
        "filesystem",
        "delta",
        "iceberg",
        "pulsar",
        "nats",
        "redis",
        "mqtt",
        "websocket",
        "sse",
        "nexmark",
        "blackhole",
        "lookup",
        "memory",
        "postgres",
    ];
    if !supported.contains(&adapter) {
        return Err(DataFusionError::Plan(format!("Unknown adapter '{adapter}'")));
    }
    Ok(())
}

pub fn apply_adapter_specific_rules(adapter: &str, mut cols: Vec<ColumnDescriptor>) -> Vec<ColumnDescriptor> {
    match adapter {
        "delta" | "iceberg" => {
            for c in &mut cols {
                if matches!(c.data_type(), DataType::Timestamp(_, _)) {
                    c.force_precision(TimeUnit::Microsecond);
                }
            }
            cols
        }
        _ => cols,
    }
}

pub fn deduce_role(options: &HashMap<String, String>) -> Result<TableRole> {
    match options.get("type").map(|s| s.as_str()) {
        None | Some("source") => Ok(TableRole::Ingestion),
        Some("sink") => Ok(TableRole::Egress),
        Some("lookup") => Ok(TableRole::Reference),
        Some(other) => plan_err!("Invalid role '{other}'"),
    }
}

pub fn serialize_backend_params(adapter: &str, options: &HashMap<String, String>) -> Result<String> {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "adapter".to_string(),
        serde_json::Value::String(adapter.to_string()),
    );

    for (k, v) in options {
        payload.insert(k.clone(), serde_json::Value::String(v.clone()));
    }

    serde_json::to_string(&payload).map_err(|e| DataFusionError::Plan(e.to_string()))
}
