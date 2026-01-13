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

// Kafka Config - Kafka configuration structure
//
// Defines configuration options for Kafka input source
//
// Note: Each input source only supports one topic and one partition

use serde_yaml::Value;
use std::collections::HashMap;

/// KafkaConfig - Kafka configuration
///
/// Contains all configuration options for Kafka input source
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Bootstrap servers (server addresses)
    /// Can be a single string (comma-separated) or a list of strings
    pub bootstrap_servers: Vec<String>,
    /// Topic name (single)
    pub topic: String,
    /// Partition ID (optional, uses subscribe auto-assignment if not specified)
    pub partition: Option<i32>,
    /// Consumer group ID
    pub group_id: String,
    /// Other configuration items (key-value pairs)
    pub properties: HashMap<String, String>,
}

impl KafkaConfig {
    /// Create new Kafka configuration
    ///
    /// # Arguments
    /// - `bootstrap_servers`: List of Kafka broker addresses
    /// - `topic`: Topic name (single)
    /// - `partition`: Partition ID (optional)
    /// - `group_id`: Consumer group ID
    /// - `properties`: Other configuration items
    pub fn new(
        bootstrap_servers: Vec<String>,
        topic: String,
        partition: Option<i32>,
        group_id: String,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            bootstrap_servers,
            topic,
            partition,
            group_id,
            properties,
        }
    }

    /// Create configuration from single bootstrap servers string
    ///
    /// Supports comma-separated multiple server addresses
    ///
    /// # Arguments
    /// - `bootstrap_servers`: Kafka broker addresses (comma-separated string)
    /// - `topic`: Topic name (single)
    /// - `partition`: Partition ID (optional)
    /// - `group_id`: Consumer group ID
    /// - `properties`: Other configuration items
    pub fn from_bootstrap_servers_str(
        bootstrap_servers: &str,
        topic: String,
        partition: Option<i32>,
        group_id: String,
        properties: HashMap<String, String>,
    ) -> Self {
        let servers: Vec<String> = bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        Self::new(servers, topic, partition, group_id, properties)
    }

    /// Get bootstrap servers string (comma-separated)
    pub fn bootstrap_servers_str(&self) -> String {
        self.bootstrap_servers.join(",")
    }

    /// Get partition display string
    pub fn partition_str(&self) -> String {
        match self.partition {
            Some(p) => p.to_string(),
            None => "auto".to_string(),
        }
    }

    /// Parse Kafka configuration from YAML Value
    ///
    /// # Arguments
    /// - `input_config`: YAML Value of input configuration
    ///
    /// # Returns
    /// - `Ok(KafkaConfig)`: Configuration parsed successfully
    /// - `Err(...)`: Parse failed
    pub fn from_yaml_value(
        input_config: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // Parse bootstrap.servers
        // Supports two formats:
        // 1. bootstrap_servers: "localhost:9092" or "kafka1:9092,kafka2:9092"
        // 2. bootstrap_servers: ["localhost:9092", "kafka2:9092"]
        let bootstrap_servers = if let Some(servers_value) = input_config.get("bootstrap_servers") {
            if let Some(servers_str) = servers_value.as_str() {
                // Single string, possibly comma-separated multiple addresses
                servers_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else if let Some(servers_seq) = servers_value.as_sequence() {
                // String list
                servers_seq
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid 'bootstrap_servers' format in Kafka input config",
                )) as Box<dyn std::error::Error + Send>);
            }
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing 'bootstrap_servers' in Kafka input config",
            )) as Box<dyn std::error::Error + Send>);
        };

        // Parse topic (single, required)
        let topic = input_config
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing or invalid 'topic' in Kafka input config (must be a string)",
                )) as Box<dyn std::error::Error + Send>
            })?
            .to_string();

        // Parse partition (optional, uses subscribe auto-assignment if not specified)
        let partition = if let Some(partition_value) = input_config.get("partition") {
            Some(partition_value.as_i64().ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid 'partition' format in Kafka input config (must be an integer)",
                )) as Box<dyn std::error::Error + Send>
            })? as i32)
        } else {
            // Don't specify partition, use subscribe auto-assignment
            None
        };

        // Parse group_id
        let group_id = input_config
            .get("group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing 'group_id' in Kafka input config",
                )) as Box<dyn std::error::Error + Send>
            })?
            .to_string();

        // Parse properties
        let mut properties = HashMap::new();
        if let Some(props) = input_config.get("properties").and_then(|v| v.as_mapping()) {
            for (key, value) in props {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_str()) {
                    properties.insert(k.to_string(), v.to_string());
                }
            }
        }

        Ok(Self::new(
            bootstrap_servers,
            topic,
            partition,
            group_id,
            properties,
        ))
    }
}
