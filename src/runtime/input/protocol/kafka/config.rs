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
}
