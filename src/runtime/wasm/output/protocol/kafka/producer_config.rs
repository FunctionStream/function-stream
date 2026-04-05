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

// Kafka Producer Config - Kafka producer configuration structure
//
// Defines configuration options for Kafka output sink
//
// Note: Each output sink only supports one topic and one partition

use std::collections::HashMap;

/// KafkaProducerConfig - Kafka producer configuration
///
/// Contains all configuration options for Kafka output sink
///
/// Each output sink only supports one topic and one partition
#[derive(Debug, Clone)]
pub struct KafkaProducerConfig {
    /// Bootstrap servers (server addresses)
    /// Can be a single string (comma-separated) or a list of strings
    pub bootstrap_servers: Vec<String>,
    /// Topic name (single)
    pub topic: String,
    /// Partition ID (single, optional, if not specified Kafka will assign automatically)
    pub partition: Option<i32>,
    /// Other configuration items (key-value pairs)
    pub properties: HashMap<String, String>,
}

impl KafkaProducerConfig {
    /// Create new Kafka producer configuration
    ///
    /// # Arguments
    /// - `bootstrap_servers`: Kafka broker address list
    /// - `topic`: Topic name (single)
    /// - `partition`: Partition ID (single, optional)
    /// - `properties`: Other configuration items
    pub fn new(
        bootstrap_servers: Vec<String>,
        topic: String,
        partition: Option<i32>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            bootstrap_servers,
            topic,
            partition,
            properties,
        }
    }

    /// Get bootstrap servers string (comma-separated)
    pub fn bootstrap_servers_str(&self) -> String {
        self.bootstrap_servers.join(",")
    }

    /// Create Kafka producer
    ///
    /// # Returns
    /// - `Ok(ThreadedProducer)`: Successfully created producer
    /// - `Err(...)`: Creation failed
    pub fn create_producer(
        &self,
    ) -> Result<
        rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
        Box<dyn std::error::Error + Send>,
    > {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};

        let mut client_config = ClientConfig::new();

        // Set bootstrap servers (required)
        client_config.set("bootstrap.servers", self.bootstrap_servers_str());

        // Apply user-defined configuration
        for (key, value) in &self.properties {
            client_config.set(key, value);
        }

        // Create producer
        let producer: ThreadedProducer<DefaultProducerContext> =
            client_config
                .create()
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Failed to create Kafka producer: {}",
                        e
                    )))
                })?;

        Ok(producer)
    }
}
