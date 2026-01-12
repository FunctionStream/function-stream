// Kafka Producer Config - Kafka producer configuration structure
//
// Defines configuration options for Kafka output sink
//
// Note: Each output sink only supports one topic and one partition

use std::collections::HashMap;
use serde_yaml::Value;

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
    pub fn create_producer(&self) -> Result<rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>, Box<dyn std::error::Error + Send>> {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{ThreadedProducer, DefaultProducerContext};

        let mut client_config = ClientConfig::new();

        // Set bootstrap servers (required)
        client_config.set("bootstrap.servers", &self.bootstrap_servers_str());

        // Apply user-defined configuration
        for (key, value) in &self.properties {
            client_config.set(key, value);
        }

        // Create producer
        let producer: ThreadedProducer<DefaultProducerContext> = client_config.create()
            .map_err(|e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create Kafka producer: {}", e),
                ))
            })?;

        Ok(producer)
    }

    /// Parse Kafka producer configuration from YAML Value
    ///
    /// # Arguments
    /// - `output_config`: YAML Value for output configuration
    ///
    /// # Returns
    /// - `Ok(KafkaProducerConfig)`: Successfully parsed configuration
    /// - `Err(...)`: Parsing failed
    pub fn from_yaml_value(output_config: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // Parse bootstrap.servers
        // Supports two formats:
        // 1. bootstrap_servers: "localhost:9092" or "kafka1:9092,kafka2:9092"
        // 2. bootstrap_servers: ["localhost:9092", "kafka2:9092"]
        let bootstrap_servers = if let Some(servers_value) = output_config.get("bootstrap_servers") {
            if let Some(servers_str) = servers_value.as_str() {
                // Single string, may contain comma-separated multiple addresses
                servers_str.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else if let Some(servers_seq) = servers_value.as_sequence() {
                // String list
                servers_seq.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid 'bootstrap_servers' format in Kafka output config",
                )) as Box<dyn std::error::Error + Send>);
            }
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing 'bootstrap_servers' in Kafka output config",
            )) as Box<dyn std::error::Error + Send>);
        };

        // Parse topic (single, required)
        let topic = output_config
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing or invalid 'topic' in Kafka output config (must be a string)",
                )) as Box<dyn std::error::Error + Send>
            })?
            .to_string();

        // Parse partition (single, optional)
        let partition = if let Some(partition_value) = output_config.get("partition") {
            Some(partition_value.as_i64()
                .ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid 'partition' format in Kafka output config (must be an integer)",
                    )) as Box<dyn std::error::Error + Send>
                })? as i32)
        } else {
            None
        };

        // Parse properties
        let mut properties = HashMap::new();
        if let Some(props) = output_config.get("properties").and_then(|v| v.as_mapping()) {
            for (key, value) in props {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_str()) {
                    properties.insert(k.to_string(), v.to_string());
                }
            }
        }

        Ok(Self::new(bootstrap_servers, topic, partition, properties))
    }
}

