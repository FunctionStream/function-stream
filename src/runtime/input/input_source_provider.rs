// InputSourceProvider - Input source provider
//
// Creates InputSource instances from configuration objects

use crate::runtime::input::InputSource;
use crate::runtime::task::InputConfig;

/// InputSourceProvider - Input source provider
/// 
/// Creates InputSource instances from configuration objects
pub struct InputSourceProvider;

impl InputSourceProvider {
    /// Create multiple InputSource from InputConfig list
    /// 
    /// # Arguments
    /// - `input_configs`: InputConfig list
    /// - `group_idx`: Input group index (used to identify which group the input source belongs to)
    /// 
    /// # Returns
    /// - `Ok(Vec<Box<dyn InputSource>>)`: Successfully created input source list
    /// - `Err(...)`: Configuration parsing or creation failed
    pub fn from_input_configs(
        input_configs: &[InputConfig],
        group_idx: usize,
    ) -> Result<Vec<Box<dyn InputSource>>, Box<dyn std::error::Error + Send>> {
        if input_configs.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Empty input configs list for input group #{}", group_idx + 1),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Check input source count limit (maximum 64)
        const MAX_INPUTS: usize = 64;
        if input_configs.len() > MAX_INPUTS {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Too many inputs in group #{}: {} (maximum is {})",
                    group_idx + 1,
                    input_configs.len(),
                    MAX_INPUTS
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Create InputSource for each InputConfig
        let mut inputs = Vec::new();
        for (input_idx, input_config) in input_configs.iter().enumerate() {
            let input = Self::from_input_config(input_config, group_idx, input_idx)?;
            inputs.push(input);
        }

        Ok(inputs)
    }

    /// Create InputSource from single InputConfig
    /// 
    /// # Arguments
    /// - `input_config`: Input source configuration
    /// - `group_idx`: Input group index (used to identify which group the input source belongs to)
    /// - `input_idx`: Input source index within group (used to identify different input sources within the same group)
    /// 
    /// # Returns
    /// - `Ok(Box<dyn InputSource>)`: Successfully created input source
    /// - `Err(...)`: Parsing failed
    fn from_input_config(
        input_config: &InputConfig,
        group_idx: usize,
        input_idx: usize,
    ) -> Result<Box<dyn InputSource>, Box<dyn std::error::Error + Send>> {
        match input_config {
            InputConfig::Kafka { 
                bootstrap_servers, 
                topic, 
                partition, 
                group_id, 
                extra 
            } => {
                use crate::runtime::input::protocol::kafka::{KafkaConfig, KafkaInputSource};
                
                // Convert bootstrap_servers string to Vec<String>
                let servers: Vec<String> = bootstrap_servers
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                
                if servers.is_empty() {
                    return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                        format!(
                            "Invalid bootstrap_servers in input config (group #{}): empty or invalid (topic: {}, group_id: {})",
                            group_idx + 1,
                            topic,
                            group_id
                        ),
                    )) as Box<dyn std::error::Error + Send>);
                }
                
                // Convert partition from Option<u32> to Option<i32>
                let partition_i32 = partition.map(|p| p as i32);
                
                // Merge extra configuration into properties
                let properties = extra.clone();
                
                // Create KafkaConfig
                let kafka_config = KafkaConfig::new(
                    servers,
                    topic.clone(),
                    partition_i32,
                    group_id.clone(),
                    properties,
                );
                
                // Create KafkaInputSource, pass in group_idx and input_idx
                Ok(Box::new(KafkaInputSource::from_config(kafka_config, group_idx, input_idx)))
            }
        }
    }
}

