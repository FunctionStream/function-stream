// OutputSinkProvider - Output sink provider
//
// Creates OutputSink instances from configuration objects

use crate::runtime::output::OutputSink;
use crate::runtime::task::OutputConfig;

/// OutputSinkProvider - Output sink provider
///
/// Creates OutputSink instances from configuration objects
pub struct OutputSinkProvider;

impl OutputSinkProvider {
    /// Create multiple OutputSink from OutputConfig list
    ///
    /// # Arguments
    /// - `output_configs`: OutputConfig list
    ///
    /// # Returns
    /// - `Ok(Vec<Box<dyn OutputSink>>)`: Successfully created output sink list
    /// - `Err(...)`: Configuration parsing or creation failed
    pub fn from_output_configs(
        output_configs: &[OutputConfig],
    ) -> Result<Vec<Box<dyn OutputSink>>, Box<dyn std::error::Error + Send>> {
        if output_configs.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty output configs list",
            )) as Box<dyn std::error::Error + Send>);
        }

        // Check output sink count limit (maximum 64)
        const MAX_OUTPUTS: usize = 64;
        if output_configs.len() > MAX_OUTPUTS {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Too many outputs: {} (maximum is {})",
                    output_configs.len(),
                    MAX_OUTPUTS
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Create OutputSink for each OutputConfig
        let mut outputs = Vec::new();
        for (sink_idx, output_config) in output_configs.iter().enumerate() {
            let output = Self::from_output_config(output_config, sink_idx)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    /// Create OutputSink from a single OutputConfig
    ///
    /// # Arguments
    /// - `output_config`: Output configuration
    /// - `sink_idx`: Output sink index (used to identify different output sinks)
    ///
    /// # Returns
    /// - `Ok(Box<dyn OutputSink>)`: Successfully created output sink
    /// - `Err(...)`: Parsing failed
    fn from_output_config(
        output_config: &OutputConfig,
        sink_idx: usize,
    ) -> Result<Box<dyn OutputSink>, Box<dyn std::error::Error + Send>> {
        match output_config {
            OutputConfig::Kafka {
                bootstrap_servers,
                topic,
                partition,
                extra,
            } => {
                use crate::runtime::output::protocol::kafka::{
                    KafkaOutputSink, KafkaProducerConfig,
                };

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
                            "Invalid bootstrap_servers in output config: empty or invalid (topic: {})",
                            topic
                        ),
                    )) as Box<dyn std::error::Error + Send>);
                }

                // Convert partition from u32 to Option<i32>
                let partition_opt = Some(*partition as i32);

                // Merge extra configuration into properties
                let properties = extra.clone();

                // Create KafkaProducerConfig
                let kafka_config =
                    KafkaProducerConfig::new(servers, topic.clone(), partition_opt, properties);

                // Create KafkaOutputSink, passing sink_idx
                Ok(Box::new(KafkaOutputSink::from_config(
                    kafka_config,
                    sink_idx,
                )))
            }
        }
    }
}
