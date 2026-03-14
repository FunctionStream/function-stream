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

use crate::runtime::output::Output;
use crate::runtime::task::OutputConfig;

pub struct OutputProvider;

impl OutputProvider {
    pub fn from_output_configs(
        output_configs: &[OutputConfig],
    ) -> Result<Vec<Box<dyn Output>>, Box<dyn std::error::Error + Send>> {
        if output_configs.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty output configs list",
            )) as Box<dyn std::error::Error + Send>);
        }

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

        let mut outputs = Vec::new();
        for (output_idx, output_config) in output_configs.iter().enumerate() {
            let output = Self::from_output_config(output_config, output_idx)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    fn from_output_config(
        output_config: &OutputConfig,
        output_idx: usize,
    ) -> Result<Box<dyn Output>, Box<dyn std::error::Error + Send>> {
        match output_config {
            OutputConfig::Kafka {
                bootstrap_servers,
                topic,
                partition,
                extra,
                runtime: _,
            } => {
                use crate::runtime::output::output_runner::OutputRunner;
                use crate::runtime::output::protocol::kafka::{
                    KafkaOutputProtocol, KafkaProducerConfig,
                };

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

                let partition_opt = Some(*partition as i32);
                let properties = extra.clone();
                let kafka_config =
                    KafkaProducerConfig::new(servers, topic.clone(), partition_opt, properties);
                let protocol = KafkaOutputProtocol::new(kafka_config);
                let runtime = output_config.output_runtime_config();
                Ok(Box::new(OutputRunner::new(protocol, output_idx, runtime)))
            }
            OutputConfig::Nats {
                url,
                subject,
                extra,
                runtime: _,
            } => {
                use crate::runtime::output::output_runner::OutputRunner;
                use crate::runtime::output::protocol::nats::{
                    NatsOutputProtocol, NatsProducerConfig,
                };

                if url.is_empty() {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Invalid nats url in output config: empty (subject: {})",
                            subject
                        ),
                    )) as Box<dyn std::error::Error + Send>);
                }

                let nats_config =
                    NatsProducerConfig::new(url.clone(), subject.clone(), extra.clone());
                let protocol = NatsOutputProtocol::new(nats_config);
                let runtime = output_config.output_runtime_config();
                Ok(Box::new(OutputRunner::new(protocol, output_idx, runtime)))
            }
        }
    }
}
