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

use crate::runtime::input::Input;
use crate::runtime::task::InputConfig;

pub struct InputProvider;

impl InputProvider {
    pub fn from_input_configs(
        input_configs: &[InputConfig],
        group_idx: usize,
    ) -> Result<Vec<Box<dyn Input>>, Box<dyn std::error::Error + Send>> {
        if input_configs.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Empty input configs list for input group #{}",
                    group_idx + 1
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

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

        let mut inputs = Vec::new();
        for (input_idx, input_config) in input_configs.iter().enumerate() {
            let input = Self::from_input_config(input_config, group_idx, input_idx)?;
            inputs.push(input);
        }

        Ok(inputs)
    }

    fn from_input_config(
        input_config: &InputConfig,
        group_idx: usize,
        input_idx: usize,
    ) -> Result<Box<dyn Input>, Box<dyn std::error::Error + Send>> {
        match input_config {
            InputConfig::Kafka {
                bootstrap_servers,
                topic,
                partition,
                group_id,
                extra,
            } => {
                use crate::runtime::input::InputRunner;
                use crate::runtime::input::protocol::kafka::{KafkaConfig, KafkaProtocol};

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

                let partition_i32 = partition.map(|p| p as i32);
                let properties = extra.clone();
                let kafka_config = KafkaConfig::new(
                    servers,
                    topic.clone(),
                    partition_i32,
                    group_id.clone(),
                    properties,
                );

                Ok(Box::new(InputRunner::new(
                    KafkaProtocol::new(kafka_config),
                    group_idx,
                    input_idx,
                )))
            }
        }
    }
}
