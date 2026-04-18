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

use super::config::KafkaConfig;
use crate::runtime::input::input_protocol::InputProtocol;
use crate::runtime::wasm::buffer_and_event::BufferOrEvent;
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::sync::OnceLock;
use std::time::Duration;

pub struct KafkaProtocol {
    config: KafkaConfig,
    consumer: OnceLock<BaseConsumer>,
}

impl KafkaProtocol {
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            consumer: OnceLock::new(),
        }
    }
}

impl InputProtocol for KafkaProtocol {
    fn name(&self) -> String {
        format!("kafka-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", self.config.bootstrap_servers_str());
        client_config.set("group.id", &self.config.group_id);
        client_config.set("enable.auto.commit", "false");

        for (k, v) in &self.config.properties {
            if k != "enable.auto.commit" {
                client_config.set(k, v);
            }
        }

        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;

        if let Some(p) = self.config.partition {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&self.config.topic, p);
            consumer.assign(&tpl).map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?;
        } else {
            consumer.subscribe(&[&self.config.topic]).map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?;
        }

        self.consumer.set(consumer).map_err(|_| {
            Box::new(std::io::Error::other("Consumer already init"))
                as Box<dyn std::error::Error + Send>
        })?;
        Ok(())
    }

    fn poll(
        &self,
        timeout: Duration,
    ) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        let consumer = self.consumer.get().ok_or_else(|| {
            Box::new(std::io::Error::other("Consumer not init"))
                as Box<dyn std::error::Error + Send>
        })?;

        match consumer.poll(timeout) {
            Some(Ok(m)) => {
                let payload = m.payload().unwrap_or_default().to_vec();
                Ok(Some(BufferOrEvent::new_buffer(
                    payload,
                    Some(self.config.topic.clone()),
                    false,
                    false,
                )))
            }
            Some(Err(e)) => Err(Box::new(std::io::Error::other(e))),
            None => Ok(None),
        }
    }

    fn on_start(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let consumer = self.consumer.get().ok_or_else(|| {
            Box::new(std::io::Error::other("Consumer not init"))
                as Box<dyn std::error::Error + Send>
        })?;
        consumer
            .fetch_metadata(Some(&self.config.topic), Duration::from_secs(5))
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;
        Ok(())
    }

    fn on_close(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
