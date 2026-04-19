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

use super::producer_config::KafkaProducerConfig;
use crate::runtime::output::output_protocol::OutputProtocol;
use crate::runtime::wasm::buffer_and_event::BufferOrEvent;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use std::sync::Mutex;
use std::time::Duration;

const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 5000;

pub struct KafkaOutputProtocol {
    config: KafkaProducerConfig,
    producer: Mutex<Option<ThreadedProducer<DefaultProducerContext>>>,
}

impl KafkaOutputProtocol {
    pub fn new(config: KafkaProducerConfig) -> Self {
        Self {
            config,
            producer: Mutex::new(None),
        }
    }
}

impl OutputProtocol for KafkaOutputProtocol {
    fn name(&self) -> String {
        format!("kafka-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let p = self.config.create_producer().map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Failed to create Kafka producer: {}",
                e
            ))) as Box<dyn std::error::Error + Send>
        })?;
        *self.producer.lock().unwrap() = Some(p);
        Ok(())
    }

    fn send(&self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        if let Some(payload) = data.into_buffer() {
            let mut record: BaseRecord<'_, (), Vec<u8>> =
                BaseRecord::to(&self.config.topic).payload(&payload);
            if let Some(part) = self.config.partition {
                record = record.partition(part);
            }

            let lock = self.producer.lock().unwrap();
            let producer = lock.as_ref().ok_or_else(|| {
                Box::new(std::io::Error::other("Kafka producer not initialized"))
                    as Box<dyn std::error::Error + Send>
            })?;

            producer.send(record).map_err(|(e, _)| {
                Box::new(std::io::Error::other(format!("Kafka send error: {}", e)))
                    as Box<dyn std::error::Error + Send>
            })?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let lock = self.producer.lock().unwrap();
        if let Some(p) = lock.as_ref() {
            let _ = p.flush(Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS));
        }
        Ok(())
    }

    fn on_checkpoint(&self, _id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.flush()
    }
}
