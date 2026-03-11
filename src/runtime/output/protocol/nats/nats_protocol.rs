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

use super::producer_config::NatsProducerConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::output::output_protocol::OutputProtocol;
use std::sync::Mutex;

pub struct NatsOutputProtocol {
    config: NatsProducerConfig,
    connection: Mutex<Option<nats::Connection>>,
}

impl NatsOutputProtocol {
    pub fn new(config: NatsProducerConfig) -> Self {
        Self {
            config,
            connection: Mutex::new(None),
        }
    }
}

impl OutputProtocol for NatsOutputProtocol {
    fn name(&self) -> String {
        format!("nats-{}", self.config.subject)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let nc = nats::connect(&self.config.url)
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;
        *self.connection.lock().unwrap() = Some(nc);
        Ok(())
    }

    fn send(&self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        if let Some(payload) = data.into_buffer() {
            let lock = self.connection.lock().unwrap();
            let nc = lock.as_ref().ok_or_else(|| {
                Box::new(std::io::Error::other("NATS connection not initialized"))
                    as Box<dyn std::error::Error + Send>
            })?;
            nc.publish(&self.config.subject, &payload).map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let lock = self.connection.lock().unwrap();
        if let Some(nc) = lock.as_ref() {
            nc.flush().map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?;
        }
        Ok(())
    }
}
