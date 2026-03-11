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

use super::config::NatsConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::input::input_protocol::InputProtocol;
use std::sync::OnceLock;
use std::time::Duration;

pub struct NatsProtocol {
    config: NatsConfig,
    subscription: OnceLock<nats::Subscription>,
}

impl NatsProtocol {
    pub fn new(config: NatsConfig) -> Self {
        Self {
            config,
            subscription: OnceLock::new(),
        }
    }
}

impl InputProtocol for NatsProtocol {
    fn name(&self) -> String {
        format!("nats-{}", self.config.subject)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let nc = nats::connect(&self.config.url)
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;

        let sub = if let Some(q) = &self.config.queue_group {
            nc.queue_subscribe(&self.config.subject, q).map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?
        } else {
            nc.subscribe(&self.config.subject).map_err(|e| {
                Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>
            })?
        };

        self.subscription.set(sub).map_err(|_| {
            Box::new(std::io::Error::other("NATS subscription already init"))
                as Box<dyn std::error::Error + Send>
        })?;
        Ok(())
    }

    fn poll(
        &self,
        timeout: Duration,
    ) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        let sub = self.subscription.get().ok_or_else(|| {
            Box::new(std::io::Error::other("NATS subscription not init"))
                as Box<dyn std::error::Error + Send>
        })?;

        match sub.next_timeout(timeout) {
            Ok(msg) => {
                let payload = msg.data.to_vec();
                Ok(Some(BufferOrEvent::new_buffer(
                    payload,
                    Some(self.config.subject.clone()),
                    false,
                    false,
                )))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(None),
            Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send>),
        }
    }
}
