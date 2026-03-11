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

use super::config::PulsarConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::input::input_protocol::InputProtocol;
use futures::StreamExt;
use pulsar::consumer::SubType;
use pulsar::{Consumer, Pulsar, TokioExecutor};
use std::cell::RefCell;
use std::time::Duration;

thread_local! {
    static PULSAR_RT: RefCell<Option<tokio::runtime::Runtime>> = RefCell::new(None);
    static PULSAR_CONSUMER: RefCell<Option<Consumer<Vec<u8>, TokioExecutor>>> = RefCell::new(None);
}

pub struct PulsarProtocol {
    config: PulsarConfig,
}

impl PulsarProtocol {
    pub fn new(config: PulsarConfig) -> Self {
        Self { config }
    }
}

impl InputProtocol for PulsarProtocol {
    fn name(&self) -> String {
        format!("pulsar-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Lazy init is done in poll() on the worker thread which owns the runtime/consumer.
        Ok(())
    }

    fn poll(
        &self,
        timeout: Duration,
    ) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        PULSAR_RT.with(|rt_cell| {
            PULSAR_CONSUMER.with(|consumer_cell| {
                let mut rt_opt = rt_cell.borrow_mut();
                let mut consumer_opt = consumer_cell.borrow_mut();

                if consumer_opt.is_none() {
                    let rt = tokio::runtime::Runtime::new()
                        .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;
                    let url = self.config.url.clone();
                    let topic = self.config.topic.clone();
                    let subscription = self.config.subscription.clone();
                    let sub_type = self.config.subscription_type.as_deref().unwrap_or("Exclusive");
                    let sub_type_enum = match sub_type.to_lowercase().as_str() {
                        "shared" => SubType::Shared,
                        "key_shared" => SubType::KeyShared,
                        "failover" => SubType::Failover,
                        _ => SubType::Exclusive,
                    };

                    let consumer: Consumer<Vec<u8>, _> = rt
                        .block_on(async {
                            let pulsar = Pulsar::builder(&url, TokioExecutor).build().await?;
                            let mut builder = pulsar
                                .consumer()
                                .with_topic(&topic)
                                .with_subscription(&subscription)
                                .with_subscription_type(sub_type_enum);
                            let consumer = builder.build().await?;
                            Result::<_, pulsar::Error>::Ok(consumer)
                        })
                        .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;

                    *rt_opt = Some(rt);
                    *consumer_opt = Some(consumer);
                }

                let rt = rt_opt.as_ref().unwrap();
                let consumer = consumer_opt.as_mut().unwrap();

                let timeout_ms = timeout.as_millis() as u64;
                let topic = self.config.topic.clone();
                let result = rt.block_on(async {
                    let next_fut = consumer.next();
                    match tokio::time::timeout(Duration::from_millis(timeout_ms), next_fut).await {
                        Ok(Some(Ok(msg))) => {
                            let payload = msg.deserialize().unwrap_or_else(|_| msg.payload.data.clone());
                            let _ = consumer.ack(&msg).await;
                            Some(Ok(payload))
                        }
                        Ok(Some(Err(e))) => Some(Err(e)),
                        Ok(None) | Err(_) => None,
                    }
                });

                match result {
                    Some(Ok(payload)) => Ok(Some(BufferOrEvent::new_buffer(
                        payload,
                        Some(topic),
                        false,
                        false,
                    ))),
                    Some(Err(e)) => Err(Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>),
                    None => Ok(None),
                }
            })
        })
    }
}
