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

use super::producer_config::PulsarProducerConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::output::output_protocol::OutputProtocol;
use pulsar::{Producer, Pulsar, TokioExecutor};
use std::cell::RefCell;

thread_local! {
    static PULSAR_OUT_RT: RefCell<Option<tokio::runtime::Runtime>> = const { RefCell::new(None) };
    static PULSAR_PRODUCER: RefCell<Option<Producer<TokioExecutor>>> = const { RefCell::new(None) };
}

pub struct PulsarOutputProtocol {
    config: PulsarProducerConfig,
}

impl PulsarOutputProtocol {
    pub fn new(config: PulsarProducerConfig) -> Self {
        Self { config }
    }
}

impl OutputProtocol for PulsarOutputProtocol {
    fn name(&self) -> String {
        format!("pulsar-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn send(&self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        if let Some(payload) = data.into_buffer() {
            PULSAR_OUT_RT.with(|rt_cell| {
                PULSAR_PRODUCER.with(
                    |producer_cell| -> Result<(), Box<dyn std::error::Error + Send>> {
                        let mut rt_opt = rt_cell.borrow_mut();
                        let mut producer_opt = producer_cell.borrow_mut();

                        if producer_opt.is_none() {
                            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                                Box::new(std::io::Error::other(e))
                                    as Box<dyn std::error::Error + Send>
                            })?;
                            let url = self.config.url.clone();
                            let topic = self.config.topic.clone();

                            let producer: Producer<TokioExecutor> = rt
                                .block_on(async {
                                    let pulsar =
                                        Pulsar::builder(&url, TokioExecutor).build().await?;
                                    let producer =
                                        pulsar.producer().with_topic(&topic).build().await?;
                                    Result::<_, pulsar::Error>::Ok(producer)
                                })
                                .map_err(|e| {
                                    Box::new(std::io::Error::other(e))
                                        as Box<dyn std::error::Error + Send>
                                })?;

                            *rt_opt = Some(rt);
                            *producer_opt = Some(producer);
                        }

                        let rt = rt_opt.as_ref().unwrap();
                        let producer = producer_opt.as_mut().unwrap();

                        rt.block_on(async {
                            producer
                                .create_message()
                                .with_content(payload)
                                .send_non_blocking()
                                .await
                                .map_err(|e| {
                                    Box::new(std::io::Error::other(e))
                                        as Box<dyn std::error::Error + Send>
                                })
                        })?;
                        Ok(())
                    },
                )
            })?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        PULSAR_OUT_RT.with(|rt_cell| {
            PULSAR_PRODUCER.with(|producer_cell| {
                let rt_opt = rt_cell.borrow();
                let mut producer_opt = producer_cell.borrow_mut();
                if let (Some(rt), Some(producer)) = (rt_opt.as_ref(), producer_opt.as_mut()) {
                    let _ = rt.block_on(producer.send_batch());
                }
            });
        });
        Ok(())
    }
}
