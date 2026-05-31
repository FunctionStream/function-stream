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

//! MQTT source: subscribe on connect, batch deserialize like Kafka source.
//! Checkpoint v1 reports no offset state (at-least-once; use stable `client_id`
//! and `clean_session=false` on the broker if you need redelivery after restart).

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use crate::core::api::context::TaskContext;
use crate::core::api::source::{SourceCheckpointReport, SourceEvent, SourceOperator};
use crate::operators::source::batch_buffer::{MAX_BATCH_LINGER_TIME, SOURCE_POLL_TIMEOUT};
use crate::operators::source::kafka::BatchDeserializer;
use crate::sql::common::{CheckpointBarrier, MetadataField};

pub fn proto_qos_to_rumqtt(qos: u32) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct MqttSourceOperator {
    pub topic: String,
    pub host: String,
    pub port: u16,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub qos: QoS,
    pub clean_session: bool,
    pub keep_alive_secs: u64,
    pub metadata_fields: Vec<MetadataField>,

    _client: Option<AsyncClient>,
    eventloop: Option<EventLoop>,
    deserializer: Box<dyn BatchDeserializer>,
    last_flush_time: Instant,
}

impl MqttSourceOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        host: String,
        port: u16,
        client_id: Option<String>,
        username: Option<String>,
        password: Option<String>,
        qos: QoS,
        clean_session: bool,
        keep_alive_secs: u64,
        metadata_fields: Vec<MetadataField>,
        deserializer: Box<dyn BatchDeserializer>,
    ) -> Self {
        Self {
            topic,
            host,
            port,
            client_id,
            username,
            password,
            qos,
            clean_session,
            keep_alive_secs,
            metadata_fields,
            _client: None,
            eventloop: None,
            deserializer,
            last_flush_time: Instant::now(),
        }
    }

    fn try_emit_buffered_batch(&mut self, reason: &str) -> Result<Option<SourceEvent>> {
        let should_flush_by_size = self.deserializer.should_flush();
        let should_flush_by_time = self.last_flush_time.elapsed() > MAX_BATCH_LINGER_TIME;

        if !self.deserializer.is_empty()
            && (should_flush_by_size || should_flush_by_time)
            && let Some(batch) = self.deserializer.flush_buffer()?
        {
            self.last_flush_time = Instant::now();
            debug!(
                num_rows = batch.num_rows(),
                num_columns = batch.num_columns(),
                flush_by_size = should_flush_by_size,
                flush_by_time = should_flush_by_time,
                reason,
                "mqtt source emitting record batch"
            );
            return Ok(Some(SourceEvent::Data(batch)));
        }
        Ok(None)
    }

    async fn connect(&mut self, ctx: &mut TaskContext) -> Result<()> {
        if ctx.parallelism > 1 {
            warn!(
                job_id = %ctx.job_id,
                subtask = ctx.subtask_index,
                parallelism = ctx.parallelism,
                "MQTT source with pipeline parallelism > 1 will duplicate messages across subtasks; use parallelism = 1"
            );
        }

        let client_id = self.client_id.clone().unwrap_or_else(|| {
            format!("fs-{}-{}-mqtt", ctx.job_id, ctx.subtask_index)
        });

        info!(
            host = %self.host,
            port = self.port,
            topic = %self.topic,
            client_id = %client_id,
            "Creating MQTT client"
        );

        let mut mqttoptions = MqttOptions::new(client_id, &self.host, self.port);
        mqttoptions.set_keep_alive(std::time::Duration::from_secs(self.keep_alive_secs.max(1)));
        mqttoptions.set_clean_session(self.clean_session);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            mqttoptions.set_credentials(user, pass);
        }

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);
        client
            .subscribe(self.topic.clone(), self.qos)
            .await
            .with_context(|| format!("Failed to subscribe to MQTT topic '{}'", self.topic))?;

        self._client = Some(client);
        self.eventloop = Some(eventloop);
        Ok(())
    }
}

#[async_trait]
impl SourceOperator for MqttSourceOperator {
    fn name(&self) -> &str {
        &self.topic
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        self.connect(ctx).await?;
        Ok(())
    }

    async fn fetch_next(&mut self, _ctx: &mut TaskContext) -> Result<SourceEvent> {
        loop {
            if let Some(event) = self.try_emit_buffered_batch("linger")? {
                return Ok(event);
            }

            let poll_result = {
                let eventloop = self
                    .eventloop
                    .as_mut()
                    .ok_or_else(|| anyhow!("MQTT event loop not initialized"))?;
                tokio::time::timeout(SOURCE_POLL_TIMEOUT, eventloop.poll()).await
            };

            match poll_result {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    let payload = publish.payload.as_ref();
                    let timestamp_ms = wall_clock_ms();

                    if !payload.is_empty() {
                        debug!(
                            topic = %publish.topic,
                            payload_bytes = payload.len(),
                            qos = ?publish.qos,
                            "mqtt source consumed message"
                        );
                        let _ = &self.metadata_fields;
                        self.deserializer.deserialize_slice(
                            payload,
                            timestamp_ms,
                            None,
                        )?;
                    }

                    if let Some(event) = self.try_emit_buffered_batch("batch full")? {
                        return Ok(event);
                    }
                }
                Ok(Ok(_)) => {
                    // ConnAck, SubAck, PingResp, etc.
                }
                Ok(Err(e)) => {
                    error!("MQTT poll error: {}", e);
                    return Err(anyhow!("MQTT error: {}", e));
                }
                Err(_) => {
                    if let Some(event) = self.try_emit_buffered_batch("poll timeout")? {
                        return Ok(event);
                    }
                    return Ok(SourceEvent::Idle);
                }
            }
        }
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<SourceCheckpointReport> {
        debug!(
            subtask = ctx.subtask_index,
            epoch = barrier.epoch,
            "MQTT source checkpoint (no offset state in v1)"
        );
        Ok(SourceCheckpointReport::default())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("MQTT source shutting down");
        self._client.take();
        self.eventloop.take();
        Ok(())
    }
}
