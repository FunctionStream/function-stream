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

//! ROS source via [rosbridge](https://github.com/RobotWebTools/rosbridge_suite) WebSocket protocol.
//!
//! Connects to `rosbridge_websocket`, subscribes to a ROS topic, extracts the `msg` payload
//! (or a custom JSON field), and deserializes with the configured format.

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, info, warn};

use crate::core::api::context::TaskContext;
use crate::core::api::source::{SourceCheckpointReport, SourceEvent, SourceOperator};
use crate::operators::source::batch_buffer::{MAX_BATCH_LINGER_TIME, SOURCE_POLL_TIMEOUT};
use crate::operators::source::kafka::BatchDeserializer;
use crate::sql::common::CheckpointBarrier;

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn extract_ros_payload(msg: &Value, message_field: &str) -> Option<Vec<u8>> {
    let payload = msg.get(message_field).unwrap_or(msg);
    match payload {
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Object(_) | Value::Array(_) => serde_json::to_vec(payload).ok(),
        Value::Null => None,
        other => serde_json::to_vec(other).ok(),
    }
}

pub struct RosSourceOperator {
    pub url: String,
    pub topic: String,
    pub message_field: String,
    deserializer: Box<dyn BatchDeserializer>,
    last_flush_time: Instant,
    ws_write: Option<
        futures::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    >,
    ws_read: Option<
        futures::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        >,
    >,
}

impl RosSourceOperator {
    pub fn new(
        url: String,
        topic: String,
        message_field: String,
        deserializer: Box<dyn BatchDeserializer>,
    ) -> Self {
        Self {
            url,
            topic,
            message_field,
            deserializer,
            last_flush_time: Instant::now(),
            ws_write: None,
            ws_read: None,
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
                reason,
                "ros source emitting record batch"
            );
            return Ok(Some(SourceEvent::Data(batch)));
        }
        Ok(None)
    }

    async fn connect_and_subscribe(&mut self) -> Result<()> {
        info!(
            url = %self.url,
            topic = %self.topic,
            "Connecting to rosbridge WebSocket"
        );

        let request = self
            .url
            .as_str()
            .into_client_request()
            .context("invalid rosbridge WebSocket URL")?;

        let (ws, _) = connect_async(request)
            .await
            .with_context(|| format!("failed to connect rosbridge at {}", self.url))?;

        let (write, read) = ws.split();
        self.ws_write = Some(write);
        self.ws_read = Some(read);

        let subscribe = serde_json::json!({
            "op": "subscribe",
            "topic": self.topic,
        });
        let write = self
            .ws_write
            .as_mut()
            .ok_or_else(|| anyhow!("rosbridge write half not initialized"))?;
        write
            .send(Message::Text(subscribe.to_string().into()))
            .await
            .context("failed to send rosbridge subscribe")?;

        info!(topic = %self.topic, "Subscribed to ROS topic via rosbridge");
        Ok(())
    }

    fn handle_rosbridge_text(&mut self, text: &str) -> Result<bool> {
        let msg: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                debug!(error = %e, "ignoring non-JSON rosbridge frame");
                return Ok(false);
            }
        };

        let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("");
        if op != "publish" {
            return Ok(false);
        }

        let topic = msg.get("topic").and_then(|v| v.as_str()).unwrap_or("");
        if topic != self.topic {
            return Ok(false);
        }

        let Some(payload) = extract_ros_payload(&msg, &self.message_field) else {
            return Ok(false);
        };

        debug!(
            topic,
            payload_bytes = payload.len(),
            "ros source received message"
        );
        self.deserializer
            .deserialize_slice(&payload, wall_clock_ms(), None)?;
        Ok(true)
    }
}

#[async_trait]
impl SourceOperator for RosSourceOperator {
    fn name(&self) -> &str {
        &self.topic
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        if ctx.parallelism > 1 {
            warn!(
                subtask = ctx.subtask_index,
                parallelism = ctx.parallelism,
                "ROS source with parallelism > 1 duplicates rosbridge subscriptions; use parallelism = 1"
            );
        }
        self.connect_and_subscribe().await
    }

    async fn fetch_next(&mut self, _ctx: &mut TaskContext) -> Result<SourceEvent> {
        loop {
            if let Some(event) = self.try_emit_buffered_batch("linger")? {
                return Ok(event);
            }

            let read_result = {
                let read = self
                    .ws_read
                    .as_mut()
                    .ok_or_else(|| anyhow!("rosbridge read half not initialized"))?;
                timeout(SOURCE_POLL_TIMEOUT, read.next()).await
            };

            match read_result {
                Ok(Some(Ok(Message::Text(text)))) => {
                    if self.handle_rosbridge_text(text.as_ref())? {
                        if let Some(event) = self.try_emit_buffered_batch("batch full")? {
                            return Ok(event);
                        }
                    }
                }
                Ok(Some(Ok(Message::Binary(bin)))) => {
                    if let Ok(text) = std::str::from_utf8(&bin) {
                        if self.handle_rosbridge_text(text)? {
                            if let Some(event) = self.try_emit_buffered_batch("batch full")? {
                                return Ok(event);
                            }
                        }
                    }
                }
                Ok(Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_)))) => {}
                Ok(Some(Ok(Message::Close(_)))) => {
                    return Err(anyhow!("rosbridge WebSocket closed"));
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    error!("rosbridge WebSocket error: {e}");
                    return Err(anyhow!("rosbridge error: {e}"));
                }
                Ok(None) => {
                    return Err(anyhow!("rosbridge WebSocket stream ended"));
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
            "ROS source checkpoint (no offset state in v1)"
        );
        Ok(SourceCheckpointReport::default())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("ROS source shutting down");
        if let Some(mut write) = self.ws_write.take() {
            let _ = write.close().await;
        }
        self.ws_read.take();
        Ok(())
    }
}
