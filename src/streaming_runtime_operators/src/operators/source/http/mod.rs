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

//! HTTP source for robot / REST telemetry.
//!
//! - **poll** (Flink `url` + `scan.interval`): periodic GET/POST to robot APIs.
//! - **webhook** (`http.listen.port`): robots POST JSON payloads to Function Stream.

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    routing::post,
};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter as GovernorRateLimiter};
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::core::api::context::TaskContext;
use crate::core::api::source::{SourceCheckpointReport, SourceEvent, SourceOperator};
use crate::operators::source::kafka::BatchDeserializer;
use crate::sql::common::CheckpointBarrier;

const HTTP_WAIT_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_BATCH_LINGER_TIME: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpSourceMode {
    Poll,
    Webhook,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpResponseSplit {
    Single,
    Ndjson,
    JsonArray,
}

pub fn proto_mode_to_runtime(mode: i32) -> HttpSourceMode {
    use protocol::function_stream_graph::HttpSourceMode as ProtoMode;
    match ProtoMode::try_from(mode) {
        Ok(ProtoMode::HttpSourceWebhook) => HttpSourceMode::Webhook,
        _ => HttpSourceMode::Poll,
    }
}

pub fn proto_split_to_runtime(split: i32) -> HttpResponseSplit {
    use protocol::function_stream_graph::HttpResponseSplit as ProtoSplit;
    match ProtoSplit::try_from(split) {
        Ok(ProtoSplit::HttpResponseNdjson) => HttpResponseSplit::Ndjson,
        Ok(ProtoSplit::HttpResponseJsonArray) => HttpResponseSplit::JsonArray,
        _ => HttpResponseSplit::Single,
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct InboundMessage {
    payload: Vec<u8>,
    timestamp_ms: u64,
}

fn normalize_webhook_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

fn split_response_body(body: &[u8], split: HttpResponseSplit) -> Result<Vec<Vec<u8>>> {
    if body.is_empty() {
        return Ok(vec![]);
    }
    match split {
        HttpResponseSplit::Single => Ok(vec![body.to_vec()]),
        HttpResponseSplit::Ndjson => Ok(body
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| line.to_vec())
            .collect()),
        HttpResponseSplit::JsonArray => {
            let value: serde_json::Value =
                serde_json::from_slice(body).context("HTTP response is not valid JSON")?;
            match value {
                serde_json::Value::Array(items) => items
                    .into_iter()
                    .map(|v| serde_json::to_vec(&v))
                    .collect::<Result<Vec<_>, _>>()
                    .context("failed to encode JSON array element"),
                _ => Ok(vec![body.to_vec()]),
            }
        }
    }
}

async fn webhook_handler(
    State(tx): State<mpsc::Sender<InboundMessage>>,
    body: Bytes,
) -> StatusCode {
    if body.is_empty() {
        return StatusCode::OK;
    }
    let msg = InboundMessage {
        payload: body.to_vec(),
        timestamp_ms: wall_clock_ms(),
    };
    if tx.send(msg).await.is_err() {
        warn!("HTTP webhook channel closed; dropping payload");
    }
    StatusCode::OK
}

pub struct HttpSourceOperator {
    pub mode: HttpSourceMode,
    pub url: String,
    pub method: Method,
    pub scan_interval: Duration,
    pub request_body: Option<String>,
    pub headers: HashMap<String, String>,
    pub listen_host: String,
    pub listen_port: u16,
    pub webhook_path: String,
    pub request_timeout: Duration,
    pub response_split: HttpResponseSplit,
    pub messages_per_second: Option<NonZeroU32>,

    client: Option<Client>,
    rate_limiter: Option<DefaultDirectRateLimiter>,
    deserializer: Box<dyn BatchDeserializer>,
    last_flush_time: Instant,
    last_poll_time: Instant,

    inbound_rx: Option<mpsc::Receiver<InboundMessage>>,
    _server_task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpSourceOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mode: HttpSourceMode,
        url: String,
        method: Method,
        scan_interval: Duration,
        request_body: Option<String>,
        headers: HashMap<String, String>,
        listen_host: String,
        listen_port: u16,
        webhook_path: String,
        request_timeout: Duration,
        response_split: HttpResponseSplit,
        messages_per_second: Option<NonZeroU32>,
        deserializer: Box<dyn BatchDeserializer>,
    ) -> Self {
        Self {
            mode,
            url,
            method,
            scan_interval,
            request_body,
            headers,
            listen_host,
            listen_port,
            webhook_path,
            request_timeout,
            response_split,
            messages_per_second,
            client: None,
            rate_limiter: None,
            deserializer,
            last_flush_time: Instant::now(),
            last_poll_time: Instant::now()
                .checked_sub(scan_interval)
                .unwrap_or_else(Instant::now),
            inbound_rx: None,
            _server_task: None,
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
                flush_by_size = should_flush_by_size,
                flush_by_time = should_flush_by_time,
                reason,
                "http source emitting record batch"
            );
            return Ok(Some(SourceEvent::Data(batch)));
        }
        Ok(None)
    }

    fn ingest_payloads(&mut self, payloads: Vec<Vec<u8>>, timestamp_ms: u64) -> Result<()> {
        for payload in payloads {
            if payload.is_empty() {
                continue;
            }
            self.deserializer
                .deserialize_slice(&payload, timestamp_ms, None)?;
        }
        Ok(())
    }

    async fn poll_http_once(&mut self) -> Result<()> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("HTTP client not initialized"))?;

        let mut req = client.request(self.method.clone(), &self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(body) = &self.request_body {
            req = req.body(body.clone());
        }

        let response = req
            .send()
            .await
            .with_context(|| format!("HTTP {} {}", self.method, self.url))?;

        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .context("failed to read HTTP response body")?;

        if !status.is_success() {
            warn!(
                status = %status,
                url = %self.url,
                body_len = bytes.len(),
                "HTTP poll returned non-success status"
            );
            return Ok(());
        }

        debug!(
            url = %self.url,
            status = %status,
            body_len = bytes.len(),
            "http source poll response"
        );

        let chunks = split_response_body(&bytes, self.response_split)?;
        self.ingest_payloads(chunks, wall_clock_ms())?;
        Ok(())
    }

    async fn start_webhook_server(&mut self, ctx: &TaskContext) -> Result<()> {
        if ctx.parallelism > 1 {
            warn!(
                job_id = %ctx.job_id,
                subtask = ctx.subtask_index,
                parallelism = ctx.parallelism,
                "HTTP webhook source with parallelism > 1 will bind duplicate ports; use parallelism = 1"
            );
        }

        let path = normalize_webhook_path(&self.webhook_path);
        let addr = format!("{}:{}", self.listen_host, self.listen_port);
        info!(
            addr = %addr,
            path = %path,
            "Starting HTTP webhook server for robot telemetry"
        );

        let (tx, rx) = mpsc::channel::<InboundMessage>(4096);
        let app = Router::new()
            .route(&path, post(webhook_handler))
            .with_state(tx);

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind HTTP webhook on {addr}"))?;

        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                error!("HTTP webhook server error: {e}");
            }
        });

        self.inbound_rx = Some(rx);
        self._server_task = Some(server);
        Ok(())
    }

    async fn start_poll_client(&mut self) -> Result<()> {
        info!(
            url = %self.url,
            method = %self.method,
            interval_ms = self.scan_interval.as_millis(),
            "Starting HTTP poll source for robot API"
        );
        let client = Client::builder()
            .timeout(self.request_timeout)
            .build()
            .context("failed to build HTTP client")?;
        self.client = Some(client);
        Ok(())
    }
}

#[async_trait]
impl SourceOperator for HttpSourceOperator {
    fn name(&self) -> &str {
        match self.mode {
            HttpSourceMode::Poll => &self.url,
            HttpSourceMode::Webhook => &self.webhook_path,
        }
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        match self.mode {
            HttpSourceMode::Poll => self.start_poll_client().await?,
            HttpSourceMode::Webhook => self.start_webhook_server(ctx).await?,
        }
        self.rate_limiter = self.messages_per_second.map(|qps| {
            GovernorRateLimiter::direct(Quota::per_second(qps))
        });
        Ok(())
    }

    async fn fetch_next(&mut self, _ctx: &mut TaskContext) -> Result<SourceEvent> {
        loop {
            if let Some(event) = self.try_emit_buffered_batch("linger")? {
                return Ok(event);
            }

            match self.mode {
                HttpSourceMode::Webhook => {
                    let rx = self
                        .inbound_rx
                        .as_mut()
                        .ok_or_else(|| anyhow!("HTTP webhook receiver not initialized"))?;

                    match tokio::time::timeout(HTTP_WAIT_TIMEOUT, rx.recv()).await {
                        Ok(Some(msg)) => {
                            let chunks =
                                split_response_body(&msg.payload, self.response_split)?;
                            self.ingest_payloads(chunks, msg.timestamp_ms)?;

                            if let Some(rl) = &self.rate_limiter {
                                rl.until_ready().await;
                            }

                            if let Some(event) = self.try_emit_buffered_batch("webhook")? {
                                return Ok(event);
                            }
                        }
                        Ok(None) => {
                            return Err(anyhow!("HTTP webhook channel closed"));
                        }
                        Err(_) => {
                            if let Some(event) =
                                self.try_emit_buffered_batch("webhook timeout")?
                            {
                                return Ok(event);
                            }
                            return Ok(SourceEvent::Idle);
                        }
                    }
                }
                HttpSourceMode::Poll => {
                    if self.last_poll_time.elapsed() >= self.scan_interval {
                        if let Err(e) = self.poll_http_once().await {
                            error!("HTTP poll error: {e:#}");
                            return Err(e);
                        }
                        self.last_poll_time = Instant::now();

                        if let Some(rl) = &self.rate_limiter {
                            rl.until_ready().await;
                        }

                        if let Some(event) = self.try_emit_buffered_batch("poll")? {
                            return Ok(event);
                        }
                    } else {
                        if let Some(event) =
                            self.try_emit_buffered_batch("poll wait")?
                        {
                            return Ok(event);
                        }
                        return Ok(SourceEvent::Idle);
                    }
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
            mode = ?self.mode,
            "HTTP source checkpoint (no offset state in v1)"
        );
        Ok(SourceCheckpointReport::default())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("HTTP source shutting down");
        if let Some(task) = self._server_task.take() {
            task.abort();
        }
        self.client.take();
        self.inbound_rx.take();
        Ok(())
    }
}
