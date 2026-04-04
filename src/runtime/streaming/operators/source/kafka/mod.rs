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

use anyhow::{Context as _, Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter as GovernorRateLimiter};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{SourceEvent, SourceOffset, SourceOperator};
use crate::runtime::streaming::format::{BadDataPolicy, DataDeserializer, Format};
use crate::sql::common::fs_schema::FieldValueType;
use crate::sql::common::{CheckpointBarrier, MetadataField};
// ============================================================================
// ============================================================================

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

pub trait BatchDeserializer: Send + 'static {
    fn deserialize_slice(
        &mut self,
        payload: &[u8],
        timestamp: u64,
        metadata: Option<HashMap<&str, FieldValueType<'_>>>,
    ) -> Result<()>;

    fn should_flush(&self) -> bool;

    fn flush_buffer(&mut self) -> Result<Option<RecordBatch>>;

    fn is_empty(&self) -> bool;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

pub struct BufferedDeserializer {
    inner: DataDeserializer,
    buffer: Vec<Vec<u8>>,
    /// Parallel to `buffer`: Kafka message timestamp (ms) per row for filling `_timestamp`.
    kafka_timestamps_ms: Vec<u64>,
    batch_size: usize,
}

impl BufferedDeserializer {
    pub fn new(
        format: Format,
        schema: SchemaRef,
        bad_data_policy: BadDataPolicy,
        batch_size: usize,
    ) -> Self {
        Self {
            inner: DataDeserializer::new(format, schema, bad_data_policy),
            buffer: Vec::with_capacity(batch_size),
            kafka_timestamps_ms: Vec::with_capacity(batch_size),
            batch_size,
        }
    }
}

impl BatchDeserializer for BufferedDeserializer {
    fn deserialize_slice(
        &mut self,
        payload: &[u8],
        timestamp: u64,
        _metadata: Option<HashMap<&str, FieldValueType<'_>>>,
    ) -> Result<()> {
        self.buffer.push(payload.to_vec());
        self.kafka_timestamps_ms.push(timestamp);
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.buffer.len() >= self.batch_size
    }

    fn flush_buffer(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let refs: Vec<&[u8]> = self.buffer.iter().map(|v| v.as_slice()).collect();
        let batch = self
            .inner
            .deserialize_batch_with_kafka_timestamps(&refs, &self.kafka_timestamps_ms)?;
        self.buffer.clear();
        self.kafka_timestamps_ms.clear();
        Ok(Some(batch))
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl SourceOffset {
    fn rdkafka_offset(self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
            SourceOffset::Group => Offset::Stored,
        }
    }
}

// ============================================================================
// ============================================================================

const KAFKA_POLL_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_BATCH_LINGER_TIME: Duration = Duration::from_millis(500);

pub struct KafkaSourceOperator {
    pub topic: String,
    pub bootstrap_servers: String,
    pub group_id: Option<String>,
    pub group_id_prefix: Option<String>,
    pub offset_mode: SourceOffset,

    pub client_configs: HashMap<String, String>,
    pub messages_per_second: NonZeroU32,
    pub metadata_fields: Vec<MetadataField>,

    consumer: Option<StreamConsumer>,
    rate_limiter: Option<DefaultDirectRateLimiter>,
    deserializer: Box<dyn BatchDeserializer>,

    current_offsets: HashMap<i32, i64>,
    is_empty_assignment: bool,

    last_flush_time: Instant,
}

impl KafkaSourceOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        bootstrap_servers: String,
        group_id: Option<String>,
        group_id_prefix: Option<String>,
        offset_mode: SourceOffset,
        client_configs: HashMap<String, String>,
        messages_per_second: NonZeroU32,
        metadata_fields: Vec<MetadataField>,
        deserializer: Box<dyn BatchDeserializer>,
    ) -> Self {
        Self {
            topic,
            bootstrap_servers,
            group_id,
            group_id_prefix,
            offset_mode,
            client_configs,
            messages_per_second,
            metadata_fields,
            consumer: None,
            rate_limiter: None,
            deserializer,
            current_offsets: HashMap::new(),
            is_empty_assignment: false,
            last_flush_time: Instant::now(),
        }
    }

    async fn init_and_assign_consumer(&mut self, ctx: &mut TaskContext) -> Result<()> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        let group_id = match (&self.group_id, &self.group_id_prefix) {
            (Some(gid), _) => gid.clone(),
            (None, Some(prefix)) => {
                format!("{}-fs-{}-{}", prefix, ctx.job_id, ctx.subtask_index)
            }
            (None, None) => format!("fs-{}-{}-consumer", ctx.job_id, ctx.subtask_index),
        };

        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            .set("group.id", &group_id)
            .create()?;

        let has_state = false;
        let state_map: HashMap<i32, KafkaState> = HashMap::new();

        let metadata = consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(30))
            .context("Failed to fetch Kafka metadata")?;

        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .ok_or_else(|| anyhow!("topic {} not in metadata", self.topic))?;

        let partitions = topic_meta.partitions();
        let mut our_partitions = HashMap::new();
        let pmax = ctx.parallelism.max(1) as i32;

        for p in partitions {
            if p.id().rem_euclid(pmax) == ctx.subtask_index as i32 {
                let offset = state_map
                    .get(&p.id())
                    .map(|s| Offset::Offset(s.offset))
                    .unwrap_or_else(|| {
                        if has_state {
                            Offset::Beginning
                        } else {
                            self.offset_mode.rdkafka_offset()
                        }
                    });
                our_partitions.insert((self.topic.clone(), p.id()), offset);
            }
        }

        if our_partitions.is_empty() {
            warn!(
                "[Task {}] Subscribed to no partitions. Entering idle mode.",
                ctx.subtask_index
            );
            self.is_empty_assignment = true;
        } else {
            let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions)?;
            consumer.assign(&topic_partitions)?;
        }

        self.consumer = Some(consumer);
        Ok(())
    }
}

// ============================================================================
// ============================================================================

#[async_trait]
impl SourceOperator for KafkaSourceOperator {
    fn name(&self) -> &str {
        &self.topic
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        self.init_and_assign_consumer(ctx).await?;
        self.rate_limiter = Some(GovernorRateLimiter::direct(Quota::per_second(
            self.messages_per_second,
        )));
        Ok(())
    }

    async fn fetch_next(&mut self, _ctx: &mut TaskContext) -> Result<SourceEvent> {
        if self.is_empty_assignment {
            return Ok(SourceEvent::Idle);
        }

        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow!("Kafka consumer not initialized"))?;
        let rate_limiter = self
            .rate_limiter
            .as_ref()
            .ok_or_else(|| anyhow!("rate limiter not initialized"))?;

        match tokio::time::timeout(KAFKA_POLL_TIMEOUT, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let partition = msg.partition();
                let offset = msg.offset();
                let timestamp = msg.timestamp().to_millis().ok_or_else(|| {
                    anyhow!("Failed to read timestamp from Kafka record: message has no timestamp")
                })?;

                self.current_offsets.insert(partition, offset);

                if let Some(payload) = msg.payload() {
                    let topic = msg.topic();

                    let connector_metadata = if !self.metadata_fields.is_empty() {
                        let mut meta = HashMap::new();
                        for f in &self.metadata_fields {
                            meta.insert(
                                f.field_name.as_str(),
                                match f.key.as_str() {
                                    "key" => FieldValueType::Bytes(msg.key()),
                                    "offset_id" => FieldValueType::Int64(Some(msg.offset())),
                                    "partition" => FieldValueType::Int32(Some(msg.partition())),
                                    "topic" => FieldValueType::String(Some(topic)),
                                    "timestamp" => FieldValueType::Int64(Some(timestamp)),
                                    _ => continue,
                                },
                            );
                        }
                        Some(meta)
                    } else {
                        None
                    };

                    self.deserializer.deserialize_slice(
                        payload,
                        timestamp.max(0) as u64,
                        connector_metadata,
                    )?;
                } else {
                    debug!(
                        "Received tombstone message at partition {} offset {}",
                        partition, offset
                    );
                }

                rate_limiter.until_ready().await;

                let should_flush_by_size = self.deserializer.should_flush();
                let should_flush_by_time = self.last_flush_time.elapsed() > MAX_BATCH_LINGER_TIME;

                if !self.deserializer.is_empty()
                    && (should_flush_by_size || should_flush_by_time)
                    && let Some(batch) = self.deserializer.flush_buffer()?
                {
                    self.last_flush_time = Instant::now();
                    return Ok(SourceEvent::Data(batch));
                }

                Ok(SourceEvent::Idle)
            }
            Ok(Err(e)) => {
                error!("Kafka recv error: {}", e);
                Err(anyhow!("Kafka error: {}", e))
            }
            Err(_) => {
                if !self.deserializer.is_empty()
                    && let Some(batch) = self.deserializer.flush_buffer()?
                {
                    self.last_flush_time = Instant::now();
                    return Ok(SourceEvent::Data(batch));
                }
                Ok(SourceEvent::Idle)
            }
        }
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<()> {
        debug!("Source [{}] executing checkpoint", ctx.subtask_index);

        let mut topic_partitions = TopicPartitionList::new();
        for (&partition, &offset) in &self.current_offsets {
            topic_partitions
                .add_partition_offset(&self.topic, partition, Offset::Offset(offset))
                .map_err(|e| anyhow!("add_partition_offset: {e}"))?;
        }

        if let Some(consumer) = &self.consumer
            && let Err(e) = consumer.commit(&topic_partitions, CommitMode::Async)
        {
            warn!("Failed to commit async offset to Kafka Broker: {:?}", e);
        }

        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("Kafka source shutting down gracefully");
        self.consumer.take();
        Ok(())
    }
}
