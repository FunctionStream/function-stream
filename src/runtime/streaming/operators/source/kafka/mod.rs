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

//! Kafka source checkpointing: `enable.auto.commit=false`, offsets captured at the checkpoint barrier
//! and reported to the job coordinator for catalog persistence; restart rewinds from that snapshot.

use anyhow::{Context as _, Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter as GovernorRateLimiter};
use protocol::storage::{KafkaPartitionOffset, KafkaSourceSubtaskCheckpoint};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{
    SourceCheckpointReport, SourceEvent, SourceOffset, SourceOperator,
};
use crate::runtime::streaming::format::{BadDataPolicy, DataDeserializer, Format};
use crate::sql::common::fs_schema::FieldValueType;
use crate::sql::common::{CheckpointBarrier, MetadataField};
// ============================================================================
// ============================================================================

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    pub partition: i32,
    pub offset: i64,
}

/// Last committed partition offsets for this source subtask, tied to a checkpoint epoch.
/// Materialized into a `.bin` under the job state dir from catalog before restart; see
/// [`TaskContext::latest_safe_epoch`] and `StreamingTableDefinition` in `storage.proto`.
#[derive(Debug, Encode, Decode)]
pub(crate) struct KafkaSourceSavedOffsets {
    /// Same numbering as [`CheckpointBarrier::epoch`] / catalog `latest_checkpoint_epoch` (as u64).
    pub(crate) epoch: u64,
    pub(crate) partitions: Vec<KafkaState>,
}

pub(crate) fn encode_kafka_offset_snapshot(saved: &KafkaSourceSavedOffsets) -> Result<Vec<u8>> {
    bincode::encode_to_vec(saved, bincode::config::standard())
        .map_err(|e| anyhow!("bincode encode Kafka offset snapshot: {e}"))
}

pub(crate) fn decode_kafka_offset_snapshot(bytes: &[u8]) -> Result<KafkaSourceSavedOffsets> {
    let (saved, _) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| anyhow!("bincode decode Kafka offset snapshot: {e}"))?;
    Ok(saved)
}

pub(crate) fn kafka_snapshot_path(
    job_dir: &std::path::Path,
    pipeline_id: u32,
    subtask_index: u32,
) -> PathBuf {
    job_dir.join(format!(
        "kafka_source_offsets_pipe{}_sub{}.bin",
        pipeline_id, subtask_index
    ))
}

fn kafka_offsets_snapshot_path(ctx: &TaskContext) -> PathBuf {
    kafka_snapshot_path(&ctx.state_dir, ctx.pipeline_id, ctx.subtask_index)
}

fn load_saved_offsets_if_recovering(ctx: &TaskContext) -> Option<KafkaSourceSavedOffsets> {
    let safe = ctx.latest_safe_epoch();
    if safe == 0 {
        return None;
    }
    let path = kafka_offsets_snapshot_path(ctx);
    let bytes = std::fs::read(&path).ok()?;
    let saved = match decode_kafka_offset_snapshot(&bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "Failed to decode Kafka offset snapshot"
            );
            return None;
        }
    };
    if saved.epoch > safe {
        warn!(
            path = %path.display(),
            saved_epoch = saved.epoch,
            safe_epoch = safe,
            "Ignoring Kafka offset snapshot newer than catalog safe epoch"
        );
        return None;
    }
    Some(saved)
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

    async fn init_and_assign_consumer(
        &mut self,
        ctx: &mut TaskContext,
        saved_offsets: Option<KafkaSourceSavedOffsets>,
    ) -> Result<()> {
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

        let (has_state, state_map) = if let Some(saved) = saved_offsets {
            info!(
                job_id = %ctx.job_id,
                pipeline_id = ctx.pipeline_id,
                subtask = ctx.subtask_index,
                epoch = saved.epoch,
                safe_epoch = ctx.latest_safe_epoch(),
                partitions = saved.partitions.len(),
                "Restoring Kafka source offsets from materialized checkpoint snapshot"
            );
            let mut m = HashMap::with_capacity(saved.partitions.len());
            for s in saved.partitions {
                m.insert(s.partition, s);
            }
            (true, m)
        } else {
            (false, HashMap::new())
        };

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
                // `current_offsets` / snapshot store last consumed offset; resume at next offset.
                let offset = state_map
                    .get(&p.id())
                    .map(|s| Offset::Offset(s.offset.saturating_add(1)))
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
        let saved = load_saved_offsets_if_recovering(ctx);
        self.init_and_assign_consumer(ctx, saved).await?;
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
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<SourceCheckpointReport> {
        debug!(
            "Source [{}] executing checkpoint epoch {}",
            ctx.subtask_index, barrier.epoch
        );

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

        let epoch = u64::from(barrier.epoch);
        let kafka_subtask = if self.current_offsets.is_empty() {
            None
        } else {
            let mut parts: Vec<(i32, i64)> =
                self.current_offsets.iter().map(|(&p, &o)| (p, o)).collect();
            parts.sort_by_key(|x| x.0);
            Some(KafkaSourceSubtaskCheckpoint {
                pipeline_id: ctx.pipeline_id,
                subtask_index: ctx.subtask_index,
                checkpoint_epoch: epoch,
                partitions: parts
                    .into_iter()
                    .map(|(partition, offset)| KafkaPartitionOffset { partition, offset })
                    .collect(),
            })
        };

        Ok(SourceCheckpointReport { kafka_subtask })
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("Kafka source shutting down gracefully");
        self.consumer.take();
        Ok(())
    }
}
