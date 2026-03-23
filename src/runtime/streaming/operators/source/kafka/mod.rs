//! Kafka 源算子：实现 [`crate::runtime::streaming::api::source::SourceOperator`]，由 [`crate::runtime::streaming::execution::SourceRunner`] 轮询 `fetch_next`。

use anyhow::{anyhow, Context as _, Result};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter as GovernorRateLimiter};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use arrow_array::RecordBatch;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{SourceEvent, SourceOffset, SourceOperator};
use crate::sql::common::{CheckpointBarrier, MetadataField};
// ============================================================================
// 1. 领域模型：Kafka 状态与配置
// ============================================================================

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

/// 模拟 Arroyo 原版的 Deserializer Buffer
/// （工业实现中，反序列化常带 buffer，满 N 条或超时后吐出一个 [`RecordBatch`]）。
pub trait BatchDeserializer: Send + 'static {
    fn deserialize_slice(
        &mut self,
        payload: &[u8],
        timestamp: u64,
        metadata: Option<HashMap<&str, FieldValueType<'_>>>,
    ) -> Result<()>;

    fn should_flush(&self) -> bool;

    fn flush_buffer(&mut self) -> Result<Option<RecordBatch>>;
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
// 2. 核心算子外壳
// ============================================================================

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
}

impl KafkaSourceOperator {
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
        }
    }

    async fn init_and_assign_consumer(&mut self, ctx: &mut TaskContext) -> Result<()> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        let group_id = match (&self.group_id, &self.group_id_prefix) {
            (Some(gid), _) => gid.clone(),
            (None, Some(prefix)) => {
                format!("{}-arroyo-{}-{}", prefix, ctx.job_id, ctx.subtask_idx)
            }
            (None, None) => format!("arroyo-{}-{}-consumer", ctx.job_id, ctx.subtask_idx),
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

        let (has_state, state_map) = {
            let mut tm = ctx.table_manager_guard().await?;
            let global_state = tm
                .get_global_keyed_state::<i32, KafkaState>("k")
                .await
                .map_err(|e| anyhow!(e))?;
            let restored_states: Vec<_> = global_state.get_all().values().copied().collect();
            let has_state = !restored_states.is_empty();
            let state_map: HashMap<i32, KafkaState> =
                restored_states.into_iter().map(|s| (s.partition, s)).collect();
            (has_state, state_map)
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
            if p.id().rem_euclid(pmax) == ctx.subtask_idx as i32 {
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
                ctx.subtask_idx
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
// 3. 实现 SourceOperator 协议
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

        let recv_result = tokio::time::timeout(Duration::from_millis(50), consumer.recv()).await;

        match recv_result {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    let timestamp = msg.timestamp().to_millis().unwrap_or(0);
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

                    self.current_offsets.insert(msg.partition(), msg.offset());

                    rate_limiter.until_ready().await;

                    if self.deserializer.should_flush() {
                        if let Some(batch) = self.deserializer.flush_buffer()? {
                            return Ok(SourceEvent::Data(batch));
                        }
                    }
                }
                Ok(SourceEvent::Idle)
            }
            Ok(Err(e)) => {
                error!("Kafka recv error: {}", e);
                Err(anyhow!("Kafka error: {}", e))
            }
            Err(_) => {
                if self.deserializer.should_flush() {
                    if let Some(batch) = self.deserializer.flush_buffer()? {
                        return Ok(SourceEvent::Data(batch));
                    }
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
        debug!("Source [{}] executing checkpoint", ctx.subtask_idx);

        let mut tm = ctx.table_manager_guard().await?;
        let global_state = tm
            .get_global_keyed_state::<i32, KafkaState>("k")
            .await
            .map_err(|e| anyhow!(e))?;

        let mut topic_partitions = TopicPartitionList::new();

        for (&partition, &offset) in &self.current_offsets {
            global_state
                .insert(
                    partition,
                    KafkaState {
                        partition,
                        offset: offset + 1,
                    },
                )
                .await;

            topic_partitions
                .add_partition_offset(&self.topic, partition, Offset::Offset(offset))
                .map_err(|e| anyhow!("add_partition_offset: {e}"))?;
        }

        if let Some(consumer) = &self.consumer {
            if let Err(e) = consumer.commit(&topic_partitions, CommitMode::Async) {
                warn!("Failed to commit async offset to Kafka Broker: {:?}", e);
            }
        }

        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!("Kafka source shutting down gracefully");
        self.consumer.take();
        Ok(())
    }
}
