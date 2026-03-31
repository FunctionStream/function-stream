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


use anyhow::{anyhow, bail, Result};
use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, TimeUnit};
use async_trait::async_trait;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::format::DataSerializer;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark};
// ============================================================================
// ============================================================================

#[derive(Debug, Clone)]
pub enum ConsistencyMode {
    AtLeastOnce,
    ExactlyOnce,
}

struct TransactionalState {
    next_transaction_index: usize,
    active_producer: FutureProducer,
    producer_awaiting_commit: Option<FutureProducer>,
}

// ============================================================================
// ============================================================================

pub struct KafkaSinkOperator {
    pub topic: String,
    pub bootstrap_servers: String,
    pub consistency_mode: ConsistencyMode,
    pub client_config: HashMap<String, String>,

    pub input_schema: FsSchema,
    pub timestamp_col_idx: Option<usize>,
    pub key_col_idx: Option<usize>,

    pub serializer: DataSerializer,

    at_least_once_producer: Option<FutureProducer>,
    transactional_state: Option<TransactionalState>,

    write_futures: Vec<DeliveryFuture>,
}

impl KafkaSinkOperator {
    pub fn new(
        topic: String,
        bootstrap_servers: String,
        consistency_mode: ConsistencyMode,
        client_config: HashMap<String, String>,
        input_schema: FsSchema,
        serializer: DataSerializer,
    ) -> Self {
        Self {
            topic,
            bootstrap_servers,
            consistency_mode,
            client_config,
            input_schema,
            timestamp_col_idx: None,
            key_col_idx: None,
            serializer,
            at_least_once_producer: None,
            transactional_state: None,
            write_futures: Vec::new(),
        }
    }

    fn resolve_schema_indices(&mut self) {
        self.timestamp_col_idx = Some(self.input_schema.timestamp_index);

        if let Some(routing_keys) = self.input_schema.routing_keys() {
            if !routing_keys.is_empty() {
                self.key_col_idx = Some(routing_keys[0]);
            }
        }
    }

    fn create_producer(&self, ctx: &TaskContext, tx_index: Option<usize>) -> Result<FutureProducer> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);

        for (k, v) in &self.client_config {
            config.set(k, v);
        }

        if let Some(idx) = tx_index {
            config.set("enable.idempotence", "true");
            let transactional_id = format!(
                "fs-tx-{}-{}-{}-{}",
                ctx.job_id, self.topic, ctx.subtask_idx, idx
            );
            config.set("transactional.id", &transactional_id);

            let producer: FutureProducer = config.create()?;
            producer
                .init_transactions(Timeout::After(Duration::from_secs(30)))
                .map_err(|e| anyhow!("Failed to init Kafka transactions: {}", e))?;
            producer
                .begin_transaction()
                .map_err(|e| anyhow!("Failed to begin Kafka transaction: {}", e))?;

            Ok(producer)
        } else {
            Ok(config.create()?)
        }
    }

    async fn flush_to_broker(&mut self) -> Result<()> {
        let producer = self.current_producer();

        producer.poll(Timeout::After(Duration::ZERO));

        for future in self.write_futures.drain(..) {
            match future.await {
                Ok(Ok(_)) => continue,
                Ok(Err((e, _))) => bail!("Kafka producer delivery failed: {}", e),
                Err(_) => bail!("Kafka delivery future canceled"),
            }
        }
        Ok(())
    }

    fn current_producer(&self) -> &FutureProducer {
        match &self.consistency_mode {
            ConsistencyMode::AtLeastOnce => self.at_least_once_producer.as_ref().unwrap(),
            ConsistencyMode::ExactlyOnce => &self.transactional_state.as_ref().unwrap().active_producer,
        }
    }
}

fn event_timestamp_ms(batch: &RecordBatch, row: usize, col: usize) -> Option<i64> {
    let arr = batch.column(col);
    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = arr.as_primitive::<arrow_array::types::TimestampSecondType>();
            (!a.is_null(row)).then(|| a.value(row) * 1000)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = arr.as_primitive::<arrow_array::types::TimestampMillisecondType>();
            (!a.is_null(row)).then(|| a.value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
            (!a.is_null(row)).then(|| a.value(row) / 1000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr.as_primitive::<arrow_array::types::TimestampNanosecondType>();
            (!a.is_null(row)).then(|| a.value(row) / 1_000_000)
        }
        _ => None,
    }
}

fn row_key_bytes(batch: &RecordBatch, row: usize, col: usize) -> Option<Vec<u8>> {
    let arr = batch.column(col);
    match arr.data_type() {
        DataType::Utf8 => {
            let s = arr.as_string::<i32>();
            if s.is_null(row) {
                None
            } else {
                Some(s.value(row).as_bytes().to_vec())
            }
        }
        DataType::LargeUtf8 => {
            let s = arr.as_string::<i64>();
            if s.is_null(row) {
                None
            } else {
                Some(s.value(row).as_bytes().to_vec())
            }
        }
        _ => None,
    }
}

// ============================================================================
// ============================================================================

#[async_trait]
impl Operator for KafkaSinkOperator {
    fn name(&self) -> &str {
        factory_operator_name::KAFKA_SINK
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        self.resolve_schema_indices();

        match self.consistency_mode {
            ConsistencyMode::AtLeastOnce => {
                self.at_least_once_producer = Some(self.create_producer(ctx, None)?);
            }
            ConsistencyMode::ExactlyOnce => {
                let mut next_idx = 0usize;

                let active_producer = self.create_producer(ctx, Some(next_idx))?;
                next_idx += 1;

                self.transactional_state = Some(TransactionalState {
                    next_transaction_index: next_idx,
                    active_producer,
                    producer_awaiting_commit: None,
                });
            }
        }
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let payloads = self.serializer.serialize(&batch)?;
        let producer = self.current_producer().clone();

        for (i, payload) in payloads.iter().enumerate() {
            let ts_millis = self
                .timestamp_col_idx
                .and_then(|idx| event_timestamp_ms(&batch, i, idx));
            let key_bytes = self
                .key_col_idx
                .and_then(|idx| row_key_bytes(&batch, i, idx));

            let mut record = FutureRecord::<Vec<u8>, Vec<u8>>::to(&self.topic).payload(&payload);
            if let Some(ts) = ts_millis {
                record = record.timestamp(ts);
            }
            if let Some(ref k) = key_bytes {
                record = record.key(k);
            }

            loop {
                match producer.send_result(record) {
                    Ok(delivery_future) => {
                        self.write_futures.push(delivery_future);
                        break;
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned_record)) => {
                        record = returned_record;
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err((e, _)) => bail!("Fatal Kafka send error: {}", e),
                }
            }
        }

        Ok(vec![])
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<()> {
        self.flush_to_broker().await?;

        if matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce) {
            let next_tx = self
                .transactional_state
                .as_ref()
                .map(|s| s.next_transaction_index)
                .unwrap();
            let new_producer = self.create_producer(ctx, Some(next_tx))?;

            let state = self.transactional_state.as_mut().unwrap();
            let old_producer = std::mem::replace(&mut state.active_producer, new_producer);
            state.producer_awaiting_commit = Some(old_producer);

            state.next_transaction_index += 1;
        }

        Ok(())
    }

    async fn commit_checkpoint(&mut self, epoch: u32, _ctx: &mut TaskContext) -> Result<()> {
        if matches!(self.consistency_mode, ConsistencyMode::AtLeastOnce) {
            return Ok(());
        }

        let state = self.transactional_state.as_mut().unwrap();
        let Some(committing_producer) = state.producer_awaiting_commit.take() else {
            warn!(
                "Received Commit for epoch {}, but no stashed producer exists. Possibly a recovery duplicate.",
                epoch
            );
            return Ok(());
        };

        let mut retries = 0;
        loop {
            match committing_producer.commit_transaction(Timeout::After(Duration::from_secs(10))) {
                Ok(_) => {
                    info!("Successfully committed Kafka transaction for epoch {}", epoch);
                    break;
                }
                Err(e) => {
                    retries += 1;
                    if retries >= 5 {
                        bail!(
                            "Failed to commit Kafka transaction after 5 retries. Fatal error: {}",
                            e
                        );
                    }
                    warn!(
                        "Failed to commit Kafka transaction (Attempt {}/5): {}. Retrying...",
                        retries, e
                    );
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }

        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        self.flush_to_broker().await?;
        info!("Kafka sink shut down gracefully.");
        Ok(vec![])
    }
}
