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

use anyhow::{Context, Result, anyhow, bail};
use arrow::compute::{
    concat_batches, filter_record_batch, kernels::cmp::gt_eq, lexsort_to_indices, partition, take,
};
use arrow::row::{RowConverter, SortField};
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{
    Array, ArrayRef, BooleanArray, PrimitiveArray, RecordBatch, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::info;

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::state::OperatorStateStore;
use crate::sql::common::converter::Converter;
use crate::sql::common::{
    CheckpointBarrier, FsSchema, FsSchemaRef, Watermark, from_nanos, to_nanos,
};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use crate::sql::schema::utils::window_arrow_struct;
use async_trait::async_trait;
use protocol::function_stream_graph::SessionWindowAggregateOperator;
// ============================================================================
// ============================================================================

struct SessionWindowConfig {
    gap: Duration,
    input_schema_ref: FsSchemaRef,
    window_field: FieldRef,
    window_index: usize,
    final_physical_exec: Arc<dyn ExecutionPlan>,
    receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    output_schema: Arc<Schema>,
}

struct ActiveSession {
    data_start: SystemTime,
    data_end: SystemTime,
    sender: Option<UnboundedSender<RecordBatch>>,
    result_stream: SendableRecordBatchStream,
}

impl ActiveSession {
    async fn new(
        aggregation_plan: Arc<dyn ExecutionPlan>,
        initial_timestamp: SystemTime,
        sender: UnboundedSender<RecordBatch>,
    ) -> Result<Self> {
        aggregation_plan.reset()?;
        let result_exec = aggregation_plan.execute(0, SessionContext::new().task_ctx())?;
        Ok(Self {
            data_start: initial_timestamp,
            data_end: initial_timestamp,
            sender: Some(sender),
            result_stream: result_exec,
        })
    }

    fn ingest_batch(
        &mut self,
        batch: RecordBatch,
        gap: Duration,
        ts_idx: usize,
    ) -> Result<Option<(SystemTime, RecordBatch)>> {
        let ts_col = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("expected timestamp column"))?;
        let start_ts = ts_col.value(0);
        let end_ts = ts_col.value(batch.num_rows() - 1);

        let current_end_with_gap = to_nanos(self.data_end + gap) as i64;

        if end_ts < current_end_with_gap {
            self.data_end = self.data_end.max(from_nanos(end_ts as u128));
            self.data_start = self.data_start.min(from_nanos(start_ts as u128));
            self.sender
                .as_ref()
                .ok_or_else(|| anyhow!("session sender already closed"))?
                .send(batch)
                .map_err(|e| anyhow!("session channel send: {e}"))?;
            return Ok(None);
        }

        if current_end_with_gap < start_ts {
            return Ok(Some((from_nanos(start_ts as u128), batch)));
        }

        self.data_start = self.data_start.min(from_nanos(start_ts as u128));

        let mut split_idx = 1;
        while split_idx < batch.num_rows() {
            let val = ts_col.value(split_idx);
            if val < to_nanos(self.data_end) as i64 {
                split_idx += 1;
                continue;
            }
            if val < to_nanos(self.data_end + gap) as i64 {
                self.data_end = from_nanos(val as u128);
                split_idx += 1;
                continue;
            }
            break;
        }

        if split_idx == batch.num_rows() {
            self.sender
                .as_ref()
                .ok_or_else(|| anyhow!("session sender already closed"))?
                .send(batch)
                .map_err(|e| anyhow!("session channel send: {e}"))?;
            return Ok(None);
        }

        self.sender
            .as_ref()
            .ok_or_else(|| anyhow!("session sender already closed"))?
            .send(batch.slice(0, split_idx))
            .map_err(|e| anyhow!("session channel send: {e}"))?;
        let remaining_batch = batch.slice(split_idx, batch.num_rows() - split_idx);
        let new_start_time = from_nanos(ts_col.value(split_idx) as u128);
        Ok(Some((new_start_time, remaining_batch)))
    }

    async fn close_and_drain(mut self, gap: Duration) -> Result<SessionWindowResult> {
        self.sender.take();

        let mut result_batches = Vec::new();
        while let Some(batch) = self.result_stream.next().await {
            result_batches.push(batch?);
        }

        if result_batches.len() != 1 || result_batches[0].num_rows() != 1 {
            bail!("active session must yield exactly one aggregate row");
        }

        Ok(SessionWindowResult {
            window_start: self.data_start,
            window_end: self.data_end + gap,
            batch: result_batches.into_iter().next().unwrap(),
        })
    }
}

#[derive(Clone)]
struct SessionWindowResult {
    window_start: SystemTime,
    window_end: SystemTime,
    batch: RecordBatch,
}

struct KeySessionState {
    config: Arc<SessionWindowConfig>,
    active_session: Option<ActiveSession>,
    buffered_batches: BTreeMap<SystemTime, Vec<RecordBatch>>,
}

impl KeySessionState {
    fn new(config: Arc<SessionWindowConfig>) -> Self {
        Self {
            config,
            active_session: None,
            buffered_batches: BTreeMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.active_session.is_none() && self.buffered_batches.is_empty()
    }

    fn earliest_data_time(&self) -> Option<SystemTime> {
        self.active_session
            .as_ref()
            .map(|s| s.data_start)
            .or_else(|| self.buffered_batches.keys().next().copied())
    }

    fn next_watermark_action_time(&self) -> Option<SystemTime> {
        self.active_session
            .as_ref()
            .map(|s| s.data_end + self.config.gap)
            .or_else(|| {
                self.buffered_batches
                    .keys()
                    .next()
                    .map(|t| *t - self.config.gap)
            })
    }

    async fn advance_by_watermark(
        &mut self,
        watermark: SystemTime,
    ) -> Result<Vec<SessionWindowResult>> {
        let mut results = vec![];

        loop {
            if let Some(session) = &mut self.active_session {
                if session.data_end + self.config.gap < watermark {
                    let closed_session = self
                        .active_session
                        .take()
                        .unwrap()
                        .close_and_drain(self.config.gap)
                        .await?;
                    results.push(closed_session);
                } else {
                    break;
                }
            } else {
                let Some((initial_ts, _)) = self.buffered_batches.first_key_value() else {
                    break;
                };
                if watermark + self.config.gap < *initial_ts {
                    break;
                }

                let (tx, rx) = unbounded_channel();
                *self.config.receiver_hook.write().unwrap() = Some(rx);

                self.active_session = Some(
                    ActiveSession::new(self.config.final_physical_exec.clone(), *initial_ts, tx)
                        .await?,
                );

                self.drain_buffer_to_active_session()?;
            }
        }
        Ok(results)
    }

    fn drain_buffer_to_active_session(&mut self) -> Result<()> {
        let session = self
            .active_session
            .as_mut()
            .ok_or_else(|| anyhow!("drain_buffer_to_active_session without active session"))?;

        while let Some((first_key, _)) = self.buffered_batches.first_key_value() {
            if session.data_end + self.config.gap < *first_key {
                break;
            }

            let (_, batches) = self.buffered_batches.pop_first().unwrap();
            for batch in batches {
                if let Some((rem_start, rem_batch)) = session.ingest_batch(
                    batch,
                    self.config.gap,
                    self.config.input_schema_ref.timestamp_index,
                )? {
                    self.buffered_batches
                        .entry(rem_start)
                        .or_default()
                        .push(rem_batch);
                }
            }
        }
        Ok(())
    }

    async fn add_data(
        &mut self,
        start_time: SystemTime,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<()> {
        self.buffered_batches
            .entry(start_time)
            .or_default()
            .push(batch);

        if self.active_session.is_some() {
            self.drain_buffer_to_active_session()?;
        }

        if let Some(wm) = watermark {
            let flushed = self.advance_by_watermark(wm).await?;
            if !flushed.is_empty() {
                bail!(
                    "unexpected flush during data ingestion; session watermark invariant violated"
                );
            }
        }
        Ok(())
    }
}

fn start_time_for_sorted_batch(batch: &RecordBatch, schema: &FsSchema) -> SystemTime {
    let timestamp_array = batch.column(schema.timestamp_index);
    let timestamp_array = timestamp_array
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .expect("timestamp column");
    from_nanos(timestamp_array.value(0) as u128)
}

/// Appends the stream `_timestamp` column (see [`build_session_output_schema`]) using each
/// session's `window_end` as the row event time.
fn append_output_timestamp_column(
    columns: &mut Vec<ArrayRef>,
    session_results: &[SessionWindowResult],
    ts_field: &Field,
) -> Result<()> {
    let nanos = |r: &SessionWindowResult| to_nanos(r.window_end) as i64 - 1;
    match ts_field.data_type() {
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let v: Vec<i64> = session_results
                .iter()
                .map(|r| nanos(r) / 1_000_000_000)
                .collect();
            columns.push(Arc::new(
                TimestampSecondArray::from(v).with_timezone_opt(tz.clone()),
            ));
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let v: Vec<i64> = session_results
                .iter()
                .map(|r| nanos(r) / 1_000_000)
                .collect();
            columns.push(Arc::new(
                TimestampMillisecondArray::from(v).with_timezone_opt(tz.clone()),
            ));
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let v: Vec<i64> = session_results.iter().map(|r| nanos(r) / 1000).collect();
            columns.push(Arc::new(
                TimestampMicrosecondArray::from(v).with_timezone_opt(tz.clone()),
            ));
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let v: Vec<i64> = session_results.iter().map(nanos).collect();
            columns.push(Arc::new(
                TimestampNanosecondArray::from(v).with_timezone_opt(tz.clone()),
            ));
        }
        dt => bail!("unsupported timestamp type for session window output: {dt}"),
    }
    Ok(())
}

fn build_session_output_schema(
    input: &FsSchema,
    window_field: FieldRef,
    window_index: usize,
    agg_schema: &Schema,
) -> Result<Arc<Schema>> {
    let key_count = input.routing_keys().map(|k| k.len()).unwrap_or(0);
    let mut fields: Vec<FieldRef> = (0..key_count)
        .map(|i| input.schema.fields()[i].clone())
        .collect();
    fields.insert(window_index, window_field);
    fields.extend(agg_schema.fields().iter().cloned());
    fields.push(input.schema.fields()[input.timestamp_index].clone());
    Ok(Arc::new(Schema::new(fields)))
}

// ============================================================================
// ============================================================================

pub struct SessionWindowOperator {
    config: Arc<SessionWindowConfig>,
    row_converter: Converter,

    session_states: HashMap<Vec<u8>, KeySessionState>,
    pq_watermark_actions: BTreeMap<SystemTime, HashSet<Vec<u8>>>,
    pq_start_times: BTreeMap<SystemTime, HashSet<Vec<u8>>>,

    // LSM-Tree state engine and per-routing-key timestamp index
    state_store: Option<Arc<OperatorStateStore>>,
    pending_timestamps: HashMap<Vec<u8>, BTreeSet<u64>>,
}

impl SessionWindowOperator {
    // State key: [RoutingKey bytes] + [8-byte big-endian timestamp]
    fn build_state_key(routing_key: &[u8], ts_nanos: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(routing_key.len() + 8);
        key.extend_from_slice(routing_key);
        key.extend_from_slice(&ts_nanos.to_be_bytes());
        key
    }

    fn extract_timestamp(key: &[u8]) -> Option<u64> {
        if key.len() >= 8 {
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&key[key.len() - 8..]);
            Some(u64::from_be_bytes(ts_bytes))
        } else {
            None
        }
    }

    fn extract_routing_key(key: &[u8]) -> Vec<u8> {
        if key.len() >= 8 {
            key[..key.len() - 8].to_vec()
        } else {
            Vec::new()
        }
    }

    fn filter_batch_by_time(
        &self,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<RecordBatch> {
        let Some(watermark) = watermark else {
            return Ok(batch);
        };

        let timestamp_column = batch
            .column(self.config.input_schema_ref.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("expected timestamp column"))?;

        let watermark_scalar = TimestampNanosecondArray::new_scalar(to_nanos(watermark) as i64);
        let on_time = gt_eq(timestamp_column, &watermark_scalar)?;

        Ok(filter_record_batch(&batch, &on_time)?)
    }

    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let sort_columns = self.config.input_schema_ref.sort_columns(batch, true);
        let sort_indices = lexsort_to_indices(&sort_columns, None)?;

        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &sort_indices, None).unwrap())
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    async fn ingest_sorted_batch(
        &mut self,
        sorted_batch: RecordBatch,
        watermark: Option<SystemTime>,
        is_recovery_replay: bool,
    ) -> Result<()> {
        let partition_ranges = if !self.config.input_schema_ref.has_routing_keys() {
            std::iter::once(0..sorted_batch.num_rows()).collect::<Vec<_>>()
        } else {
            let key_len = self
                .config
                .input_schema_ref
                .routing_keys()
                .as_ref()
                .unwrap()
                .len();
            let key_cols = sorted_batch
                .columns()
                .iter()
                .take(key_len)
                .cloned()
                .collect::<Vec<_>>();
            partition(key_cols.as_slice())?.ranges()
        };

        let key_count = self
            .config
            .input_schema_ref
            .routing_keys()
            .map(|k| k.len())
            .unwrap_or(0);

        for range in partition_ranges {
            let key_batch = sorted_batch.slice(range.start, range.end - range.start);

            let row_key = if key_count == 0 {
                Vec::new()
            } else {
                self.row_converter
                    .convert_columns(&key_batch.slice(0, 1).columns()[0..key_count])
                    .context("row key convert")?
                    .as_ref()
                    .to_vec()
            };

            // Write-ahead persistence: skip during recovery replay to avoid duplicate writes
            if !is_recovery_replay {
                let ts_col = key_batch
                    .column(self.config.input_schema_ref.timestamp_index)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let ts_nanos = ts_col.value(0) as u64;

                let state_key = Self::build_state_key(&row_key, ts_nanos);
                let store = self
                    .state_store
                    .as_ref()
                    .expect("State store not initialized");

                store
                    .put(state_key, key_batch.clone())
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

                self.pending_timestamps
                    .entry(row_key.clone())
                    .or_default()
                    .insert(ts_nanos);
            }

            let state = self
                .session_states
                .entry(row_key.clone())
                .or_insert_with(|| KeySessionState::new(self.config.clone()));

            let initial_action = state.next_watermark_action_time();
            let initial_start = state.earliest_data_time();

            let batch_start =
                start_time_for_sorted_batch(&key_batch, &self.config.input_schema_ref);

            state.add_data(batch_start, key_batch, watermark).await?;

            let new_action = state
                .next_watermark_action_time()
                .ok_or_else(|| anyhow!("missing next watermark action after add_data"))?;
            let new_start = state
                .earliest_data_time()
                .ok_or_else(|| anyhow!("missing earliest data after add_data"))?;

            match initial_action {
                Some(ia) => {
                    if ia != new_action {
                        self.pq_watermark_actions
                            .get_mut(&ia)
                            .expect("pq watermark entry")
                            .remove(&row_key);
                        self.pq_watermark_actions
                            .entry(new_action)
                            .or_default()
                            .insert(row_key.clone());
                    }
                    let is = initial_start.expect("initial start");
                    if is != new_start {
                        self.pq_start_times
                            .get_mut(&is)
                            .expect("pq start entry")
                            .remove(&row_key);
                        self.pq_start_times
                            .entry(new_start)
                            .or_default()
                            .insert(row_key.clone());
                    }
                }
                None => {
                    self.pq_watermark_actions
                        .entry(new_action)
                        .or_default()
                        .insert(row_key.clone());
                    self.pq_start_times
                        .entry(new_start)
                        .or_default()
                        .insert(row_key);
                }
            }
        }
        Ok(())
    }

    async fn evaluate_watermark_with_meta(
        &mut self,
        watermark: SystemTime,
    ) -> Result<Vec<(Vec<u8>, Vec<SessionWindowResult>)>> {
        let mut emit_results: Vec<(Vec<u8>, Vec<SessionWindowResult>)> = Vec::new();

        loop {
            let popped_action_time = match self.pq_watermark_actions.first_key_value() {
                Some((t, _)) if *t < watermark => *t,
                _ => break,
            };
            let keys = self
                .pq_watermark_actions
                .remove(&popped_action_time)
                .expect("pop watermark pq");

            for key in keys {
                let state = self
                    .session_states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing session state for key"))?;
                let initial_start = state
                    .earliest_data_time()
                    .ok_or_else(|| anyhow!("missing earliest data in evaluate_watermark"))?;

                let completed_sessions = state.advance_by_watermark(watermark).await?;
                if !completed_sessions.is_empty() {
                    emit_results.push((key.clone(), completed_sessions));
                }

                self.pq_start_times
                    .get_mut(&initial_start)
                    .expect("pq start")
                    .remove(&key);

                if state.is_empty() {
                    self.session_states.remove(&key);
                } else {
                    let new_start = state.earliest_data_time().expect("earliest after advance");
                    self.pq_start_times
                        .entry(new_start)
                        .or_default()
                        .insert(key.clone());

                    let new_next_action = state
                        .next_watermark_action_time()
                        .expect("next action after advance");
                    if new_next_action == popped_action_time {
                        bail!(
                            "processed watermark at {:?} but next watermark action stayed at {:?}",
                            watermark,
                            popped_action_time
                        );
                    }
                    self.pq_watermark_actions
                        .entry(new_next_action)
                        .or_default()
                        .insert(key);
                }
            }
        }

        Ok(emit_results)
    }

    fn format_to_arrow(
        &self,
        results: Vec<(Vec<u8>, Vec<SessionWindowResult>)>,
    ) -> Result<RecordBatch> {
        let (rows, session_results): (Vec<_>, Vec<_>) = results
            .into_iter()
            .flat_map(|(row, s_results)| s_results.into_iter().map(move |res| (row.clone(), res)))
            .unzip();

        let key_columns = if let Some(parser) = self.row_converter.parser() {
            self.row_converter
                .convert_rows(rows.iter().map(|row| parser.parse(row.as_ref())).collect())?
        } else {
            vec![]
        };

        let start_times: Vec<i64> = session_results
            .iter()
            .map(|r| to_nanos(r.window_start) as i64)
            .collect();
        let end_times: Vec<i64> = session_results
            .iter()
            .map(|r| to_nanos(r.window_end) as i64)
            .collect();

        let window_start_array = PrimitiveArray::<TimestampNanosecondType>::from(start_times);
        let window_end_array = PrimitiveArray::<TimestampNanosecondType>::from(end_times.clone());

        let result_batches: Vec<&RecordBatch> =
            session_results.iter().map(|res| &res.batch).collect();
        let merged_batch = concat_batches(&session_results[0].batch.schema(), result_batches)?;

        let DataType::Struct(window_fields) = self.config.window_field.data_type() else {
            bail!("expected window field to be a struct");
        };

        let window_struct_array = StructArray::try_new(
            window_fields.clone(),
            vec![Arc::new(window_start_array), Arc::new(window_end_array)],
            None,
        )?;

        let mut columns = key_columns;
        columns.insert(self.config.window_index, Arc::new(window_struct_array));
        columns.extend_from_slice(merged_batch.columns());

        let ts_field = self
            .config
            .input_schema_ref
            .schema
            .field(self.config.input_schema_ref.timestamp_index);
        append_output_timestamp_column(&mut columns, &session_results, ts_field)?;

        RecordBatch::try_new(self.config.output_schema.clone(), columns)
            .context("failed to create session window output batch")
    }

    #[allow(dead_code)]
    fn earliest_batch_time(&self) -> Option<SystemTime> {
        self.pq_start_times
            .first_key_value()
            .map(|(start_time, _keys)| *start_time)
    }
}

#[async_trait]
impl Operator for SessionWindowOperator {
    fn name(&self) -> &str {
        "SessionWindow"
    }

    // Recovery & event sourcing: rebuild in-memory sessions from LSM-Tree
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        let store = OperatorStateStore::new(
            ctx.pipeline_id,
            ctx.state_dir.clone(),
            ctx.memory_controller.clone(),
            ctx.io_manager.clone(),
        )
        .map_err(|e| anyhow!("Failed to init state store: {e}"))?;

        let safe_epoch = ctx.latest_safe_epoch();
        let active_keys = store
            .restore_metadata(safe_epoch)
            .await
            .map_err(|e| anyhow!("State recovery failed: {e}"))?;

        if !active_keys.is_empty() {
            info!(
                pipeline_id = ctx.pipeline_id,
                key_count = active_keys.len(),
                "Session Operator recovering active state keys from LSM-Tree..."
            );

            let mut recovered_batches = Vec::new();

            for key in active_keys {
                if let Some(ts) = Self::extract_timestamp(&key) {
                    let row_key = Self::extract_routing_key(&key);
                    self.pending_timestamps
                        .entry(row_key)
                        .or_default()
                        .insert(ts);
                }

                let batches = store.get_batches(&key).await.map_err(|e| anyhow!("{e}"))?;
                recovered_batches.extend(batches);
            }

            // Temporal ordering is critical: replay must preserve watermark/session merge invariants
            recovered_batches.sort_by_key(|b| {
                b.column(self.config.input_schema_ref.timestamp_index)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .map(|ts| ts.value(0))
                    .unwrap_or(0)
            });

            for batch in recovered_batches {
                self.ingest_sorted_batch(batch, None, true).await?;
            }

            info!(
                pipeline_id = ctx.pipeline_id,
                "Session Window Operator successfully replayed events and rebuilt in-memory sessions."
            );
        }

        self.state_store = Some(store);
        Ok(())
    }

    // Write-ahead: persist raw data before in-memory ingestion
    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let watermark_time = ctx.current_watermark();

        let filtered_batch = self.filter_batch_by_time(batch, watermark_time)?;
        if filtered_batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let sorted_batch = self.sort_batch(&filtered_batch)?;

        self.ingest_sorted_batch(sorted_batch, watermark_time, false)
            .await?;

        Ok(vec![])
    }

    // Watermark-driven session closure with precise LSM-Tree garbage collection
    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let Watermark::EventTime(current_time) = watermark else {
            return Ok(vec![]);
        };

        let completed_sessions = self.evaluate_watermark_with_meta(current_time).await?;
        if completed_sessions.is_empty() {
            return Ok(vec![]);
        }

        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        // GC: tombstone expired raw data covered by closed sessions
        for (row_key, session_results) in &completed_sessions {
            if let Some(ts_set) = self.pending_timestamps.get_mut(row_key) {
                for session_res in session_results {
                    let start_nanos = to_nanos(session_res.window_start) as u64;
                    let end_nanos = to_nanos(session_res.window_end - self.config.gap) as u64;

                    let expired_ts: Vec<u64> =
                        ts_set.range(start_nanos..=end_nanos).copied().collect();

                    for ts in expired_ts {
                        let state_key = Self::build_state_key(row_key, ts);
                        store
                            .remove_batches(state_key)
                            .map_err(|e| anyhow!("{e}"))?;
                        ts_set.remove(&ts);
                    }
                }
            }
        }

        let output_batch = self.format_to_arrow(completed_sessions)?;
        Ok(vec![StreamOutput::Forward(output_batch)])
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        self.state_store
            .as_ref()
            .expect("State store not initialized")
            .snapshot_epoch(barrier.epoch as u64)
            .map_err(|e| anyhow!("Snapshot failed: {e}"))?;

        info!(
            epoch = barrier.epoch,
            "Session Window Operator snapshotted state."
        );
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// ============================================================================

pub struct SessionAggregatingWindowConstructor;

impl SessionAggregatingWindowConstructor {
    pub fn with_config(
        &self,
        config: SessionWindowAggregateOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<SessionWindowOperator> {
        let window_field = Arc::new(Field::new(
            config.window_field_name,
            window_arrow_struct(),
            true,
        ));

        let receiver_hook = Arc::new(RwLock::new(None));

        let codec = StreamingExtensionCodec {
            context: StreamingDecodingContext::UnboundedBatchStream(receiver_hook.clone()),
        };

        let final_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;
        let final_execution_plan = final_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        let input_schema: FsSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("missing input schema"))?
            .try_into()?;

        let row_converter = if input_schema.routing_keys().is_none() {
            let array = Arc::new(BooleanArray::from(vec![false]));
            Converter::Empty(
                RowConverter::new(vec![SortField::new(DataType::Boolean)])?,
                array,
            )
        } else {
            let key_count = input_schema.routing_keys().as_ref().unwrap().len();
            Converter::RowConverter(RowConverter::new(
                input_schema
                    .schema
                    .fields()
                    .into_iter()
                    .take(key_count)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )?)
        };

        let output_schema = build_session_output_schema(
            &input_schema,
            window_field.clone(),
            config.window_index as usize,
            final_execution_plan.schema().as_ref(),
        )?;

        let session_config = Arc::new(SessionWindowConfig {
            gap: Duration::from_micros(config.gap_micros),
            window_field,
            window_index: config.window_index as usize,
            input_schema_ref: Arc::new(input_schema),
            final_physical_exec: final_execution_plan,
            receiver_hook,
            output_schema,
        });

        Ok(SessionWindowOperator {
            config: session_config,
            session_states: HashMap::new(),
            pq_start_times: BTreeMap::new(),
            pq_watermark_actions: BTreeMap::new(),
            row_converter,
            state_store: None,
            pending_timestamps: HashMap::new(),
        })
    }
}
