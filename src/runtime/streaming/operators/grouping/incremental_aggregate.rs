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

use crate::sql::common::constants::updating_state_field;
use anyhow::{Result, bail};
use arrow::compute::max_array;
use arrow::row::{RowConverter, SortField};
use arrow_array::builder::{
    BinaryBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder, TimeUnit};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use itertools::Itertools;
use prost::Message;
use protocol::grpc::api::UpdatingAggregateOperator;
use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime};
use std::{collections::HashMap, mem, sync::Arc};
use tracing::{debug, warn};
// =========================================================================
// =========================================================================
use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::operators::{Key, UpdatingCache};
use crate::runtime::util::decode_aggregate;
use crate::sql::common::{
    CheckpointBarrier, FsSchema, TIMESTAMP_FIELD, UPDATING_META_FIELD, Watermark, to_nanos,
};
use crate::sql::physical::updating_meta_fields;

#[derive(Debug, Copy, Clone)]
struct BatchData {
    count: u64,
    generation: u64,
}

impl BatchData {
    fn new(generation: u64) -> Self {
        Self {
            count: 1,
            generation,
        }
    }

    fn inc(&mut self) {
        self.count += 1;
        self.generation += 1;
    }

    fn dec(&mut self) {
        self.count = self.count.checked_sub(1).unwrap_or_default();
        self.generation += 1;
    }
}

#[derive(Debug)]
enum IncrementalState {
    Sliding {
        expr: Arc<AggregateFunctionExpr>,
        accumulator: Box<dyn Accumulator>,
    },
    Batch {
        expr: Arc<AggregateFunctionExpr>,
        data: HashMap<Key, BatchData>,
        row_converter: Arc<RowConverter>,
        changed_values: HashSet<Key>,
    },
}

impl IncrementalState {
    fn update_batch(&mut self, new_generation: u64, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => {
                accumulator.update_batch(batch)?;
            }
            IncrementalState::Batch {
                data,
                row_converter,
                changed_values,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        data.get_mut(r.as_ref()).unwrap().inc();
                        changed_values.insert(data.get_key_value(r.as_ref()).unwrap().0.clone());
                    } else {
                        let key = Key(Arc::new(r.as_ref().to_vec()));
                        data.insert(key.clone(), BatchData::new(new_generation));
                        changed_values.insert(key);
                    }
                }
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.retract_batch(batch),
            IncrementalState::Batch {
                data,
                row_converter,
                changed_values,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    match data.get(r.as_ref()).map(|d| d.count) {
                        Some(0) => {
                            debug!(
                                "tried to retract value for key with count 0; implies append lost"
                            );
                        }
                        Some(_) => {
                            data.get_mut(r.as_ref()).unwrap().dec();
                            changed_values
                                .insert(data.get_key_value(r.as_ref()).unwrap().0.clone());
                        }
                        None => {
                            debug!("tried to retract value for missing key: implies append lost");
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate(),
            IncrementalState::Batch {
                expr,
                data,
                row_converter,
                ..
            } => {
                let parser = row_converter.parser();
                let input = row_converter.convert_rows(
                    data.iter()
                        .filter(|(_, c)| c.count > 0)
                        .map(|(v, _)| parser.parse(&v.0)),
                )?;
                let mut acc = expr.create_accumulator()?;
                acc.update_batch(&input)?;
                acc.evaluate_mut()
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AccumulatorType {
    Sliding,
    Batch,
}

impl AccumulatorType {
    fn state_fields(&self, agg: &AggregateFunctionExpr) -> DFResult<Vec<FieldRef>> {
        Ok(match self {
            AccumulatorType::Sliding => agg.sliding_state_fields()?,
            AccumulatorType::Batch => vec![],
        })
    }
}

#[derive(Debug)]
struct Aggregator {
    func: Arc<AggregateFunctionExpr>,
    accumulator_type: AccumulatorType,
    row_converter: Arc<RowConverter>,
    state_cols: Vec<usize>,
}

// =========================================================================
// =========================================================================

pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Aggregator>,
    accumulators: UpdatingCache<Vec<IncrementalState>>,
    updated_keys: HashMap<Key, Option<Vec<ScalarValue>>>,

    input_schema: Arc<FsSchema>,
    has_routing_keys: bool,

    sliding_state_schema: Arc<FsSchema>,
    batch_state_schema: Arc<FsSchema>,
    schema_without_metadata: Arc<Schema>,
    final_output_schema: Arc<Schema>,
    ttl: Duration,
    key_converter: RowConverter,
    new_generation: u64,
}

static GLOBAL_KEY: LazyLock<Arc<Vec<u8>>> = LazyLock::new(|| Arc::new(Vec::new()));

impl IncrementalAggregatingFunc {
    fn update_batch(
        &mut self,
        key: &[u8],
        batch: &[Vec<ArrayRef>],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify_and_update(key, Instant::now(), |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    let values = if let Some(idx) = idx {
                        &inputs.iter().map(|c| c.slice(idx, 1)).collect()
                    } else {
                        inputs
                    };
                    accs.update_batch(self.new_generation, values)?;
                }
                Ok(())
            })
            .expect("tried to update for non-existent key")
    }

    fn retract_batch(
        &mut self,
        key: &[u8],
        batch: &[Vec<ArrayRef>],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify(key, |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    let values = if let Some(idx) = idx {
                        &inputs.iter().map(|c| c.slice(idx, 1)).collect()
                    } else {
                        inputs
                    };
                    accs.retract_batch(values)?;
                }
                Ok::<(), datafusion::common::DataFusionError>(())
            })
            .expect("tried to retract state for non-existent key")?;
        Ok(())
    }

    fn evaluate(&mut self, key: &[u8]) -> DFResult<Vec<ScalarValue>> {
        self.accumulators
            .get_mut(key)
            .expect("tried to evaluate non-existent key")
            .iter_mut()
            .map(|s| s.evaluate())
            .collect::<DFResult<_>>()
    }

    fn get_retracts(batch: &RecordBatch) -> Option<&BooleanArray> {
        if let Some(meta_col) = batch.column_by_name(UPDATING_META_FIELD) {
            let meta_struct = meta_col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("_updating_meta must be StructArray");

            let is_retract_array = meta_struct
                .column_by_name(updating_state_field::IS_RETRACT)
                .expect("meta struct must have is_retract");

            Some(
                is_retract_array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("is_retract must be BooleanArray"),
            )
        } else {
            None
        }
    }

    fn make_accumulators(&self) -> Vec<IncrementalState> {
        self.aggregates
            .iter()
            .map(|agg| match agg.accumulator_type {
                AccumulatorType::Sliding => IncrementalState::Sliding {
                    expr: agg.func.clone(),
                    accumulator: agg.func.create_sliding_accumulator().unwrap(),
                },
                AccumulatorType::Batch => IncrementalState::Batch {
                    expr: agg.func.clone(),
                    data: Default::default(),
                    row_converter: agg.row_converter.clone(),
                    changed_values: Default::default(),
                },
            })
            .collect()
    }

    fn compute_inputs(&self, batch: &RecordBatch) -> Vec<Vec<ArrayRef>> {
        self.aggregates
            .iter()
            .map(|agg| {
                agg.func
                    .expressions()
                    .iter()
                    .map(|ex| {
                        ex.evaluate(batch)
                            .unwrap()
                            .into_array(batch.num_rows())
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);
        let aggregate_input_cols = self.compute_inputs(batch);

        let mut first = false;
        if !self
            .accumulators
            .contains_key(GLOBAL_KEY.as_ref().as_slice())
        {
            first = true;
            self.accumulators.insert(
                GLOBAL_KEY.clone(),
                Instant::now(),
                self.new_generation,
                self.make_accumulators(),
            );
        }

        if !self
            .updated_keys
            .contains_key(GLOBAL_KEY.as_ref().as_slice())
        {
            if first {
                self.updated_keys.insert(Key(GLOBAL_KEY.clone()), None);
            } else {
                let v = Some(self.evaluate(GLOBAL_KEY.as_ref().as_slice())?);
                self.updated_keys.insert(Key(GLOBAL_KEY.clone()), v);
            }
        }

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                if r.unwrap_or_default() {
                    self.retract_batch(
                        GLOBAL_KEY.as_ref().as_slice(),
                        &aggregate_input_cols,
                        Some(i),
                    )?;
                } else {
                    self.update_batch(
                        GLOBAL_KEY.as_ref().as_slice(),
                        &aggregate_input_cols,
                        Some(i),
                    )?;
                }
            }
        } else {
            self.update_batch(GLOBAL_KEY.as_ref().as_slice(), &aggregate_input_cols, None)
                .unwrap();
        }
        Ok(())
    }

    fn keyed_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let sort_columns = &self
            .input_schema
            .sort_columns(batch, false)
            .into_iter()
            .map(|e| e.values)
            .collect::<Vec<_>>();

        let keys = self.key_converter.convert_columns(sort_columns).unwrap();

        for k in &keys {
            if !self.updated_keys.contains_key(k.as_ref()) {
                if let Some((key, accs)) = self.accumulators.get_mut_key_value(k.as_ref()) {
                    self.updated_keys.insert(
                        key,
                        Some(
                            accs.iter_mut()
                                .map(|s| s.evaluate())
                                .collect::<DFResult<_>>()?,
                        ),
                    );
                } else {
                    self.updated_keys
                        .insert(Key(Arc::new(k.as_ref().to_vec())), None);
                }
            }
        }

        let aggregate_input_cols = self.compute_inputs(batch);

        for (i, key) in keys.iter().enumerate() {
            if !self.accumulators.contains_key(key.as_ref()) {
                self.accumulators.insert(
                    Arc::new(key.as_ref().to_vec()),
                    Instant::now(),
                    0,
                    self.make_accumulators(),
                );
            };

            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            if retract {
                self.retract_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            } else {
                self.update_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            }
        }
        Ok(())
    }

    // =========================================================================
    // =========================================================================

    fn checkpoint_sliding(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        let mut states = vec![vec![]; self.sliding_state_schema.schema.fields.len()];
        let parser = self.key_converter.parser();
        let mut generation_builder = UInt64Builder::with_capacity(self.updated_keys.len());

        let mut cols = self
            .key_converter
            .convert_rows(self.updated_keys.keys().map(|k| {
                let (accumulators, generation) =
                    self.accumulators.get_mut_generation(k.0.as_ref()).unwrap();
                generation_builder.append_value(generation);

                for (state, agg) in accumulators.iter_mut().zip(self.aggregates.iter()) {
                    let IncrementalState::Sliding { expr, accumulator } = state else {
                        continue;
                    };
                    let state = accumulator.state().unwrap_or_else(|_| {
                        let state = accumulator.state().unwrap();
                        *accumulator = expr.create_sliding_accumulator().unwrap();
                        let states: Vec<_> =
                            state.iter().map(|s| s.to_array()).try_collect().unwrap();
                        accumulator.merge_batch(&states).unwrap();
                        state
                    });

                    for (idx, v) in agg.state_cols.iter().zip(state.into_iter()) {
                        states[*idx].push(v);
                    }
                }
                parser.parse(k.0.as_ref())
            }))?;

        cols.extend(
            states
                .into_iter()
                .skip(cols.len())
                .map(|c| ScalarValue::iter_to_array(c).unwrap()),
        );

        let generations = generation_builder.finish();
        self.new_generation = self
            .new_generation
            .max(max_array::<UInt64Type, _>(&generations).unwrap());
        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    fn checkpoint_batch(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self
            .aggregates
            .iter()
            .all(|agg| agg.accumulator_type == AccumulatorType::Sliding)
        {
            return Ok(None);
        }
        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        let size = self.updated_keys.len();
        let mut rows = Vec::with_capacity(size);
        let mut accumulator_builder = UInt32Builder::with_capacity(size);
        let mut args_row_builder = BinaryBuilder::with_capacity(size, size * 4);
        let mut count_builder = UInt64Builder::with_capacity(size);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(size);
        let mut generation_builder = UInt64Builder::with_capacity(size);

        let now = to_nanos(SystemTime::now()) as i64;
        let parser = self.key_converter.parser();

        for k in self.updated_keys.keys() {
            let row = parser.parse(&k.0);
            for (i, state) in self
                .accumulators
                .get_mut(k.0.as_ref())
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                let IncrementalState::Batch {
                    data,
                    changed_values,
                    ..
                } = state
                else {
                    continue;
                };

                for vk in changed_values.iter() {
                    if let Some(count) = data.get(vk) {
                        accumulator_builder.append_value(i as u32);
                        args_row_builder.append_value(&*vk.0);
                        count_builder.append_value(count.count);
                        generation_builder.append_value(count.generation);
                        timestamp_builder.append_value(now);
                        rows.push(row.to_owned())
                    }
                }
                data.retain(|_, v| v.count > 0);
            }
        }

        let mut cols = self.key_converter.convert_rows(rows.into_iter())?;
        cols.push(Arc::new(accumulator_builder.finish()));
        cols.push(Arc::new(args_row_builder.finish()));
        cols.push(Arc::new(count_builder.finish()));
        cols.push(Arc::new(timestamp_builder.finish()));

        let generations = generation_builder.finish();
        self.new_generation = self
            .new_generation
            .max(max_array::<UInt64Type, _>(&generations).unwrap());
        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    fn restore_sliding(
        &mut self,
        key: &[u8],
        now: Instant,
        i: usize,
        aggregate_states: &Vec<Vec<ArrayRef>>,
        generation: u64,
    ) -> Result<()> {
        let mut accumulators = self.make_accumulators();
        for ((_, state_cols), acc) in self
            .aggregates
            .iter()
            .zip(aggregate_states.iter())
            .zip(accumulators.iter_mut())
        {
            if let IncrementalState::Sliding { accumulator, .. } = acc {
                accumulator.merge_batch(&state_cols.iter().map(|c| c.slice(i, 1)).collect_vec())?
            }
        }
        self.accumulators
            .insert(Arc::new(key.to_vec()), now, generation, accumulators);
        Ok(())
    }

    async fn initialize(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        let mut deleted_keys = vec![];
        for (k, v) in self.accumulators.iter_mut() {
            let is_deleted = v.last_mut().unwrap().evaluate()?.is_null();
            if is_deleted {
                deleted_keys.push(k.clone());
            } else {
                for is in v {
                    if let IncrementalState::Batch { data, .. } = is {
                        data.retain(|_, v| v.count > 0);
                    }
                }
            }
        }
        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }
        Ok(())
    }

    fn generate_changelog(&mut self) -> Result<Option<RecordBatch>> {
        let mut output_keys = Vec::with_capacity(self.updated_keys.len() * 2);
        let mut output_values =
            vec![Vec::with_capacity(self.updated_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(self.updated_keys.len() * 2);

        let (updated_keys, updated_values): (Vec<_>, Vec<_>) =
            mem::take(&mut self.updated_keys).into_iter().unzip();
        let mut deleted_keys = vec![];

        for (k, retract) in updated_keys.iter().zip(updated_values.into_iter()) {
            let append = self.evaluate(&k.0)?;

            if let Some(v) = retract {
                if v.iter()
                    .zip(append.iter())
                    .take(v.len() - 1)
                    .all(|(a, b)| a == b)
                {
                    continue;
                }
                is_retracts.push(true);
                output_keys.push(k.clone());
                for (out, val) in output_values.iter_mut().zip(v) {
                    out.push(val);
                }
            }

            if !append.last().unwrap().is_null() {
                is_retracts.push(false);
                output_keys.push(k.clone());
                for (out, val) in output_values.iter_mut().zip(append) {
                    out.push(val);
                }
            } else {
                deleted_keys.push(k);
            }
        }

        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }

        let mut ttld_keys = vec![];
        for (k, mut v) in self.accumulators.time_out(Instant::now()) {
            is_retracts.push(true);
            ttld_keys.push(k);
            for (out, val) in output_values
                .iter_mut()
                .zip(v.iter_mut().map(|s| s.evaluate()))
            {
                out.push(val?);
            }
        }

        if output_keys.is_empty() && ttld_keys.is_empty() {
            return Ok(None);
        }

        let row_parser = self.key_converter.parser();
        let mut result_cols = self.key_converter.convert_rows(
            output_keys
                .iter()
                .map(|k| row_parser.parse(k.0.as_slice()))
                .chain(ttld_keys.iter().map(|k| row_parser.parse(k.as_slice()))),
        )?;

        for acc in output_values.into_iter() {
            result_cols.push(ScalarValue::iter_to_array(acc).unwrap());
        }

        let record_batch =
            RecordBatch::try_new(self.schema_without_metadata.clone(), result_cols).unwrap();

        let metadata = self
            .metadata_expr
            .evaluate(&record_batch)
            .unwrap()
            .into_array(record_batch.num_rows())
            .unwrap();
        let metadata = set_retract_metadata(metadata, Arc::new(BooleanArray::from(is_retracts)));

        let mut final_batch = record_batch.columns().to_vec();
        final_batch.push(metadata);

        Ok(Some(RecordBatch::try_new(
            self.final_output_schema.clone(),
            final_batch,
        )?))
    }
}

fn set_retract_metadata(metadata: ArrayRef, is_retract: Arc<BooleanArray>) -> ArrayRef {
    let metadata = metadata.as_struct();
    let arrays: Vec<Arc<dyn Array>> = vec![is_retract, metadata.column(1).clone()];
    Arc::new(StructArray::new(updating_meta_fields(), arrays, None))
}

// =========================================================================
// =========================================================================

#[async_trait::async_trait]
impl Operator for IncrementalAggregatingFunc {
    fn name(&self) -> &str {
        "UpdatingAggregatingFunc"
    }

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        self.initialize(ctx).await?;
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        if self.has_routing_keys {
            self.keyed_aggregate(&batch)?;
        } else {
            self.global_aggregate(&batch)?;
        }

        Ok(vec![])
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        if let Some(changelog_batch) = self.generate_changelog()? {
            Ok(vec![StreamOutput::Forward(changelog_batch)])
        } else {
            Ok(vec![])
        }
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// =========================================================================
// =========================================================================

pub struct IncrementalAggregatingConstructor;

impl IncrementalAggregatingConstructor {
    pub fn with_config(
        &self,
        config: UpdatingAggregateOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<IncrementalAggregatingFunc> {
        let ttl = Duration::from_micros(if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        });

        let input_schema: FsSchema = config.input_schema.unwrap().try_into()?;
        let final_schema: FsSchema = config.final_schema.unwrap().try_into()?;
        let mut schema_without_metadata = SchemaBuilder::from((*final_schema.schema).clone());
        schema_without_metadata.remove(final_schema.schema.index_of(UPDATING_META_FIELD).unwrap());

        let metadata_expr = parse_physical_expr(
            &PhysicalExprNode::decode(&mut config.metadata_expr.as_slice())?,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) =
            aggregate_exec.physical_plan_type.unwrap()
        else {
            bail!("invalid proto");
        };

        let mut sliding_state_fields = input_schema
            .routing_keys()
            .map(|v| {
                v.iter()
                    .map(|idx| input_schema.schema.field(*idx).clone())
                    .collect_vec()
            })
            .unwrap_or_default();

        let has_routing_keys = input_schema.routing_keys().is_some();
        let mut batch_state_fields = sliding_state_fields.clone();
        let key_fields = (0..sliding_state_fields.len()).collect_vec();

        let aggregates: Vec<_> = aggregate_exec
            .aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .map(|(expr, name)| {
                Ok(decode_aggregate(
                    &input_schema.schema,
                    name,
                    expr,
                    registry.as_ref(),
                )?)
            })
            .map_ok(|agg| {
                let retract = match agg.create_sliding_accumulator() {
                    Ok(s) => s.supports_retract_batch(),
                    _ => false,
                };
                (
                    agg,
                    if retract {
                        AccumulatorType::Sliding
                    } else {
                        AccumulatorType::Batch
                    },
                )
            })
            .map_ok(|(agg, t)| {
                let row_converter = Arc::new(RowConverter::new(
                    agg.expressions()
                        .iter()
                        .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                        .collect::<DFResult<_>>()?,
                )?);
                let fields = t.state_fields(&agg)?;
                let field_names = fields.iter().map(|f| f.name().to_string()).collect_vec();
                sliding_state_fields.extend(fields.into_iter().map(|f| (*f).clone()));
                Ok::<_, anyhow::Error>((agg, t, row_converter, field_names))
            })
            .flatten_ok()
            .collect::<Result<_>>()?;

        let state_schema = Schema::new(sliding_state_fields);

        let aggregates = aggregates
            .into_iter()
            .map(|(agg, t, row_converter, field_names)| Aggregator {
                func: agg,
                accumulator_type: t,
                row_converter,
                state_cols: field_names
                    .iter()
                    .map(|f| state_schema.index_of(f).unwrap())
                    .collect(),
            })
            .collect();

        let mut state_fields = state_schema.fields().to_vec();
        let timestamp_field = state_fields.pop().unwrap();
        state_fields.push(Arc::new(
            (*timestamp_field).clone().with_name(TIMESTAMP_FIELD),
        ));

        let sliding_state_schema = Arc::new(FsSchema::from_schema_keys(
            Arc::new(Schema::new(state_fields)),
            key_fields.clone(),
        )?);

        batch_state_fields.push(Field::new("accumulator", DataType::UInt32, false));
        batch_state_fields.push(Field::new("args_row", DataType::Binary, false));
        batch_state_fields.push(Field::new("count", DataType::UInt64, false));
        batch_state_fields.push(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));
        let timestamp_index = batch_state_fields.len() - 1;

        let mut storage_key_fields = key_fields.clone();
        storage_key_fields.push(storage_key_fields.len());
        storage_key_fields.push(storage_key_fields.len());

        let batch_state_schema = Arc::new(FsSchema::new(
            Arc::new(Schema::new(batch_state_fields)),
            timestamp_index,
            Some(storage_key_fields),
            Some(key_fields),
        ));

        Ok(IncrementalAggregatingFunc {
            flush_interval: Duration::from_micros(config.flush_interval_micros),
            metadata_expr,
            ttl,
            aggregates,
            accumulators: UpdatingCache::with_time_to_idle(ttl),
            schema_without_metadata: Arc::new(schema_without_metadata.finish()),
            final_output_schema: final_schema.schema.clone(),
            updated_keys: Default::default(),
            input_schema: Arc::new(input_schema.clone()),
            has_routing_keys,
            key_converter: RowConverter::new(input_schema.sort_fields(false))?,
            sliding_state_schema,
            batch_state_schema,
            new_generation: 0,
        })
    }
}
