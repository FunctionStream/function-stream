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

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, PrimitiveArray, RecordBatch, StringArray,
    StructArray, TimestampNanosecondBuilder, UInt32Array, UInt32Builder,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimestampNanosecondType};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use futures::{StreamExt, ready, stream::Stream};

use crate::sql::common::constants::{cdc, debezium_op_short, physical_plan_node_name};
use crate::sql::common::{TIMESTAMP_FIELD, UPDATING_META_FIELD};
use crate::sql::physical::source_exec::make_stream_properties;

// ============================================================================
// CdcDebeziumPackExec (Execution Plan Node)
// ============================================================================

/// Packs internal flat changelog rows into Debezium-style `before` / `after` / `op` / timestamp.
///
/// Intended as the last physical node before a sink that expects Debezium CDC envelopes.
#[derive(Debug)]
pub struct CdcDebeziumPackExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl CdcDebeziumPackExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;

        let struct_fields: Vec<_> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.name() == UPDATING_META_FIELD || index == timestamp_index {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect();

        let payload_struct_type = DataType::Struct(struct_fields.into());

        let before_field = Arc::new(Field::new(cdc::BEFORE, payload_struct_type.clone(), true));
        let after_field = Arc::new(Field::new(cdc::AFTER, payload_struct_type, true));
        let op_field = Arc::new(Field::new(cdc::OP, DataType::Utf8, false));
        let timestamp_field = Arc::new(input_schema.field(timestamp_index).clone());

        let output_schema = Arc::new(Schema::new(vec![
            before_field,
            after_field,
            op_field,
            timestamp_field,
        ]));

        Ok(Self {
            input,
            schema: output_schema.clone(),
            properties: make_stream_properties(output_schema),
        })
    }

    pub(crate) fn from_decoded_parts(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self {
            properties: make_stream_properties(schema.clone()),
            input,
            schema,
        }
    }
}

impl DisplayAs for CdcDebeziumPackExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CdcDebeziumPackExec")
    }
}

impl ExecutionPlan for CdcDebeziumPackExec {
    fn name(&self) -> &str {
        physical_plan_node_name::TO_DEBEZIUM_EXEC
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "CdcDebeziumPackExec expects exactly 1 child".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(children[0].clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let updating_meta_index = self.input.schema().index_of(UPDATING_META_FIELD).ok();
        let timestamp_index = self.input.schema().index_of(TIMESTAMP_FIELD)?;

        let struct_projection = (0..self.input.schema().fields().len())
            .filter(|index| (updating_meta_index != Some(*index)) && *index != timestamp_index)
            .collect();

        Ok(Box::pin(CdcDebeziumPackStream {
            input: self.input.execute(partition, context)?,
            schema: self.schema.clone(),
            updating_meta_index,
            timestamp_index,
            struct_projection,
        }))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

// ============================================================================
// CdcDebeziumPackStream (Physical Stream Execution)
// ============================================================================

struct CdcDebeziumPackStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    updating_meta_index: Option<usize>,
    timestamp_index: usize,
    struct_projection: Vec<usize>,
}

#[derive(Debug)]
struct RowCompactionState {
    first_idx: usize,
    last_idx: usize,
    first_is_create: bool,
    last_is_create: bool,
    max_timestamp: i64,
}

impl CdcDebeziumPackStream {
    fn compact_changelog<'a>(
        num_rows: usize,
        is_retract: &'a BooleanArray,
        id_array: &'a FixedSizeBinaryArray,
        timestamps: &'a PrimitiveArray<TimestampNanosecondType>,
    ) -> (Vec<&'a [u8]>, HashMap<&'a [u8], RowCompactionState>) {
        let mut state_map: HashMap<&[u8], RowCompactionState> = HashMap::new();
        let mut unique_order = vec![];

        for i in 0..num_rows {
            let row_id = id_array.value(i);
            let is_create = !is_retract.value(i);
            let timestamp = timestamps.value(i);

            state_map
                .entry(row_id)
                .and_modify(|state| {
                    state.last_idx = i;
                    state.last_is_create = is_create;
                    state.max_timestamp = state.max_timestamp.max(timestamp);
                })
                .or_insert_with(|| {
                    unique_order.push(row_id);
                    RowCompactionState {
                        first_idx: i,
                        last_idx: i,
                        first_is_create: is_create,
                        last_is_create: is_create,
                        max_timestamp: timestamp,
                    }
                });
        }
        (unique_order, state_map)
    }

    fn as_debezium_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let value_struct = batch.project(&self.struct_projection)?;
        let timestamps = batch
            .column(self.timestamp_index)
            .as_primitive::<TimestampNanosecondType>();

        let columns: Vec<Arc<dyn Array>> = if let Some(meta_index) = self.updating_meta_index {
            let metadata = batch.column(meta_index).as_struct();
            let is_retract = metadata.column(0).as_boolean();
            let row_ids = metadata.column(1).as_fixed_size_binary();

            let (ordered_ids, state_map) =
                Self::compact_changelog(batch.num_rows(), is_retract, row_ids, timestamps);

            let mut before_builder = UInt32Builder::with_capacity(state_map.len());
            let mut after_builder = UInt32Builder::with_capacity(state_map.len());
            let mut op_vec = Vec::with_capacity(state_map.len());
            let mut ts_builder = TimestampNanosecondBuilder::with_capacity(state_map.len());

            for row_id in ordered_ids {
                let state = state_map
                    .get(row_id)
                    .expect("row id from order must exist in map");

                match (state.first_is_create, state.last_is_create) {
                    (true, true) => {
                        before_builder.append_null();
                        after_builder.append_value(state.last_idx as u32);
                        op_vec.push(debezium_op_short::CREATE);
                    }
                    (false, false) => {
                        before_builder.append_value(state.first_idx as u32);
                        after_builder.append_null();
                        op_vec.push(debezium_op_short::DELETE);
                    }
                    (false, true) => {
                        before_builder.append_value(state.first_idx as u32);
                        after_builder.append_value(state.last_idx as u32);
                        op_vec.push(debezium_op_short::UPDATE);
                    }
                    (true, false) => {
                        continue;
                    }
                }
                ts_builder.append_value(state.max_timestamp);
            }

            let before_indices = before_builder.finish();
            let after_indices = after_builder.finish();

            let before_array = Self::take_struct_columns(&value_struct, &before_indices)?;
            let after_array = Self::take_struct_columns(&value_struct, &after_indices)?;
            let op_array = StringArray::from(op_vec);

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(op_array),
                Arc::new(ts_builder.finish()),
            ]
        } else {
            let num_rows = value_struct.num_rows();

            let after_array = StructArray::try_new(
                value_struct.schema().fields().clone(),
                value_struct.columns().to_vec(),
                None,
            )?;
            let before_array =
                StructArray::new_null(value_struct.schema().fields().clone(), num_rows);

            let op_array = StringArray::from_iter_values(std::iter::repeat_n(
                debezium_op_short::CREATE,
                num_rows,
            ));

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(op_array),
                batch.column(self.timestamp_index).clone(),
            ]
        };

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn take_struct_columns(
        value_struct: &RecordBatch,
        indices: &UInt32Array,
    ) -> Result<StructArray> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(value_struct.num_columns());

        for col in value_struct.columns() {
            arrays.push(take(col.as_ref(), indices, None)?);
        }

        Ok(StructArray::try_new(
            value_struct.schema().fields().clone(),
            arrays,
            indices.nulls().cloned(),
        )?)
    }
}

impl Stream for CdcDebeziumPackStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match ready!(this.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(this.as_debezium_batch(&batch))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for CdcDebeziumPackStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
