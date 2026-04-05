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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{
    BooleanBuilder, RecordBatch, StructArray, TimestampNanosecondBuilder, UInt32Builder,
};
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType,
};
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use futures::{StreamExt, ready, stream::Stream};

use crate::sql::common::TIMESTAMP_FIELD;
use crate::sql::common::constants::{cdc, debezium_op_short, physical_plan_node_name};
use crate::sql::functions::MultiHashFunction;
use crate::sql::physical::meta::{updating_meta_field, updating_meta_fields};
use crate::sql::physical::source_exec::make_stream_properties;

// ============================================================================
// CdcDebeziumUnrollExec (Execution Plan Node)
// ============================================================================

/// Physical node that unrolls Debezium CDC payloads (`before` / `after` / `op`) into a flat
/// changelog stream with retract metadata.
///
/// - `c` / `r` → emit `after` (`is_retract = false`)
/// - `d` → emit `before` (`is_retract = true`)
/// - `u` → emit `before` (retract) then `after` (insert)
#[derive(Debug)]
pub struct CdcDebeziumUnrollExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
    primary_key_indices: Vec<usize>,
}

impl CdcDebeziumUnrollExec {
    /// Builds the node and validates Debezium payload schema constraints.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, primary_key_indices: Vec<usize>) -> Result<Self> {
        let input_schema = input.schema();

        let before_index = input_schema.index_of(cdc::BEFORE)?;
        let after_index = input_schema.index_of(cdc::AFTER)?;
        let op_index = input_schema.index_of(cdc::OP)?;
        let _timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;

        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();

        if before_type != after_type {
            return Err(DataFusionError::Plan(
                "CDC 'before' and 'after' columns must share the exact same DataType".to_string(),
            ));
        }

        if *input_schema.field(op_index).data_type() != DataType::Utf8 {
            return Err(DataFusionError::Plan(
                "CDC 'op' (operation) column must be of type Utf8 (String)".to_string(),
            ));
        }

        let DataType::Struct(fields) = before_type else {
            return Err(DataFusionError::Plan(
                "CDC 'before' and 'after' payload columns must be Structs".to_string(),
            ));
        };

        let mut unrolled_fields = fields.to_vec();
        unrolled_fields.push(updating_meta_field());
        unrolled_fields.push(Arc::new(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )));

        let schema = Arc::new(Schema::new(unrolled_fields));

        Ok(Self {
            input,
            schema: schema.clone(),
            properties: make_stream_properties(schema),
            primary_key_indices,
        })
    }

    /// Used when deserializing a plan with a pre-baked output schema (see [`StreamingExtensionCodec`]).
    pub(crate) fn from_decoded_parts(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        primary_key_indices: Vec<usize>,
    ) -> Self {
        Self {
            properties: make_stream_properties(schema.clone()),
            input,
            schema,
            primary_key_indices,
        }
    }

    pub fn primary_key_indices(&self) -> &[usize] {
        &self.primary_key_indices
    }
}

impl DisplayAs for CdcDebeziumUnrollExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CdcDebeziumUnrollExec")
    }
}

impl ExecutionPlan for CdcDebeziumUnrollExec {
    fn name(&self) -> &str {
        physical_plan_node_name::DEBEZIUM_UNROLLING_EXEC
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
                "CdcDebeziumUnrollExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            primary_key_indices: self.primary_key_indices.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CdcDebeziumUnrollStream::try_new(
            self.input.execute(partition, context)?,
            self.schema.clone(),
            self.primary_key_indices.clone(),
        )?))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

// ============================================================================
// CdcDebeziumUnrollStream (Physical Stream Execution)
// ============================================================================

struct CdcDebeziumUnrollStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    before_index: usize,
    after_index: usize,
    op_index: usize,
    timestamp_index: usize,
    primary_key_indices: Vec<usize>,
}

impl CdcDebeziumUnrollStream {
    fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        primary_key_indices: Vec<usize>,
    ) -> Result<Self> {
        if primary_key_indices.is_empty() {
            return plan_err!(
                "A CDC source requires at least one primary key to maintain state correctly."
            );
        }

        let input_schema = input.schema();
        Ok(Self {
            input,
            schema,
            before_index: input_schema.index_of(cdc::BEFORE)?,
            after_index: input_schema.index_of(cdc::AFTER)?,
            op_index: input_schema.index_of(cdc::OP)?,
            timestamp_index: input_schema.index_of(TIMESTAMP_FIELD)?,
            primary_key_indices,
        })
    }

    fn unroll_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let before_col = batch.column(self.before_index);
        let after_col = batch.column(self.after_index);

        let op_array = batch.column(self.op_index).as_string::<i32>();
        let timestamp_array = batch
            .column(self.timestamp_index)
            .as_primitive::<TimestampNanosecondType>();

        let max_capacity = num_rows * 2;
        let mut take_indices = UInt32Builder::with_capacity(max_capacity);
        let mut is_retract_builder = BooleanBuilder::with_capacity(max_capacity);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(max_capacity);

        for i in 0..num_rows {
            let op = op_array.value(i);
            let ts = timestamp_array.value(i);

            match op {
                debezium_op_short::CREATE | debezium_op_short::READ => {
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(ts);
                }
                debezium_op_short::DELETE => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(ts);
                }
                debezium_op_short::UPDATE => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(ts);

                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(ts);
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Encountered unexpected Debezium operation code: '{op}'"
                    )));
                }
            }
        }

        let take_indices = take_indices.finish();
        let unrolled_row_count = take_indices.len();

        let combined_array = concat(&[before_col.as_ref(), after_col.as_ref()])?;
        let unrolled_array = take(&combined_array, &take_indices, None)?;

        let mut final_columns = unrolled_array.as_struct().columns().to_vec();

        let pk_columns: Vec<ColumnarValue> = self
            .primary_key_indices
            .iter()
            .map(|&idx| ColumnarValue::Array(Arc::clone(&final_columns[idx])))
            .collect();

        let hash_column = MultiHashFunction::default().invoke(&pk_columns)?;
        let ids_array = hash_column.into_array(unrolled_row_count)?;

        let meta_struct = StructArray::try_new(
            updating_meta_fields(),
            vec![Arc::new(is_retract_builder.finish()), ids_array],
            None,
        )?;

        final_columns.push(Arc::new(meta_struct));
        final_columns.push(Arc::new(timestamp_builder.finish()));

        Ok(RecordBatch::try_new(self.schema.clone(), final_columns)?)
    }
}

impl Stream for CdcDebeziumUnrollStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match ready!(this.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(this.unroll_batch(&batch))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for CdcDebeziumUnrollStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
