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
    Array, BooleanBuilder, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
    TimestampNanosecondBuilder, UInt32Builder,
};
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use futures::{ready, stream::Stream, StreamExt};

use crate::sql::common::constants::{cdc, debezium_op_short, physical_plan_node_name};
use crate::sql::common::TIMESTAMP_FIELD;
use crate::sql::functions::MultiHashFunction;
use crate::sql::physical::meta::{updating_meta_field, updating_meta_fields};
use crate::sql::physical::readers::make_stream_properties;

#[derive(Debug)]
pub struct DebeziumUnrollingExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, primary_keys: Vec<usize>) -> Result<Self> {
        let input_schema = input.schema();
        let before_index = input_schema.index_of(cdc::BEFORE)?;
        let after_index = input_schema.index_of(cdc::AFTER)?;
        let op_index = input_schema.index_of(cdc::OP)?;
        let _timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();
        if before_type != after_type {
            return Err(DataFusionError::Internal(
                "before and after columns must have the same type".to_string(),
            ));
        }
        let op_type = input_schema.field(op_index).data_type();
        if *op_type != DataType::Utf8 {
            return Err(DataFusionError::Internal(
                "op column must be a string".to_string(),
            ));
        }
        let DataType::Struct(fields) = before_type else {
            return Err(DataFusionError::Internal(
                "before and after columns must be structs".to_string(),
            ));
        };
        let mut fields = fields.to_vec();
        fields.push(updating_meta_field());
        fields.push(Arc::new(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )));

        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            input,
            schema: schema.clone(),
            properties: make_stream_properties(schema),
            primary_keys,
        })
    }

    pub(crate) fn from_decoded_parts(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        primary_keys: Vec<usize>,
    ) -> Self {
        Self {
            properties: make_stream_properties(schema.clone()),
            input,
            schema,
            primary_keys,
        }
    }

    pub fn primary_key_indices(&self) -> &[usize] {
        &self.primary_keys
    }
}

impl DisplayAs for DebeziumUnrollingExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DebeziumUnrollingExec")
    }
}

impl ExecutionPlan for DebeziumUnrollingExec {
    fn name(&self) -> &str {
        physical_plan_node_name::DEBEZIUM_UNROLLING_EXEC
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
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
                "DebeziumUnrollingExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(DebeziumUnrollingExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            primary_keys: self.primary_keys.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DebeziumUnrollingStream::try_new(
            self.input.execute(partition, context)?,
            self.schema.clone(),
            self.primary_keys.clone(),
        )?))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

struct DebeziumUnrollingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    before_index: usize,
    after_index: usize,
    op_index: usize,
    timestamp_index: usize,
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingStream {
    fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        primary_keys: Vec<usize>,
    ) -> Result<Self> {
        if primary_keys.is_empty() {
            return plan_err!("there must be at least one primary key for a Debezium source");
        }
        let input_schema = input.schema();
        let before_index = input_schema.index_of(cdc::BEFORE)?;
        let after_index = input_schema.index_of(cdc::AFTER)?;
        let op_index = input_schema.index_of(cdc::OP)?;
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;

        Ok(Self {
            input,
            schema,
            before_index,
            after_index,
            op_index,
            timestamp_index,
            primary_keys,
        })
    }

    fn unroll_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let before = batch.column(self.before_index).as_ref();
        let after = batch.column(self.after_index).as_ref();
        let op = batch
            .column(self.op_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("op column is not a string".to_string()))?;

        let timestamp = batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("timestamp column is not a timestamp".to_string())
            })?;

        let num_rows = batch.num_rows();
        let combined_array = concat(&[before, after])?;
        let mut take_indices = UInt32Builder::with_capacity(num_rows);
        let mut is_retract_builder = BooleanBuilder::with_capacity(num_rows);

        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(2 * num_rows);
        for i in 0..num_rows {
            let op = op.value(i);
            match op {
                debezium_op_short::CREATE | debezium_op_short::READ => {
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                debezium_op_short::UPDATE => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                debezium_op_short::DELETE => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected op value: {op}"
                    )));
                }
            }
        }
        let take_indices = take_indices.finish();
        let unrolled_array = take(&combined_array, &take_indices, None)?;

        let mut columns = unrolled_array.as_struct().columns().to_vec();

        let hash = MultiHashFunction::default().invoke(
            &self
                .primary_keys
                .iter()
                .map(|i| ColumnarValue::Array(columns[*i].clone()))
                .collect::<Vec<_>>(),
        )?;

        let ids = hash.into_array(num_rows)?;

        let meta = StructArray::try_new(
            updating_meta_fields(),
            vec![Arc::new(is_retract_builder.finish()), ids],
            None,
        )?;
        columns.push(Arc::new(meta));
        columns.push(Arc::new(timestamp_builder.finish()));
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for DebeziumUnrollingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.unroll_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for DebeziumUnrollingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
