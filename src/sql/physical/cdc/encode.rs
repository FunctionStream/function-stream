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

//! 内部回撤流压回 Debezium `before` / `after` / `op` 信封。

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, PrimitiveArray, RecordBatch, StringArray,
    StructArray, TimestampNanosecondBuilder,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
use datafusion::arrow::datatypes::TimestampNanosecondType;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use futures::{ready, stream::Stream, StreamExt};

use crate::sql::common::constants::{cdc, debezium_op_short, physical_plan_node_name};
use crate::sql::common::{TIMESTAMP_FIELD, UPDATING_META_FIELD};
use crate::sql::physical::readers::make_stream_properties;

#[derive(Debug)]
pub struct ToDebeziumExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl ToDebeziumExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let struct_fields: Vec<_> = input_schema
            .fields()
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.name() == UPDATING_META_FIELD || index == timestamp_index {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect();
        let struct_data_type = DataType::Struct(struct_fields.into());
        let before_field = Arc::new(Field::new(cdc::BEFORE, struct_data_type.clone(), true));
        let after_field = Arc::new(Field::new(cdc::AFTER, struct_data_type, true));
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

impl DisplayAs for ToDebeziumExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ToDebeziumExec")
    }
}

impl ExecutionPlan for ToDebeziumExec {
    fn name(&self) -> &str {
        physical_plan_node_name::TO_DEBEZIUM_EXEC
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
                "ToDebeziumExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(ToDebeziumExec::try_new(children[0].clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let updating_meta_index = self.input.schema().index_of(UPDATING_META_FIELD).ok();
        let timestamp_index = self.input.schema().index_of(TIMESTAMP_FIELD)?;
        let struct_projection = (0..self.input.schema().fields().len())
            .filter(|index| {
                updating_meta_index
                    .map(|is_retract_index| *index != is_retract_index)
                    .unwrap_or(true)
                    && *index != timestamp_index
            })
            .collect();

        Ok(Box::pin(ToDebeziumStream {
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

struct ToDebeziumStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    updating_meta_index: Option<usize>,
    timestamp_index: usize,
    struct_projection: Vec<usize>,
}

/// 按主键 id 归并一行内的 changelog，输出 before/after 行索引与 op 字母。
fn compact_changelog_by_id<'a>(
    num_rows: usize,
    is_retract: &'a BooleanArray,
    id: &'a FixedSizeBinaryArray,
    timestamps: &'a PrimitiveArray<TimestampNanosecondType>,
) -> (
    Vec<&'a [u8]>,
    HashMap<&'a [u8], (usize, usize, bool, bool, i64)>,
) {
    let mut id_map: HashMap<&[u8], (usize, usize, bool, bool, i64)> = HashMap::new();
    let mut order = vec![];
    for i in 0..num_rows {
        let row_id = id.value(i);
        let is_create = !is_retract.value(i);
        let timestamp = timestamps.value(i);

        id_map
            .entry(row_id)
            .and_modify(|e| {
                e.1 = i;
                e.3 = is_create;
                e.4 = e.4.max(timestamp);
            })
            .or_insert_with(|| {
                order.push(row_id);
                (i, i, is_create, is_create, timestamp)
            });
    }
    (order, id_map)
}

impl ToDebeziumStream {
    fn as_debezium_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let value_struct = batch.project(&self.struct_projection)?;
        let timestamps = batch
            .column(self.timestamp_index)
            .as_primitive::<TimestampNanosecondType>();

        let columns: Vec<Arc<dyn Array>> = if let Some(metadata_index) = self.updating_meta_index {
            let metadata = batch
                .column(metadata_index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Invalid type for updating_meta column".to_string())
                })?;

            let is_retract = metadata.column(0).as_boolean();
            let id = metadata.column(1).as_fixed_size_binary();

            let (order, id_map) =
                compact_changelog_by_id(batch.num_rows(), is_retract, id, timestamps);

            let mut before = Vec::with_capacity(id_map.len());
            let mut after = Vec::with_capacity(id_map.len());
            let mut op = Vec::with_capacity(id_map.len());
            let mut ts = TimestampNanosecondBuilder::with_capacity(id_map.len());

            for row_id in order {
                let (first_idx, last_idx, first_is_create, last_is_create, timestamp) =
                    id_map.get(row_id).unwrap();

                if *first_is_create && *last_is_create {
                    before.push(None);
                    after.push(Some(*last_idx));
                    op.push(debezium_op_short::CREATE);
                } else if !(*first_is_create) && !(*last_is_create) {
                    before.push(Some(*first_idx));
                    after.push(None);
                    op.push(debezium_op_short::DELETE);
                } else if !(*first_is_create) && *last_is_create {
                    before.push(Some(*first_idx));
                    after.push(Some(*last_idx));
                    op.push(debezium_op_short::UPDATE);
                } else {
                    continue;
                }

                ts.append_value(*timestamp);
            }

            let before_array = Self::create_output_array(&value_struct, &before)?;
            let after_array = Self::create_output_array(&value_struct, &after)?;
            let op_array = StringArray::from(op);

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(op_array),
                Arc::new(ts.finish()),
            ]
        } else {
            let after_array = StructArray::try_new(
                value_struct.schema().fields().clone(),
                value_struct.columns().to_vec(),
                None,
            )?;

            let before_array = StructArray::new_null(
                value_struct.schema().fields().clone(),
                value_struct.num_rows(),
            );

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(StringArray::from(vec![
                    debezium_op_short::CREATE;
                    value_struct.num_rows()
                ])),
                batch.column(self.timestamp_index).clone(),
            ]
        };

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn create_output_array(
        value_struct: &RecordBatch,
        indices: &[Option<usize>],
    ) -> Result<StructArray> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(value_struct.num_columns());
        for col in value_struct.columns() {
            let new_array = take(
                col.as_ref(),
                &indices
                    .iter()
                    .map(|&idx| idx.map(|i| i as u64))
                    .collect::<PrimitiveArray<UInt64Type>>(),
                None,
            )?;
            arrays.push(new_array);
        }

        Ok(StructArray::try_new(
            value_struct.schema().fields().clone(),
            arrays,
            Some(NullBuffer::from(
                indices.iter().map(|&idx| idx.is_some()).collect::<Vec<_>>(),
            )),
        )?)
    }
}

impl Stream for ToDebeziumStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.as_debezium_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for ToDebeziumStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
