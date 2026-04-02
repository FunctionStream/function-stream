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

//!

use anyhow::{anyhow, Result};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{sort_to_indices, take};
use async_trait::async_trait;
use futures::StreamExt;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::operators::StatelessPhysicalExecutor;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};

// ===========================================================================
// ===========================================================================

pub struct KeyExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
    key_fields: Vec<usize>,
}

impl KeyExecutionOperator {
    pub fn new(
        name: String,
        executor: StatelessPhysicalExecutor,
        key_fields: Vec<usize>,
    ) -> Self {
        Self {
            name,
            executor,
            key_fields,
        }
    }
}

#[async_trait]
impl Operator for KeyExecutionOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let mut outputs = Vec::new();

        let mut stream = self.executor.process_batch(batch).await?;

        while let Some(batch_result) = stream.next().await {
            let out_batch = batch_result?;
            let num_rows = out_batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let mut final_hashes = vec![0u64; num_rows];

            for &col_idx in &self.key_fields {
                let col = out_batch.column(col_idx);
                let int64_array = col
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .ok_or_else(|| anyhow!("Column at index {} must be Int64Array", col_idx))?;

                for i in 0..num_rows {
                    let val = int64_array.value(i) as u64;
                    if self.key_fields.len() == 1 {
                        final_hashes[i] = val;
                    } else {
                        final_hashes[i] ^= val;
                    }
                }
            }

            let hash_array = UInt64Array::from(final_hashes);
            let sorted_indices = sort_to_indices(&hash_array, None, None)
                .map_err(|e| anyhow!("Failed to sort by key: {e}"))?;

            let sorted_hashes_ref = take(&hash_array, &sorted_indices, None)?;
            let sorted_hashes = sorted_hashes_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let sorted_columns: std::result::Result<Vec<ArrayRef>, _> = out_batch
                .columns()
                .iter()
                .map(|col| take(col, &sorted_indices, None))
                .collect();
            let sorted_batch = RecordBatch::try_new(out_batch.schema(), sorted_columns?)?;

            let mut start_idx = 0;
            while start_idx < num_rows {
                let current_hash = sorted_hashes.value(start_idx);
                let mut end_idx = start_idx + 1;

                while end_idx < num_rows && sorted_hashes.value(end_idx) == current_hash {
                    end_idx += 1;
                }

                let sub_batch = sorted_batch.slice(start_idx, end_idx - start_idx);

                outputs.push(StreamOutput::Keyed(current_hash, sub_batch));

                start_idx = end_idx;
            }
        }

        Ok(outputs)
    }

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![StreamOutput::Watermark(watermark)])
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

