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

//! Key-by over the physical plan output: key column(s) are **values** projected by the plan
//! (e.g. `_key_user_id`); **shuffle / `StreamOutput::Keyed` uses `u64` hashes** computed by
//! [`datafusion_common::hash_utils::create_hashes`] on those columns — same mechanism as
//! [`crate::runtime::streaming::operators::key_by::KeyByOperator`].

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{Collector, Operator};
use crate::runtime::streaming::operators::StatelessPhysicalExecutor;
use crate::sql::common::{CheckpointBarrier, Watermark};
use ahash::RandomState;
use anyhow::{Result, anyhow};
use arrow::compute::{sort_to_indices, take};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use async_trait::async_trait;
use datafusion_common::hash_utils::create_hashes;
use futures::StreamExt;

// ===========================================================================
// ===========================================================================

pub struct KeyExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
    key_fields: Vec<usize>,
    random_state: RandomState,
}

impl KeyExecutionOperator {
    pub fn new(name: String, executor: StatelessPhysicalExecutor, key_fields: Vec<usize>) -> Self {
        let deterministic_random_state = RandomState::with_seeds(
            0x1234567890ABCDEF,
            0x0FEDCBA987654321,
            0x1357924680135792,
            0x2468013579246801,
        );

        Self {
            name,
            executor,
            key_fields,
            random_state: deterministic_random_state,
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
        collector: &mut dyn Collector,
    ) -> Result<()> {
        let mut outputs = Vec::new();

        let mut stream = self.executor.process_batch(batch).await?;

        while let Some(batch_result) = stream.next().await {
            let out_batch = batch_result?;
            let num_rows = out_batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let key_arrays: Vec<ArrayRef> = self
                .key_fields
                .iter()
                .map(|&i| out_batch.column(i).clone())
                .collect();

            let mut hash_buffer = vec![0u64; num_rows];
            create_hashes(&key_arrays, &self.random_state, &mut hash_buffer)
                .map_err(|e| anyhow!("KeyExecution failed to hash columns: {e}"))?;

            let hash_array = UInt64Array::from(hash_buffer);

            let sorted_indices = sort_to_indices(&hash_array, None, None)
                .map_err(|e| anyhow!("Failed to sort by hash: {e}"))?;

            let sorted_hashes_ref = take(&hash_array, &sorted_indices, None)?;
            let sorted_hashes = sorted_hashes_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let sorted_columns: Result<Vec<ArrayRef>, _> = out_batch
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
        for out in outputs {
            collector.collect(out, _ctx).await?;
        }
        Ok(())
    }

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        collector
            .collect(StreamOutput::Watermark(watermark), _ctx)
            .await?;
        Ok(())
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
