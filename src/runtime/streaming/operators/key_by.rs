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

use anyhow::{Result, anyhow};
use arrow::compute::{sort_to_indices, take};
use arrow_array::{Array, RecordBatch, UInt64Array};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::hash_utils::create_hashes;
use datafusion_physical_expr::expressions::Column;
use std::sync::Arc;

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{Collector, Operator};
use crate::sql::common::{CheckpointBarrier, Watermark};

use protocol::function_stream_graph::KeyPlanOperator;

pub struct KeyByOperator {
    name: String,
    key_extractors: Vec<Arc<dyn PhysicalExpr>>,
    random_state: ahash::RandomState,
}

impl KeyByOperator {
    pub fn new(name: String, key_extractors: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            name,
            key_extractors,
            random_state: ahash::RandomState::new(),
        }
    }
}

#[async_trait]
impl Operator for KeyByOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let mut key_columns = Vec::with_capacity(self.key_extractors.len());
        for expr in &self.key_extractors {
            let column_array = expr
                .evaluate(&batch)
                .map_err(|e| anyhow!("Failed to evaluate key expr: {}", e))?
                .into_array(num_rows)
                .map_err(|e| anyhow!("Failed to convert into array: {}", e))?;
            key_columns.push(column_array);
        }

        let mut hash_buffer = vec![0u64; num_rows];
        create_hashes(&key_columns, &self.random_state, &mut hash_buffer)
            .map_err(|e| anyhow!("Failed to compute hashes: {}", e))?;

        let hash_array = UInt64Array::from(hash_buffer);

        let sorted_indices = sort_to_indices(&hash_array, None, None)
            .map_err(|e| anyhow!("Failed to sort hashes: {}", e))?;

        let sorted_hashes_ref = take(&hash_array, &sorted_indices, None)?;
        let sorted_hashes = sorted_hashes_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let sorted_columns: std::result::Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col, &sorted_indices, None))
            .collect();
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns?)?;

        let mut outputs = Vec::new();
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

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

pub struct KeyByConstructor;

impl KeyByConstructor {
    pub fn with_config(&self, config: KeyPlanOperator) -> Result<KeyByOperator> {
        let mut key_extractors: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(config.key_fields.len());

        for field_idx in &config.key_fields {
            let idx = *field_idx as usize;
            let expr = Arc::new(Column::new(&format!("col_{}", idx), idx)) as Arc<dyn PhysicalExpr>;
            key_extractors.push(expr);
        }

        let name = if config.name.is_empty() {
            "KeyBy".to_string()
        } else {
            config.name.clone()
        };

        Ok(KeyByOperator::new(name, key_extractors))
    }
}
