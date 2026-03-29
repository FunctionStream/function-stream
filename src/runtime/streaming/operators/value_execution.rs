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


use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::operators::StatelessPhysicalExecutor;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};

pub struct ValueExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
}

impl ValueExecutionOperator {
    pub fn new(name: String, executor: StatelessPhysicalExecutor) -> Self {
        Self { name, executor }
    }
}

#[async_trait]
impl MessageOperator for ValueExecutionOperator {
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
            if out_batch.num_rows() > 0 {
                outputs.push(StreamOutput::Forward(out_batch));
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
}
