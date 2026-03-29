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

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::SourceOperator;
use crate::runtime::streaming::protocol::stream_out::StreamOutput;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::time::Duration;
use crate::sql::common::{CheckpointBarrier, Watermark};

// ---------------------------------------------------------------------------
// ConstructedOperator
// ---------------------------------------------------------------------------

pub enum ConstructedOperator {
    Source(Box<dyn SourceOperator>),
    Operator(Box<dyn MessageOperator>),
}

#[async_trait]
pub trait MessageOperator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>>;

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>>;

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()>;

    async fn commit_checkpoint(
        &mut self,
        _epoch: u32,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    async fn process_tick(
        &mut self,
        _tick_index: u64,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}
