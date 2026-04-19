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
use crate::runtime::streaming::protocol::event::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};
use arrow_array::RecordBatch;
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// ConstructedOperator
// ---------------------------------------------------------------------------

pub enum ConstructedOperator {
    Source(Box<dyn SourceOperator>),
    Operator(Box<dyn Operator>),
}

#[async_trait]
pub trait Collector: Send {
    async fn collect(&mut self, out: StreamOutput, ctx: &mut TaskContext) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Operator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
        collector: &mut dyn Collector,
    ) -> anyhow::Result<()>;

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut TaskContext,
        collector: &mut dyn Collector,
    ) -> anyhow::Result<()>;

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()>;

    /// Global checkpoint **phase 2** (after metadata is durable): finalize external side effects.
    ///
    /// Default is no-op. Examples of overrides: transactional Kafka sink calls
    /// `commit_transaction` on the producer stashed during [`Self::snapshot_state`].
    async fn commit_checkpoint(
        &mut self,
        epoch: u32,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    /// Global checkpoint **rollback** when phase 2 must not commit (e.g. catalog persist failed).
    ///
    /// Default is no-op. Transactional Kafka sink overrides with `abort_transaction` on the stashed producer.
    async fn abort_checkpoint(&mut self, epoch: u32, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}
