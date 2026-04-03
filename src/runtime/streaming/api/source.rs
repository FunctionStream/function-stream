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
use crate::sql::common::{CheckpointBarrier, Watermark};
use arrow_array::RecordBatch;
use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceOffset {
    Earliest,
    Latest,
    #[default]
    Group,
}

#[derive(Debug)]
pub enum SourceEvent {
    Data(RecordBatch),
    Watermark(Watermark),
    Idle,
    EndOfStream,
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn fetch_next(&mut self, ctx: &mut TaskContext) -> anyhow::Result<SourceEvent>;

    fn poll_watermark(&mut self) -> Option<Watermark> {
        None
    }

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

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }
}
