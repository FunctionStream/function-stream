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

use crate::sql::common::{CheckpointBarrier, Watermark};
use crate::streaming::api::context::TaskContext;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use protocol::storage::{
    KafkaSourceSubtaskCheckpoint, SourceCheckpointInfo, source_checkpoint_info,
};

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

/// Checkpoint metadata produced by a source subtask during a barrier snapshot.
/// Sources fill this directly with [`SourceCheckpointInfo`] — the coordinator collects
/// and persists these entries without any further translation step.
#[derive(Debug, Default, Clone)]
pub struct SourceCheckpointReport {
    pub infos: Vec<SourceCheckpointInfo>,
}

impl SourceCheckpointReport {
    pub fn from_kafka_checkpoint(kafka: KafkaSourceSubtaskCheckpoint) -> Self {
        Self {
            infos: vec![SourceCheckpointInfo {
                info: Some(source_checkpoint_info::Info::Kafka(kafka)),
            }],
        }
    }
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> &str;

    /// Inject persisted checkpoint records before the source is started.
    /// Called by the engine after the operator is constructed and before [`Self::on_start`].
    /// Default implementation is a no-op; sources with stateful recovery override this.
    fn set_recovery_checkpoint(&mut self, _infos: Vec<SourceCheckpointInfo>) {}

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
    ) -> anyhow::Result<SourceCheckpointReport>;

    /// Same checkpoint **phase 2** hook as [`super::operator::Operator::commit_checkpoint`].
    /// Kafka source keeps the default: offsets are reported at the barrier in [`Self::snapshot_state`].
    async fn commit_checkpoint(
        &mut self,
        epoch: u64,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    /// Same rollback hook as [`super::operator::Operator::abort_checkpoint`].
    async fn abort_checkpoint(&mut self, epoch: u64, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }
}
