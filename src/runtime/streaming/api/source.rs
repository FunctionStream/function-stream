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
use protocol::storage::{KafkaSourceSubtaskCheckpoint, SourceCheckpointPayload, source_checkpoint_payload};

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

/// Optional metadata returned when a source completes a checkpoint barrier snapshot.
#[derive(Debug, Default, Clone)]
pub struct SourceCheckpointReport {
    pub payloads: Vec<SourceCheckpointPayload>,
}

impl SourceCheckpointReport {
    pub fn from_kafka_checkpoint(kafka: KafkaSourceSubtaskCheckpoint) -> Self {
        Self {
            payloads: vec![SourceCheckpointPayload {
                checkpoint: Some(source_checkpoint_payload::Checkpoint::Kafka(kafka)),
            }],
        }
    }
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
    ) -> anyhow::Result<SourceCheckpointReport>;

    /// Same checkpoint **phase 2** hook as [`super::operator::Operator::commit_checkpoint`].
    /// Kafka source keeps the default: offsets are reported at the barrier in [`Self::snapshot_state`].
    async fn commit_checkpoint(
        &mut self,
        epoch: u32,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    /// Same rollback hook as [`super::operator::Operator::abort_checkpoint`].
    async fn abort_checkpoint(&mut self, epoch: u32, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        let _ = epoch;
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }
}
