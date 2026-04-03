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

use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::network::endpoint::PhysicalSender;
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::event::TrackedEvent;

use arrow_array::RecordBatch;
use std::sync::Arc;
use std::time::Duration;

/// 与单个子任务绑定的运行时参数（可由 `TaskContext::new` 默认填充，后续可扩展为从 Job 配置注入）。
#[derive(Debug, Clone)]
pub struct TaskContextConfig {
    /// Source 在无数据（`SourceEvent::Idle`）时的退避休眠时长。
    pub source_idle_timeout: Duration,
}

impl Default for TaskContextConfig {
    fn default() -> Self {
        Self {
            source_idle_timeout: Duration::from_millis(50),
        }
    }
}

pub struct TaskContext {
    pub job_id: String,
    pub vertex_id: u32,
    pub subtask_idx: u32,
    pub parallelism: u32,

    pub outboxes: Vec<PhysicalSender>,

    memory_pool: Arc<MemoryPool>,

    current_watermark: Option<std::time::SystemTime>,

    config: TaskContextConfig,
}

impl TaskContext {
    pub fn new(
        job_id: String,
        vertex_id: u32,
        subtask_idx: u32,
        parallelism: u32,
        outboxes: Vec<PhysicalSender>,
        memory_pool: Arc<MemoryPool>,
    ) -> Self {
        Self {
            job_id,
            vertex_id,
            subtask_idx,
            parallelism,
            outboxes,
            memory_pool,
            current_watermark: None,
            config: TaskContextConfig::default(),
        }
    }

    pub fn config(&self) -> &TaskContextConfig {
        &self.config
    }

    // ========================================================================
    // ========================================================================

    pub fn last_present_watermark(&self) -> Option<std::time::SystemTime> {
        self.current_watermark
    }

    pub fn advance_watermark(&mut self, watermark: std::time::SystemTime) {
        if let Some(current) = self.current_watermark {
            if watermark > current {
                self.current_watermark = Some(watermark);
            }
        } else {
            self.current_watermark = Some(watermark);
        }
    }

    // ========================================================================
    // ========================================================================

    pub fn task_identity(&self) -> String {
        format!(
            "Job[{}], Vertex[{}], Subtask[{}/{}]",
            self.job_id, self.vertex_id, self.subtask_idx, self.parallelism
        )
    }

    // ========================================================================
    // ========================================================================

    pub async fn collect(&self, batch: RecordBatch) -> anyhow::Result<()> {
        if self.outboxes.is_empty() {
            return Ok(());
        }

        let bytes_required = batch.get_array_memory_size();
        let ticket = self.memory_pool.request_memory(bytes_required).await;
        let tracked_event = TrackedEvent::new(StreamEvent::Data(batch), Some(ticket));

        for outbox in &self.outboxes {
            outbox.send(tracked_event.clone()).await?;
        }
        Ok(())
    }

    pub async fn collect_keyed(&self, key_hash: u64, batch: RecordBatch) -> anyhow::Result<()> {
        if self.outboxes.is_empty() {
            return Ok(());
        }

        let bytes_required = batch.get_array_memory_size();
        let ticket = self.memory_pool.request_memory(bytes_required).await;
        let tracked_event = TrackedEvent::new(StreamEvent::Data(batch), Some(ticket));

        let target_idx = (key_hash as usize) % self.outboxes.len();
        self.outboxes[target_idx].send(tracked_event).await?;
        Ok(())
    }

    pub async fn broadcast(&self, event: StreamEvent) -> anyhow::Result<()> {
        let tracked_event = TrackedEvent::control(event);
        for outbox in &self.outboxes {
            outbox.send(tracked_event.clone()).await?;
        }
        Ok(())
    }
}
