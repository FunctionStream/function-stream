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
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use crate::runtime::streaming::network::endpoint::PhysicalSender;

use arrow_array::RecordBatch;
use std::sync::Arc;

pub struct TaskContext {
    pub job_id: String,
    pub vertex_id: u32,
    pub subtask_idx: u32,
    pub parallelism: u32,

    pub outboxes: Vec<PhysicalSender>,

    memory_pool: Arc<MemoryPool>,

    current_watermark: Option<std::time::SystemTime>,
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
        }
    }

    // ========================================================================
    // 水位线与时间流管理 API
    // ========================================================================

    /// 供业务算子调用：获取当前任务的安全水位线
    pub fn last_present_watermark(&self) -> Option<std::time::SystemTime> {
        self.current_watermark
    }

    /// 供底座框架 (SubtaskRunner) 调用：推进本地时间，保证单调递增
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
    // 可观测性 API (Observability)
    // ========================================================================

    /// 格式化当前 Task 的唯一标识，用于分布式追踪和日志打印
    pub fn task_identity(&self) -> String {
        format!(
            "Job[{}], Vertex[{}], Subtask[{}/{}]",
            self.job_id, self.vertex_id, self.subtask_idx, self.parallelism
        )
    }

    // ========================================================================
    // 背压网络发送 API
    // ========================================================================

    /// 受内存池管控的数据发送：申请精准字节的内存船票后广播到所有下游
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

    /// 按 Key 哈希路由到单分区（用于 Shuffle / KeyBy）
    pub async fn collect_keyed(
        &self,
        key_hash: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
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

    /// 广播控制信号（如 Watermark, Barrier：不申请内存船票，保证在拥堵时畅通无阻）
    pub async fn broadcast(&self, event: StreamEvent) -> anyhow::Result<()> {
        let tracked_event = TrackedEvent::control(event);
        for outbox in &self.outboxes {
            outbox.send(tracked_event.clone()).await?;
        }
        Ok(())
    }
}
