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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use arrow_array::RecordBatch;

use crate::runtime::memory::{MemoryBlock, MemoryPool, get_array_memory_size};
use crate::runtime::streaming::network::endpoint::PhysicalSender;
use crate::runtime::streaming::protocol::event::{StreamEvent, TrackedEvent};
use crate::runtime::streaming::state::IoManager;

#[derive(Debug, Clone)]
pub struct TaskContextConfig {
    pub source_idle_timeout: Duration,
}

impl Default for TaskContextConfig {
    fn default() -> Self {
        Self {
            source_idle_timeout: Duration::from_millis(50),
        }
    }
}

/// Task execution context.
///
/// Acts as the sole bridge between operators and engine infrastructure (network, memory,
/// configuration) for a single subtask.
pub struct TaskContext {
    /// Job identifier.
    pub job_id: String,
    /// Logical pipeline (vertex) index within the job graph.
    pub pipeline_id: u32,
    /// This subtask's index within the pipeline's parallelism.
    pub subtask_index: u32,
    /// Number of parallel subtasks for this pipeline.
    pub parallelism: u32,

    /// Precomputed display string for high-frequency logging without per-call allocation.
    task_name: String,

    /// Downstream physical senders (outbound edges).
    downstream_senders: Vec<PhysicalSender>,

    /// Job-wide shared pool; memory is accounted only when [`Self::collect`] / [`Self::collect_keyed`] run.
    memory_pool: Arc<MemoryPool>,

    /// Latest aligned event-time watermark for this subtask.
    current_watermark: Option<SystemTime>,

    /// Subtask-level tunables.
    config: TaskContextConfig,

    pub state_dir: PathBuf,
    pub io_manager: IoManager,

    /// Pipeline-wide slab from the global pool; each stateful operator sub-allocates a ticket.
    pub pipeline_state_memory_block: Option<Arc<MemoryBlock>>,
    /// Bytes reserved per stateful operator from [`Self::pipeline_state_memory_block`].
    pub operator_state_memory_bytes: u64,

    /// Last globally-committed safe epoch for crash recovery.
    safe_epoch: u64,
}

impl TaskContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: String,
        pipeline_id: u32,
        subtask_index: u32,
        parallelism: u32,
        downstream_senders: Vec<PhysicalSender>,
        memory_pool: Arc<MemoryPool>,
        io_manager: IoManager,
        state_dir: PathBuf,
        pipeline_state_memory_block: Option<Arc<MemoryBlock>>,
        operator_state_memory_bytes: u64,
        safe_epoch: u64,
    ) -> Self {
        let task_name = format!(
            "Task-[{}]-Pipe[{}]-Sub[{}/{}]",
            job_id, pipeline_id, subtask_index, parallelism
        );

        Self {
            job_id,
            pipeline_id,
            subtask_index,
            parallelism,
            task_name,
            downstream_senders,
            memory_pool,
            current_watermark: None,
            config: TaskContextConfig::default(),
            state_dir,
            io_manager,
            pipeline_state_memory_block,
            operator_state_memory_bytes,
            safe_epoch,
        }
    }

    #[inline]
    pub fn latest_safe_epoch(&self) -> u64 {
        self.safe_epoch
    }

    #[inline]
    pub fn config(&self) -> &TaskContextConfig {
        &self.config
    }

    #[inline]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    // -------------------------------------------------------------------------
    // Watermark
    // -------------------------------------------------------------------------

    #[inline]
    pub fn current_watermark(&self) -> Option<SystemTime> {
        self.current_watermark
    }

    pub fn advance_watermark(&mut self, watermark: SystemTime) {
        if let Some(current) = self.current_watermark {
            self.current_watermark = Some(current.max(watermark));
        } else {
            self.current_watermark = Some(watermark);
        }
    }

    // -------------------------------------------------------------------------
    // Data emission
    // -------------------------------------------------------------------------

    /// Fan-out a data batch to all downstreams (forward / broadcast).
    ///
    /// Back-pressure and memory accounting happen here via [`MemoryPool::request_block`], not
    /// when building the pipeline.
    pub async fn collect(&self, batch: RecordBatch) -> Result<()> {
        if self.downstream_senders.is_empty() {
            return Ok(());
        }

        let bytes_required = get_array_memory_size(&batch);
        let block = self.memory_pool.request_block(bytes_required).await;
        let ticket = block
            .try_allocate(bytes_required)
            .ok_or_else(|| anyhow!("memory block allocation failed"))?;
        let tracked_event = TrackedEvent::new(StreamEvent::Data(batch), Some(ticket));

        self.broadcast_event(tracked_event).await
    }

    /// Route a batch to one downstream by hash partitioning (shuffle).
    pub async fn collect_keyed(&self, key_hash: u64, batch: RecordBatch) -> Result<()> {
        let num_downstreams = self.downstream_senders.len();
        if num_downstreams == 0 {
            return Ok(());
        }

        let bytes_required = get_array_memory_size(&batch);
        let block = self.memory_pool.request_block(bytes_required).await;
        let ticket = block
            .try_allocate(bytes_required)
            .ok_or_else(|| anyhow!("memory block allocation failed"))?;
        let event = TrackedEvent::new(StreamEvent::Data(batch), Some(ticket));

        let target_idx = (key_hash as usize) % num_downstreams;

        self.downstream_senders[target_idx]
            .send(event)
            .await
            .with_context(|| {
                format!(
                    "{} failed to route keyed data to downstream index {}",
                    self.task_name, target_idx
                )
            })?;

        Ok(())
    }

    /// Broadcast a control event (watermark, barrier, end-of-stream).
    pub async fn broadcast(&self, event: StreamEvent) -> Result<()> {
        if self.downstream_senders.is_empty() {
            return Ok(());
        }
        let tracked_event = TrackedEvent::control(event);
        self.broadcast_event(tracked_event).await
    }

    // -------------------------------------------------------------------------
    // Internal dispatch
    // -------------------------------------------------------------------------

    async fn broadcast_event(&self, event: TrackedEvent) -> Result<()> {
        let mut iter = self.downstream_senders.iter().enumerate().peekable();

        while let Some((idx, sender)) = iter.next() {
            if iter.peek().is_some() {
                sender.send(event.clone()).await.with_context(|| {
                    format!(
                        "{} failed to broadcast event to downstream index {}",
                        self.task_name, idx
                    )
                })?;
            } else {
                sender.send(event).await.with_context(|| {
                    format!(
                        "{} failed to send final event to downstream index {}",
                        self.task_name, idx
                    )
                })?;
                break;
            }
        }

        Ok(())
    }
}
