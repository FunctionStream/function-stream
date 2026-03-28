//! Chandy–Lamport 风格屏障对齐（零内存缓冲：未对齐时从轮询池移除输入流，依赖底层背压）。

use std::collections::HashSet;

use crate::sql::common::CheckpointBarrier;

#[derive(Debug)]
pub enum AlignmentStatus {
    /// 未对齐：外层应将当前通道从 `StreamMap` 挂起（Pause）。
    Pending,
    /// 已对齐：外层触发快照并唤醒所有挂起通道（Resume）。
    Complete,
}

#[derive(Debug)]
pub struct BarrierAligner {
    input_count: usize,
    current_epoch: Option<u32>,
    reached_inputs: HashSet<usize>,
}

impl BarrierAligner {
    pub fn new(input_count: usize) -> Self {
        Self {
            input_count,
            current_epoch: None,
            reached_inputs: HashSet::new(),
        }
    }

    pub fn mark(&mut self, input_idx: usize, barrier: &CheckpointBarrier) -> AlignmentStatus {
        if self.current_epoch != Some(barrier.epoch) {
            self.current_epoch = Some(barrier.epoch);
            self.reached_inputs.clear();
        }

        self.reached_inputs.insert(input_idx);

        if self.reached_inputs.len() == self.input_count {
            self.current_epoch = None;
            self.reached_inputs.clear();
            AlignmentStatus::Complete
        } else {
            AlignmentStatus::Pending
        }
    }
}
