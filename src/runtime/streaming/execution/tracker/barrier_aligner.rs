//! Chandy–Lamport 风格屏障对齐。

use std::collections::HashSet;
use crate::runtime::streaming::protocol::TrackedEvent;
use crate::sql::common::CheckpointBarrier;

#[derive(Debug)]
pub enum AlignmentStatus {
    Pending,
    Complete(Vec<(usize, TrackedEvent)>),
}

#[derive(Debug)]
pub struct BarrierAligner {
    input_count: usize,
    current_epoch: Option<u32>,
    reached_inputs: HashSet<usize>,
    buffered_events: Vec<(usize, TrackedEvent)>,
}

impl BarrierAligner {
    pub fn new(input_count: usize) -> Self {
        Self {
            input_count,
            current_epoch: None,
            reached_inputs: HashSet::new(),
            buffered_events: Vec::new(),
        }
    }

    pub fn is_blocked(&self, input_idx: usize) -> bool {
        self.current_epoch.is_some() && self.reached_inputs.contains(&input_idx)
    }

    pub fn buffer_event(&mut self, input_idx: usize, event: TrackedEvent) {
        self.buffered_events.push((input_idx, event));
    }

    pub fn mark(&mut self, input_idx: usize, barrier: &CheckpointBarrier) -> AlignmentStatus {
        if self.current_epoch != Some(barrier.epoch) {
            self.current_epoch = Some(barrier.epoch);
            self.reached_inputs.clear();
            self.buffered_events.clear();
        }

        self.reached_inputs.insert(input_idx);

        if self.reached_inputs.len() == self.input_count {
            let released = std::mem::take(&mut self.buffered_events);
            self.current_epoch = None;
            self.reached_inputs.clear();
            AlignmentStatus::Complete(released)
        } else {
            AlignmentStatus::Pending
        }
    }
}