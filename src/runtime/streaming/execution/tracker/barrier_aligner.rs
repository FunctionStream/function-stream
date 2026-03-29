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


use std::collections::HashSet;

use crate::sql::common::CheckpointBarrier;

#[derive(Debug)]
pub enum AlignmentStatus {
    Pending,
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
