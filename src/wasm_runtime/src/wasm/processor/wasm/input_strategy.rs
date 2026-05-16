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

pub trait InputStrategy: Send {
    fn next_mask(&mut self, count: usize, last_idx: usize, finished: u64) -> u64;
}

pub struct SequentialStrategy;
impl InputStrategy for SequentialStrategy {
    fn next_mask(&mut self, count: usize, last_idx: usize, finished: u64) -> u64 {
        let alive = ((1u64 << count) - 1) & !finished;
        let last_bit = 1u64 << last_idx;
        if (last_bit & alive) != 0 {
            last_bit
        } else {
            1u64 << alive.trailing_zeros()
        }
    }
}

pub struct RoundRobinStrategy;
impl InputStrategy for RoundRobinStrategy {
    fn next_mask(&mut self, count: usize, last_idx: usize, finished: u64) -> u64 {
        let alive = ((1u64 << count) - 1) & !finished;
        let higher = alive & (!0u64 << (last_idx + 1));
        if higher != 0 {
            1u64 << higher.trailing_zeros()
        } else {
            1u64 << alive.trailing_zeros()
        }
    }
}

pub struct PriorityStrategy;
impl InputStrategy for PriorityStrategy {
    fn next_mask(&mut self, count: usize, _last_idx: usize, finished: u64) -> u64 {
        let alive = ((1u64 << count) - 1) & !finished;
        1u64 << alive.trailing_zeros()
    }
}

pub struct GroupParallelStrategy;
impl InputStrategy for GroupParallelStrategy {
    fn next_mask(&mut self, count: usize, last_idx: usize, finished: u64) -> u64 {
        let alive = ((1u64 << count) - 1) & !finished;
        let higher = alive & (!0u64 << (last_idx + 1));
        if higher != 0 {
            1u64 << higher.trailing_zeros()
        } else {
            1u64 << alive.trailing_zeros()
        }
    }
}

/// Supported selector names in YAML config
pub const SELECTOR_ROUND_ROBIN: &str = "round-robin";
pub const SELECTOR_SEQUENTIAL: &str = "sequential";
pub const SELECTOR_PRIORITY: &str = "priority";
pub const SELECTOR_GROUP_PARALLEL: &str = "group-parallel";

/// Create an InputStrategy from the configured selector name.
/// Returns None for unknown names; caller should fall back to round-robin.
pub fn from_selector_name(name: &str) -> Option<Box<dyn InputStrategy>> {
    match name.trim().to_lowercase().as_str() {
        SELECTOR_ROUND_ROBIN => Some(Box::new(RoundRobinStrategy)),
        SELECTOR_SEQUENTIAL => Some(Box::new(SequentialStrategy)),
        SELECTOR_PRIORITY => Some(Box::new(PriorityStrategy)),
        SELECTOR_GROUP_PARALLEL => Some(Box::new(GroupParallelStrategy)),
        _ => None,
    }
}
