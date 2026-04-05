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

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;

use crate::runtime::streaming::memory::MemoryTicket;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum Watermark {
    EventTime(SystemTime),
    Idle,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    pub epoch: u32,
    pub min_epoch: u32,
    pub timestamp: SystemTime,
    pub then_stop: bool,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Data(RecordBatch),
    Watermark(Watermark),
    Barrier(CheckpointBarrier),
    EndOfStream,
}

#[derive(Debug, Clone)]
pub enum StreamOutput {
    Forward(RecordBatch),
    Keyed(u64, RecordBatch),
    Broadcast(RecordBatch),
    Watermark(Watermark),
}

#[derive(Debug, Clone)]
pub struct TrackedEvent {
    pub event: StreamEvent,
    pub _ticket: Option<Arc<MemoryTicket>>,
}

impl TrackedEvent {
    pub fn new(event: StreamEvent, ticket: Option<MemoryTicket>) -> Self {
        Self {
            event,
            _ticket: ticket.map(Arc::new),
        }
    }

    pub fn control(event: StreamEvent) -> Self {
        Self {
            event,
            _ticket: None,
        }
    }
}

pub fn merge_watermarks(per_input: &[Option<Watermark>]) -> Option<Watermark> {
    if per_input.iter().any(|w| w.is_none()) {
        return None;
    }

    let mut min_event: Option<SystemTime> = None;
    let mut all_idle = true;

    for w in per_input.iter().flatten() {
        match w {
            Watermark::Idle => {}
            Watermark::EventTime(t) => {
                all_idle = false;
                min_event = Some(match min_event {
                    None => *t,
                    Some(m) => m.min(*t),
                });
            }
        }
    }

    if all_idle {
        Some(Watermark::Idle)
    } else {
        Some(Watermark::EventTime(min_event.expect(
            "non-idle alignment must have at least one EventTime",
        )))
    }
}

pub fn watermark_strictly_advances(new: Watermark, previous: Option<Watermark>) -> bool {
    match previous {
        None => true,
        Some(prev) => match (new, prev) {
            (Watermark::EventTime(tn), Watermark::EventTime(tp)) => tn > tp,
            (Watermark::Idle, Watermark::Idle) => false,
            (Watermark::Idle, Watermark::EventTime(_)) => true,
            (Watermark::EventTime(_), Watermark::Idle) => true,
        },
    }
}

#[cfg(test)]
mod watermark_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn merge_waits_for_all_channels() {
        let wms = vec![Some(Watermark::EventTime(SystemTime::UNIX_EPOCH)), None];
        assert!(merge_watermarks(&wms).is_none());
    }

    #[test]
    fn merge_min_event_time_ignores_idle() {
        let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let wms = vec![Some(Watermark::EventTime(t1)), Some(Watermark::Idle)];
        assert_eq!(merge_watermarks(&wms), Some(Watermark::EventTime(t1)));

        let wms = vec![
            Some(Watermark::EventTime(t1)),
            Some(Watermark::EventTime(t2)),
        ];
        assert_eq!(merge_watermarks(&wms), Some(Watermark::EventTime(t2)));
    }

    #[test]
    fn merge_all_idle() {
        let wms = vec![Some(Watermark::Idle), Some(Watermark::Idle)];
        assert_eq!(merge_watermarks(&wms), Some(Watermark::Idle));
    }
}
