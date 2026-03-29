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

use crate::runtime::streaming::protocol::watermark::{merge_watermarks, watermark_strictly_advances};
use crate::sql::common::Watermark;

#[derive(Debug)]
pub struct WatermarkTracker {
    watermarks: Vec<Option<Watermark>>,
    current_min_watermark: Option<Watermark>,
    eof_count: usize,
}

impl WatermarkTracker {
    pub fn new(input_count: usize) -> Self {
        Self {
            watermarks: vec![None; input_count],
            current_min_watermark: None,
            eof_count: 0,
        }
    }

    pub fn update(&mut self, input_idx: usize, wm: Watermark) -> Option<Watermark> {
        self.watermarks[input_idx] = Some(wm);

        if self.watermarks.iter().any(|w| w.is_none()) {
            return None;
        }

        let new_min = merge_watermarks(&self.watermarks)?;

        if !watermark_strictly_advances(new_min, self.current_min_watermark) {
            return None;
        }

        self.current_min_watermark = Some(new_min);
        Some(new_min)
    }

    pub fn increment_eof(&mut self) -> usize {
        self.eof_count += 1;
        self.eof_count
    }

    pub fn input_count(&self) -> usize {
        self.watermarks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn no_emit_until_all_inputs_seen() {
        let mut t = WatermarkTracker::new(2);
        let w = Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_secs(3));
        assert!(t.update(0, w).is_none());
        let w2 = Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
        assert_eq!(t.update(1, w2), Some(w2));
    }

    #[test]
    fn dedup_same_aligned() {
        let mut t = WatermarkTracker::new(1);
        let w = Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
        assert_eq!(t.update(0, w), Some(w));
        assert!(t.update(0, w).is_none());
    }

    #[test]
    fn advances_only_when_min_strictly_increases() {
        let mut t = WatermarkTracker::new(2);
        let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let t5 = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        assert!(t.update(0, Watermark::EventTime(t5)).is_none());
        assert_eq!(t.update(1, Watermark::EventTime(t1)), Some(Watermark::EventTime(t1)));
        let t3 = SystemTime::UNIX_EPOCH + Duration::from_secs(3);
        assert_eq!(
            t.update(1, Watermark::EventTime(t3)),
            Some(Watermark::EventTime(t3))
        );
        assert!(t.update(1, Watermark::EventTime(t3)).is_none());
    }

    #[test]
    fn backward_aligned_min_is_ignored() {
        let mut t = WatermarkTracker::new(2);
        let t5 = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let t10 = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        assert!(t.update(0, Watermark::EventTime(t10)).is_none());
        assert_eq!(
            t.update(1, Watermark::EventTime(t5)),
            Some(Watermark::EventTime(t5))
        );
        let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
        assert!(t.update(0, Watermark::EventTime(t2)).is_none());
    }
}
