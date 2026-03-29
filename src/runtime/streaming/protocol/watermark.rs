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

//! 水位线类型来自 `arroyo_types::Watermark`；此处提供 **多路对齐合并** 与 **单调推进** 判断。

use crate::sql::common::Watermark;

/// 多输入对齐：`Idle` 不参与事件时间取最小；若全部为 `Idle` 则输出 `Idle`。
/// 任一路尚未有水位线时返回 `None`（木桶短板未齐）。
pub fn merge_watermarks(per_input: &[Option<Watermark>]) -> Option<Watermark> {
    if per_input.iter().any(|w| w.is_none()) {
        return None;
    }

    let mut min_event: Option<std::time::SystemTime> = None;
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
        Some(Watermark::EventTime(
            min_event.expect("non-idle alignment must have at least one EventTime"),
        ))
    }
}

/// `new` 相对 `previous` 是否为 **严格推进**；`previous == None` 时恒为真。
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
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

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
