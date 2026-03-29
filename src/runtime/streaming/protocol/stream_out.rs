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

use arrow_array::RecordBatch;
use crate::sql::common::Watermark;

/// 算子产出的数据及下游 **路由意图**（由 `SubtaskRunner` 选择 `collect` / `collect_keyed` / `broadcast` / 水位广播）。
#[derive(Debug, Clone)]
pub enum StreamOutput {
    /// 发往所有下游（与 `TaskContext::collect` 一致：当前实现为每条边各发一份 `Data`）。
    Forward(RecordBatch),
    /// 按 `key_hash % outboxes.len()` 发往单一分区（KeyBy / Shuffle）。
    Keyed(u64, RecordBatch),
    /// 广播同一份数据到所有下游边（如 broadcast join）。
    Broadcast(RecordBatch),
    /// 向所有下游广播水位线（如表达式水位生成器）。
    Watermark(Watermark),
}
