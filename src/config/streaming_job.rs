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

use serde::{Deserialize, Serialize};

pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 60 * 1000;
pub const DEFAULT_PIPELINE_PARALLELISM: u32 = 1;
pub const DEFAULT_KEY_BY_PARALLELISM: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamingJobConfig {
    #[serde(default)]
    pub checkpoint_interval_ms: Option<u64>,
    #[serde(default)]
    pub pipeline_parallelism: Option<u32>,
    /// Physical parallelism for KeyBy / key-extraction operators in planned streaming graphs.
    #[serde(default)]
    pub key_by_parallelism: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedStreamingJobConfig {
    pub checkpoint_interval_ms: u64,
    pub pipeline_parallelism: u32,
    pub key_by_parallelism: u32,
}

impl StreamingJobConfig {
    pub fn resolve(&self) -> ResolvedStreamingJobConfig {
        ResolvedStreamingJobConfig {
            checkpoint_interval_ms: self
                .checkpoint_interval_ms
                .filter(|&ms| ms > 0)
                .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL_MS),
            pipeline_parallelism: self
                .pipeline_parallelism
                .filter(|&p| p > 0)
                .unwrap_or(DEFAULT_PIPELINE_PARALLELISM),
            key_by_parallelism: self
                .key_by_parallelism
                .filter(|&p| p > 0)
                .unwrap_or(DEFAULT_KEY_BY_PARALLELISM),
        }
    }
}
