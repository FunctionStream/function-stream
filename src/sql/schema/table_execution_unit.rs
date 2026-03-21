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

use super::temporal_pipeline_config::TemporalPipelineConfig;

#[derive(Debug, Clone)]
pub struct EngineDescriptor {
    pub engine_type: String,
    pub raw_payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncMode {
    AppendOnly,
    Incremental,
}

#[derive(Debug, Clone)]
pub struct TableExecutionUnit {
    pub label: String,
    pub engine_meta: EngineDescriptor,
    pub sync_mode: SyncMode,
    pub temporal_offset: TemporalPipelineConfig,
}
