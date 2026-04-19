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

//! Runtime-installed SQL planning defaults (from `GlobalConfig` / `conf/config.yaml`).

use std::sync::OnceLock;

use crate::config::streaming_job::ResolvedStreamingJobConfig;
use crate::sql::common::constants::sql_planning_default;
use crate::sql::types::SqlConfig;

static SQL_PLANNING: OnceLock<SqlConfig> = OnceLock::new();

/// Installs [`SqlConfig`] derived from resolved streaming job YAML (KeyBy parallelism, etc.).
/// Safe to call once at bootstrap; later calls are ignored if already set.
pub fn install_sql_planning_from_streaming_job(job: &ResolvedStreamingJobConfig) {
    let cfg = SqlConfig {
        default_parallelism: sql_planning_default::DEFAULT_PARALLELISM,
        key_by_parallelism: job.key_by_parallelism as usize,
    };
    let _ = SQL_PLANNING.set(cfg).ok();
}

pub(crate) fn sql_planning_snapshot() -> SqlConfig {
    SQL_PLANNING.get().cloned().unwrap_or_default()
}
