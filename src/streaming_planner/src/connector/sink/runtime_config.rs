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

use std::collections::HashMap;

use datafusion::common::{DataFusionError, Result, plan_err};

use crate::common::connector_options::ConnectorOptions;
use crate::common::with_option_keys as opt;
use function_stream_config::global_config::{
    DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES, DEFAULT_SINK_BUFFER_MEMORY_BYTES,
};
use function_stream_config::streaming_job::DEFAULT_CHECKPOINT_INTERVAL_MS;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SinkRuntimeConfig {
    pub pipeline_parallelism: Option<u32>,
    pub key_by_parallelism: Option<u32>,
    pub checkpoint_interval_ms: u64,
    pub operator_memory_bytes: u64,
    pub sink_memory_bytes: u64,
}

pub type SinkRuntimeProperties = HashMap<String, String>;

impl SinkRuntimeConfig {
    pub fn extract_from_options(options: &mut ConnectorOptions) -> Result<Self> {
        let pipeline_parallelism = options
            .pull_opt_u64(opt::PIPELINE_PARALLELISM)?
            .map(|v| v as u32);
        let key_by_parallelism = options
            .pull_opt_u64(opt::KEY_BY_PARALLELISM)?
            .map(|v| v as u32);
        let checkpoint_interval_ms = options
            .pull_opt_u64(opt::CHECKPOINT_INTERVAL_MS)?
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL_MS);
        let operator_memory_bytes = options
            .pull_opt_u64(opt::OPERATOR_MEMORY_BYTES)?
            .unwrap_or(DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES);
        let sink_memory_bytes = options
            .pull_opt_u64(opt::SINK_MEMORY_BYTES)?
            .unwrap_or(DEFAULT_SINK_BUFFER_MEMORY_BYTES);
        Ok(Self {
            pipeline_parallelism,
            key_by_parallelism,
            checkpoint_interval_ms,
            operator_memory_bytes,
            sink_memory_bytes,
        })
    }

    pub fn from_options_map(opts: &HashMap<String, String>) -> Result<Self> {
        let pipeline_parallelism = parse_opt_u32(opts, opt::PIPELINE_PARALLELISM)?;
        let key_by_parallelism = parse_opt_u32(opts, opt::KEY_BY_PARALLELISM)?;
        let checkpoint_interval_ms = parse_opt_u64(opts, opt::CHECKPOINT_INTERVAL_MS)?
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL_MS);
        let operator_memory_bytes = parse_opt_u64(opts, opt::OPERATOR_MEMORY_BYTES)?
            .unwrap_or(DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES);
        let sink_memory_bytes = parse_opt_u64(opts, opt::SINK_MEMORY_BYTES)?
            .unwrap_or(DEFAULT_SINK_BUFFER_MEMORY_BYTES);
        Ok(Self {
            pipeline_parallelism,
            key_by_parallelism,
            checkpoint_interval_ms,
            operator_memory_bytes,
            sink_memory_bytes,
        })
    }

    pub fn to_runtime_properties(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if let Some(v) = self.pipeline_parallelism {
            out.insert(opt::PIPELINE_PARALLELISM.to_string(), v.to_string());
        }
        if let Some(v) = self.key_by_parallelism {
            out.insert(opt::KEY_BY_PARALLELISM.to_string(), v.to_string());
        }
        out.insert(
            opt::CHECKPOINT_INTERVAL_MS.to_string(),
            self.checkpoint_interval_ms.to_string(),
        );
        out.insert(
            opt::OPERATOR_MEMORY_BYTES.to_string(),
            self.operator_memory_bytes.to_string(),
        );
        out.insert(
            opt::SINK_MEMORY_BYTES.to_string(),
            self.sink_memory_bytes.to_string(),
        );
        out
    }
}

fn parse_opt_u32(opts: &HashMap<String, String>, key: &str) -> Result<Option<u32>> {
    let Some(raw) = opts.get(key) else {
        return Ok(None);
    };
    let normalized = normalize_numeric_option(raw);
    let parsed = normalized.parse::<u32>().map_err(|_| {
        DataFusionError::Plan(format!(
            "WITH option '{key}' expects unsigned integer, got '{raw}'"
        ))
    })?;
    if parsed == 0 {
        return plan_err!("WITH option '{key}' must be > 0");
    }
    Ok(Some(parsed))
}

fn parse_opt_u64(opts: &HashMap<String, String>, key: &str) -> Result<Option<u64>> {
    let Some(raw) = opts.get(key) else {
        return Ok(None);
    };
    let normalized = normalize_numeric_option(raw);
    let parsed = normalized.parse::<u64>().map_err(|_| {
        DataFusionError::Plan(format!(
            "WITH option '{key}' expects unsigned integer, got '{raw}'"
        ))
    })?;
    if parsed == 0 {
        return plan_err!("WITH option '{key}' must be > 0");
    }
    Ok(Some(parsed))
}

fn normalize_numeric_option(raw: &str) -> &str {
    raw.trim().trim_matches('\'').trim_matches('"').trim()
}
