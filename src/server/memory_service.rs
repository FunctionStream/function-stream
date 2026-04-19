// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result};
use tracing::info;

use crate::config::{
    DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES, DEFAULT_STREAMING_RUNTIME_MEMORY_BYTES, GlobalConfig,
};

pub struct MemoryService;

impl MemoryService {
    pub fn initialize(config: &GlobalConfig) -> Result<()> {
        use crate::config::system::system_memory_info;

        let mem_info = system_memory_info().ok();
        let total_physical = mem_info.as_ref().map(|m| m.total_physical).unwrap_or(0);
        let avail_physical = mem_info.as_ref().map(|m| m.available_physical).unwrap_or(0);
        let total_virtual = mem_info.as_ref().map(|m| m.total_virtual).unwrap_or(0);
        let avail_virtual = mem_info.as_ref().map(|m| m.available_virtual).unwrap_or(0);

        let streaming_runtime_memory_bytes = config
            .streaming
            .streaming_runtime_memory_bytes
            .unwrap_or(DEFAULT_STREAMING_RUNTIME_MEMORY_BYTES);

        let operator_state_store_memory_bytes = config
            .streaming
            .operator_state_store_memory_bytes
            .unwrap_or(DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES);

        info!(
            total_physical_mb = total_physical / (1024 * 1024),
            available_physical_mb = avail_physical / (1024 * 1024),
            total_virtual_mb = total_virtual / (1024 * 1024),
            available_virtual_mb = avail_virtual / (1024 * 1024),
            streaming_runtime_memory_mb = streaming_runtime_memory_bytes / (1024 * 1024),
            operator_state_store_memory_mb = operator_state_store_memory_bytes / (1024 * 1024),
            "MemoryService: global streaming + operator state pools"
        );

        let total_pool_bytes =
            streaming_runtime_memory_bytes.saturating_add(operator_state_store_memory_bytes);
        crate::runtime::memory::init_global_memory_pool(total_pool_bytes)
            .context("Global memory pool initialization failed")?;

        info!("MemoryService initialized");
        Ok(())
    }
}
