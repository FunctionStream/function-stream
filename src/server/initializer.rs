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

use std::time::Instant;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::config::GlobalConfig;

pub type InitializerFn = fn(&GlobalConfig) -> Result<()>;

#[derive(Clone)]
pub struct Component {
    pub name: &'static str,
    pub initializer: InitializerFn,
}

pub struct ComponentRegistry {
    components: Vec<Component>,
}

#[derive(Default)]
pub struct ComponentRegistryBuilder {
    components: Vec<Component>,
}

impl ComponentRegistryBuilder {
    pub fn new() -> Self {
        Self {
            components: Vec::with_capacity(8),
        }
    }

    pub fn register(mut self, name: &'static str, initializer: InitializerFn) -> Self {
        self.components.push(Component { name, initializer });
        self
    }

    pub fn build(self) -> ComponentRegistry {
        ComponentRegistry {
            components: self.components,
        }
    }
}

impl ComponentRegistry {
    pub fn initialize_all(&self, config: &GlobalConfig) -> Result<()> {
        if self.components.is_empty() {
            warn!("Component registry is empty; no components to initialize");
            return Ok(());
        }

        let total = self.components.len();
        info!(
            total_components = total,
            "Commencing system initialization sequence"
        );

        for (index, component) in self.components.iter().enumerate() {
            let start_time = Instant::now();

            debug!(
                component = component.name,
                step = format!("{}/{}", index + 1, total),
                "Initializing component"
            );

            (component.initializer)(config).with_context(|| {
                format!("Fatal error initializing component: {}", component.name)
            })?;

            debug!(
                component = component.name,
                elapsed_ms = start_time.elapsed().as_millis(),
                "Component initialized successfully"
            );
        }

        info!("System initialization sequence completed successfully");
        Ok(())
    }
}

pub fn build_core_registry() -> ComponentRegistry {
    let builder = {
        let b = ComponentRegistryBuilder::new()
            .register("WasmCache", initialize_wasm_cache)
            .register("TaskManager", initialize_task_manager)
            .register("GlobalMemoryPool", initialize_global_memory_pool)
            .register("JobManager", initialize_job_manager);
        #[cfg(feature = "python")]
        let b = b.register("PythonService", initialize_python_service);
        b
    };

    builder
        .register(
            "StreamCatalog",
            crate::storage::stream_catalog::initialize_stream_catalog,
        )
        .register("Coordinator", initialize_coordinator)
        .build()
}

pub fn bootstrap_system(config: &GlobalConfig) -> Result<()> {
    let registry = build_core_registry();

    registry.initialize_all(config)?;

    crate::storage::stream_catalog::restore_global_catalog_from_store();
    crate::storage::stream_catalog::restore_streaming_jobs_from_store();

    info!("System bootstrap finished. Node is ready to accept traffic.");
    Ok(())
}

fn initialize_wasm_cache(config: &GlobalConfig) -> Result<()> {
    crate::runtime::processor::wasm::wasm_cache::set_cache_config(
        crate::runtime::processor::wasm::wasm_cache::WasmCacheConfig {
            enabled: config.wasm.enable_cache,
            cache_dir: crate::config::paths::resolve_path(&config.wasm.cache_dir),
            max_size: config.wasm.max_cache_size,
        },
    );

    debug!(
        enabled = config.wasm.enable_cache,
        dir = %config.wasm.cache_dir,
        max_size = config.wasm.max_cache_size,
        "WASM cache configured"
    );

    Ok(())
}

fn initialize_task_manager(config: &GlobalConfig) -> Result<()> {
    crate::runtime::wasm::taskexecutor::TaskManager::init(config)
        .context("TaskManager service failed to start")?;
    Ok(())
}

#[cfg(feature = "python")]
fn initialize_python_service(config: &GlobalConfig) -> Result<()> {
    crate::runtime::processor::python::PythonService::initialize(config)
        .context("Python Runtime initialization failed")?;
    Ok(())
}

// Streaming heap limits from config + host probe; shared by GlobalMemoryPool and JobManager.
fn resolve_streaming_memory_limits(config: &GlobalConfig) -> (u64, u64) {
    use crate::config::system::system_memory_info;

    let mem_info = system_memory_info().ok();
    let total_physical = mem_info.as_ref().map(|m| m.total_physical).unwrap_or(0);
    let auto_runtime_bytes = (total_physical as f64 * 0.8) as u64;

    let max_memory_bytes = config
        .streaming
        .max_memory_bytes
        .unwrap_or(if auto_runtime_bytes > 0 {
            auto_runtime_bytes
        } else {
            256 * 1024 * 1024
        });

    let per_operator_memory_bytes = config
        .streaming
        .per_operator_state_memory_bytes
        .unwrap_or(64 * 1024 * 1024);

    (max_memory_bytes, per_operator_memory_bytes)
}

// Singleton global memory pools (streaming + operator state); registered before JobManager.
fn initialize_global_memory_pool(config: &GlobalConfig) -> Result<()> {
    use crate::config::system::system_memory_info;

    let mem_info = system_memory_info().ok();
    let total_physical = mem_info.as_ref().map(|m| m.total_physical).unwrap_or(0);
    let avail_physical = mem_info.as_ref().map(|m| m.available_physical).unwrap_or(0);
    let total_virtual = mem_info.as_ref().map(|m| m.total_virtual).unwrap_or(0);
    let avail_virtual = mem_info.as_ref().map(|m| m.available_virtual).unwrap_or(0);

    let (max_memory_bytes, per_operator_memory_bytes) = resolve_streaming_memory_limits(config);

    info!(
        total_physical_mb = total_physical / (1024 * 1024),
        available_physical_mb = avail_physical / (1024 * 1024),
        total_virtual_mb = total_virtual / (1024 * 1024),
        available_virtual_mb = avail_virtual / (1024 * 1024),
        runtime_memory_mb = max_memory_bytes / (1024 * 1024),
        shared_state_memory_mb = per_operator_memory_bytes / (1024 * 1024),
        "GlobalMemoryPool: streaming + operator state limits (singleton)"
    );

    crate::runtime::memory::init_global_memory_pool(max_memory_bytes)
        .context("Global streaming memory pool initialization failed")?;
    crate::runtime::memory::init_global_state_memory_pool(per_operator_memory_bytes)
        .context("Global operator state memory pool initialization failed")?;

    info!("GlobalMemoryPool component initialized");
    Ok(())
}

fn initialize_job_manager(config: &GlobalConfig) -> Result<()> {
    use crate::runtime::streaming::factory::OperatorFactory;
    use crate::runtime::streaming::factory::Registry;
    use crate::runtime::streaming::job::{JobManager, StateConfig};
    use std::sync::Arc;

    let (_, per_operator_memory_bytes) = resolve_streaming_memory_limits(config);

    let registry = Arc::new(Registry::new());
    let factory = Arc::new(OperatorFactory::new(registry));

    let state_base_dir = std::env::temp_dir().join("function-stream").join("state");
    let state_config = StateConfig {
        per_operator_memory_bytes,
        ..StateConfig::default()
    };

    JobManager::init(factory, state_base_dir, state_config)
        .context("JobManager service failed to start")?;

    Ok(())
}

fn initialize_coordinator(_config: &GlobalConfig) -> Result<()> {
    crate::runtime::wasm::taskexecutor::TaskManager::get()
        .context("Dependency violation: Coordinator requires TaskManager")?;

    crate::runtime::memory::try_global_memory_pool()
        .context("Dependency violation: Coordinator requires GlobalMemoryPool")?;
    crate::runtime::memory::try_global_state_memory_pool()
        .context("Dependency violation: Coordinator requires GlobalMemoryPool (state sub-pool)")?;

    crate::storage::stream_catalog::CatalogManager::global()
        .context("Dependency violation: Coordinator requires StreamCatalog")?;

    crate::runtime::streaming::job::JobManager::global()
        .context("Dependency violation: Coordinator requires JobManager")?;

    Ok(())
}
