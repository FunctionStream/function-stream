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
        info!(total_components = total, "Commencing system initialization sequence");

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
            .register("TaskManager", initialize_task_manager);
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
    crate::runtime::taskexecutor::TaskManager::init(config)
        .context("TaskManager service failed to start")?;
    Ok(())
}

#[cfg(feature = "python")]
fn initialize_python_service(config: &GlobalConfig) -> Result<()> {
    crate::runtime::processor::python::PythonService::initialize(config)
        .context("Python Runtime initialization failed")?;
    Ok(())
}

fn initialize_coordinator(_config: &GlobalConfig) -> Result<()> {
    crate::runtime::taskexecutor::TaskManager::get()
        .context("Dependency violation: Coordinator requires TaskManager")?;

    crate::storage::stream_catalog::CatalogManager::global()
        .context("Dependency violation: Coordinator requires StreamCatalog")?;

    Ok(())
}
