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

use crate::config::GlobalConfig;
use anyhow::{Context, Result};

type InitializerFn = fn(&GlobalConfig) -> Result<()>;

#[derive(Clone)]
struct Component {
    name: &'static str,
    initializer: InitializerFn,
}

#[derive(Default)]
pub struct ComponentRegistryBuilder {
    components: Vec<Component>,
}

impl ComponentRegistryBuilder {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(8)
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            components: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn register(mut self, name: &'static str, initializer: InitializerFn) -> Self {
        self.components.push(Component { name, initializer });
        self
    }

    #[inline]
    pub fn build(self) -> ComponentRegistry {
        ComponentRegistry {
            components: self.components,
        }
    }
}

pub struct ComponentRegistry {
    components: Vec<Component>,
}

impl ComponentRegistry {
    pub fn initialize_all(&self, config: &GlobalConfig) -> Result<()> {
        if self.components.is_empty() {
            log::warn!("No components registered for initialization");
            return Ok(());
        }

        log::info!("Initializing {} components...", self.components.len());

        for (idx, component) in self.components.iter().enumerate() {
            let start = std::time::Instant::now();
            log::debug!(
                "[{}/{}] Initializing component: {}",
                idx + 1,
                self.components.len(),
                component.name
            );

            (component.initializer)(config)
                .with_context(|| format!("Component '{}' initialization failed", component.name))?;

            let elapsed = start.elapsed();
            log::debug!(
                "[{}/{}] Component '{}' initialized successfully in {:?}",
                idx + 1,
                self.components.len(),
                component.name,
                elapsed
            );
        }

        log::info!(
            "All {} components initialized successfully",
            self.components.len()
        );
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.components.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }
}

fn initialize_wasm_cache(config: &GlobalConfig) -> Result<()> {
    crate::runtime::processor::wasm::wasm_cache::set_cache_config(
        crate::runtime::processor::wasm::wasm_cache::WasmCacheConfig {
            enabled: config.wasm.enable_cache,
            cache_dir: crate::config::paths::resolve_path(&config.wasm.cache_dir),
            max_size: config.wasm.max_cache_size,
        },
    );
    log::info!(
        "WASM cache configuration: enabled={}, dir={}, max_size={} bytes",
        config.wasm.enable_cache,
        config.wasm.cache_dir,
        config.wasm.max_cache_size
    );
    Ok(())
}

fn initialize_task_manager(config: &GlobalConfig) -> Result<()> {
    crate::runtime::taskexecutor::TaskManager::init(config)
        .context("TaskManager initialization failed")?;
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
        .context("Coordinator requires TaskManager to be initialized first")?;
    log::info!("Coordinator verified and ready");
    Ok(())
}

pub fn register_components() -> ComponentRegistry {
    let builder = {
        let b = ComponentRegistryBuilder::new()
            .register("WasmCache", initialize_wasm_cache)
            .register("TaskManager", initialize_task_manager);
        #[cfg(feature = "python")]
        let b = b.register("PythonService", initialize_python_service);
        b
    };

    builder
        .register("Coordinator", initialize_coordinator)
        .build()
}
