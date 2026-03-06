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

// Python WASM Host
//
// This module provides python-specific wasm host implementation
// that manages a global engine and component for python wasm runtime.
// Configuration is used to specify paths instead of hardcoding.

use crate::config::PythonConfig;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};
use wasmtime::{Engine, component::Component};

// Global Python WASM Engine (thread-safe, shareable)
static GLOBAL_PYTHON_ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();

// Global Python WASM Component (thread-safe, shareable)
static GLOBAL_PYTHON_COMPONENT: OnceLock<Arc<Component>> = OnceLock::new();

// Global Python Configuration (initialized once)
static GLOBAL_PYTHON_CONFIG: OnceLock<RwLock<PythonConfig>> = OnceLock::new();

/// Initialize the Python host with configuration
///
/// This must be called before `get_python_engine_and_component`.
/// The configuration specifies paths for WASM file and cache directory.
///
/// # Arguments
/// - `config`: Python runtime configuration
///
/// # Returns
/// - `Ok(())`: Initialization successful
/// - `Err(...)`: Configuration already set or validation failed
pub fn initialize_config(config: &PythonConfig) -> anyhow::Result<()> {
    let wasm_path = config.wasm_path_buf();
    if !wasm_path.exists() {
        return Err(anyhow::anyhow!(
            "Python WASM file not found at: {}. Please ensure the file exists or build it first with: cd python/functionstream-runtime && make build",
            wasm_path.display()
        ));
    }

    // Store configuration
    match GLOBAL_PYTHON_CONFIG.set(RwLock::new(config.clone())) {
        Ok(_) => {
            log::info!(
                "[Python Host] Configuration initialized: wasm_path={}, cache_dir={}, enable_cache={}",
                config.wasm_path,
                config.cache_dir,
                config.enable_cache
            );
            Ok(())
        }
        Err(_) => {
            // Configuration already set, check if it matches
            if let Some(existing) = GLOBAL_PYTHON_CONFIG.get() {
                let existing_config = existing
                    .read()
                    .map_err(|e| anyhow::anyhow!("Failed to read existing configuration: {}", e))?;
                if existing_config.wasm_path == config.wasm_path
                    && existing_config.cache_dir == config.cache_dir
                    && existing_config.enable_cache == config.enable_cache
                {
                    log::debug!("[Python Host] Configuration already set with same values");
                    return Ok(());
                }
            }
            Err(anyhow::anyhow!(
                "Python host configuration has already been initialized with different values"
            ))
        }
    }
}

/// Get the current Python configuration
///
/// Returns the default configuration if not explicitly initialized.
fn get_config() -> PythonConfig {
    GLOBAL_PYTHON_CONFIG
        .get()
        .and_then(|lock| lock.read().ok())
        .map(|config| config.clone())
        .unwrap_or_default()
}

/// Load Python WASM bytes from the configured path
fn load_python_wasm_bytes(config: &PythonConfig) -> anyhow::Result<Vec<u8>> {
    let wasm_path = config.wasm_path_buf();

    if !wasm_path.exists() {
        return Err(anyhow::anyhow!(
            "Python WASM file not found at: {}. Please ensure the file exists or build it first with: cd python/functionstream-runtime && make build",
            wasm_path.display()
        ));
    }

    std::fs::read(&wasm_path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read Python WASM file from {}: {}",
            wasm_path.display(),
            e
        )
    })
}

/// Get or create global Python WASM Engine
///
/// The engine is initialized on first call and reused for all subsequent calls.
fn get_global_python_engine() -> anyhow::Result<Arc<Engine>> {
    if let Some(engine) = GLOBAL_PYTHON_ENGINE.get() {
        return Ok(Arc::clone(engine));
    }

    let engine = GLOBAL_PYTHON_ENGINE.get_or_init(|| {
        let engine_start = std::time::Instant::now();
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.async_support(false);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.debug_info(false);
        config.generate_address_map(false);
        config.parallel_compilation(true);

        let engine = Engine::new(&config).unwrap_or_else(|e| {
            panic!("Failed to create global Python WASM engine: {}", e);
        });

        let engine_elapsed = engine_start.elapsed().as_secs_f64();
        log::debug!(
            "[Python Host] Global Engine created: {:.3}s",
            engine_elapsed
        );

        Arc::new(engine)
    });

    Ok(Arc::clone(engine))
}

/// Load precompiled component from cache if available
fn load_precompiled_component(engine: &Engine, config: &PythonConfig) -> Option<Component> {
    if !config.enable_cache {
        log::debug!("[Python Host] Component caching is disabled");
        return None;
    }

    let cache_path = config.cwasm_cache_path();

    if !cache_path.exists() {
        log::debug!(
            "[Python Host] No cached component found at: {}",
            cache_path.display()
        );
        return None;
    }

    match std::fs::read(&cache_path) {
        Ok(precompiled_bytes) => {
            log::debug!(
                "[Python Host] Loading precompiled component from cache: {}",
                cache_path.display()
            );
            match unsafe { Component::deserialize(engine, &precompiled_bytes) } {
                Ok(component) => {
                    log::debug!(
                        "[Python Host] Precompiled component loaded successfully from cache (size: {} KB)",
                        precompiled_bytes.len() / 1024
                    );
                    return Some(component);
                }
                Err(e) => {
                    log::warn!(
                        "[Python Host] Failed to deserialize cached component: {}. Removing invalid cache and falling back to compilation.",
                        e
                    );
                    // Remove invalid cache file
                    let _ = std::fs::remove_file(&cache_path);
                }
            }
        }
        Err(e) => {
            log::warn!(
                "[Python Host] Failed to read cached component: {}. Falling back to compilation.",
                e
            );
        }
    }

    None
}

/// Save precompiled component to cache
fn save_precompiled_component(
    engine: &Engine,
    wasm_bytes: &[u8],
    config: &PythonConfig,
) -> anyhow::Result<PathBuf> {
    if !config.enable_cache {
        return Err(anyhow::anyhow!("Component caching is disabled"));
    }

    let cache_dir = config.cache_dir_buf();
    let cache_path = config.cwasm_cache_path();

    // Create cache directory if it doesn't exist
    std::fs::create_dir_all(&cache_dir).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create cache directory {}: {}",
            cache_dir.display(),
            e
        )
    })?;

    log::debug!("[Python Host] Precompiling component for cache...");

    // Precompile component
    let precompiled_bytes = engine
        .precompile_component(wasm_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to precompile component: {}", e))?;

    // Write to cache file
    std::fs::write(&cache_path, &precompiled_bytes).map_err(|e| {
        anyhow::anyhow!(
            "Failed to write cached component to {}: {}",
            cache_path.display(),
            e
        )
    })?;

    log::debug!(
        "[Python Host] Cached precompiled component to {} (size: {} KB)",
        cache_path.display(),
        precompiled_bytes.len() / 1024
    );

    Ok(cache_path)
}

/// Get or create global Python WASM Component
///
/// The component is loaded from cache if available, otherwise compiled from the WASM file.
/// If cache doesn't exist and caching is enabled, the component is compiled and saved to cache.
/// Configuration paths are used instead of hardcoded paths.
fn get_global_python_component() -> anyhow::Result<Arc<Component>> {
    if let Some(component) = GLOBAL_PYTHON_COMPONENT.get() {
        return Ok(Arc::clone(component));
    }

    let config = get_config();

    let component = GLOBAL_PYTHON_COMPONENT.get_or_init(|| {
        let component_start = std::time::Instant::now();
        let engine = get_global_python_engine().unwrap_or_else(|e| {
            panic!("Failed to get global Python engine: {}", e);
        });

        // Try to load precompiled component from cache first
        if let Some(precompiled) = load_precompiled_component(&engine, &config) {
            let component_elapsed = component_start.elapsed().as_secs_f64();
            log::debug!(
                "[Python Host] Precompiled component loaded from cache: {:.3}s",
                component_elapsed
            );
            return Arc::new(precompiled);
        }

        // Cache doesn't exist or is invalid, compile from WASM file
        log::debug!(
            "[Python Host] Loading Python WASM component from: {}",
            config.wasm_path
        );
        let wasm_bytes = load_python_wasm_bytes(&config).unwrap_or_else(|e| {
            panic!("Failed to load Python WASM bytes: {}", e);
        });

        let compile_start = std::time::Instant::now();
        let component = Component::from_binary(&engine, &wasm_bytes).unwrap_or_else(|e| {
            let error_msg = format!("Failed to parse Python WASM component: {}", e);
            log::error!("{}", error_msg);
            log::error!(
                "WASM bytes preview (first 100 bytes): {:?}",
                wasm_bytes.iter().take(100).collect::<Vec<_>>()
            );
            panic!("{}", error_msg);
        });

        let compile_elapsed = compile_start.elapsed().as_secs_f64();
        log::debug!(
            "[Python Host] Component compiled: {:.3}s (size: {} KB)",
            compile_elapsed,
            wasm_bytes.len() / 1024
        );

        // Save precompiled component to cache for future use
        if config.enable_cache
            && let Err(e) = save_precompiled_component(&engine, &wasm_bytes, &config)
        {
            log::warn!(
                "[Python Host] Failed to save precompiled component to cache: {}",
                e
            );
            log::warn!("[Python Host] Component will be recompiled on next run");
        }

        let component_elapsed = component_start.elapsed().as_secs_f64();
        log::debug!("[Python Host] Component ready: {:.3}s", component_elapsed);

        Arc::new(component)
    });

    Ok(Arc::clone(component))
}

/// Get global Python WASM Engine and Component
///
/// This function returns both the engine and component, ensuring they are initialized.
/// The component is loaded from the configured path on first call.
///
/// # Important
/// Call `initialize_config` before this function to set custom paths.
/// If not called, default configuration will be used.
pub fn get_python_engine_and_component() -> anyhow::Result<(Arc<Engine>, Arc<Component>)> {
    let engine = get_global_python_engine()?;
    let component = get_global_python_component()?;
    Ok((engine, component))
}
