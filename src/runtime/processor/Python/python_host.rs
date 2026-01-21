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
// This module provides Python-specific WASM host implementation
// that manages a global engine and component for Python WASM runtime

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use wasmtime::{Engine, component::Component};

// Global Python WASM Engine (thread-safe, shareable)
static GLOBAL_PYTHON_ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();

// Global Python WASM Component (thread-safe, shareable)
static GLOBAL_PYTHON_COMPONENT: OnceLock<Arc<Component>> = OnceLock::new();

/// Get the default Python WASM file path
///
/// The path is relative to the project root:
/// `python/functionstream-runtime/target/functionstream-runtime.wasm`
fn get_python_wasm_path() -> PathBuf {
    // Try to get project root from environment or use current directory
    let project_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // If CARGO_MANIFEST_DIR is not set, try to find project root by going up
            let mut current = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            loop {
                if current.join("Cargo.toml").exists() || current.join("wit").exists() {
                    break;
                }
                if !current.pop() {
                    break;
                }
            }
            current
        });

    project_root
        .join("python")
        .join("functionstream-runtime")
        .join("target")
        .join("functionstream-runtime.wasm")
}

/// Get the cache directory for precompiled components
fn get_cache_dir() -> PathBuf {
    let project_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut current = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            loop {
                if current.join("Cargo.toml").exists() || current.join("wit").exists() {
                    break;
                }
                if !current.pop() {
                    break;
                }
            }
            current
        });

    project_root.join(".cache").join("python-wasm")
}

/// Get the cache file path for precompiled Python WASM component
fn get_cache_file_path() -> PathBuf {
    get_cache_dir().join("functionstream-runtime.cwasm")
}

/// Load Python WASM bytes from the default path
fn load_python_wasm_bytes() -> anyhow::Result<Vec<u8>> {
    let wasm_path = get_python_wasm_path();
    
    if !wasm_path.exists() {
        return Err(anyhow::anyhow!(
            "Python WASM file not found at: {}. Please build it first with: cd python/functionstream-runtime && make build",
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
        log::info!("[Python Host] Global Engine created: {:.3}s", engine_elapsed);

        Arc::new(engine)
    });

    Ok(Arc::clone(engine))
}

/// Load precompiled component from cache if available
fn load_precompiled_component(engine: &Engine) -> Option<Component> {
    let cache_path = get_cache_file_path();
    
    if !cache_path.exists() {
        return None;
    }

    match std::fs::read(&cache_path) {
        Ok(precompiled_bytes) => {
            log::info!("[Python Host] Loading precompiled component from cache: {}", cache_path.display());
            match unsafe { Component::deserialize(engine, &precompiled_bytes) } {
                Ok(component) => {
                    log::info!("[Python Host] Precompiled component loaded successfully from cache (size: {} KB)", 
                               precompiled_bytes.len() / 1024);
                    return Some(component);
                }
                Err(e) => {
                    log::warn!("[Python Host] Failed to deserialize cached component: {}. Removing invalid cache and falling back to compilation.", e);
                    // Remove invalid cache file
                    let _ = std::fs::remove_file(&cache_path);
                }
            }
        }
        Err(e) => {
            log::warn!("[Python Host] Failed to read cached component: {}. Falling back to compilation.", e);
        }
    }

    None
}

/// Save precompiled component to cache
fn save_precompiled_component(engine: &Engine, wasm_bytes: &[u8]) -> anyhow::Result<PathBuf> {
    let cache_dir = get_cache_dir();
    let cache_path = get_cache_file_path();

    // Create cache directory if it doesn't exist
    std::fs::create_dir_all(&cache_dir).map_err(|e| {
        anyhow::anyhow!("Failed to create cache directory {}: {}", cache_dir.display(), e)
    })?;

    log::info!("[Python Host] Precompiling component for cache...");
    
    // Precompile component
    let precompiled_bytes = engine.precompile_component(wasm_bytes).map_err(|e| {
        anyhow::anyhow!("Failed to precompile component: {}", e)
    })?;

    // Write to cache file
    std::fs::write(&cache_path, &precompiled_bytes).map_err(|e| {
        anyhow::anyhow!("Failed to write cached component to {}: {}", cache_path.display(), e)
    })?;

    log::info!("[Python Host] Cached precompiled component to {} (size: {} KB)", 
               cache_path.display(),
               precompiled_bytes.len() / 1024);

    Ok(cache_path)
}

/// Get or create global Python WASM Component
///
/// The component is loaded from cache if available, otherwise compiled from the built-in WASM file path.
/// If cache doesn't exist, the component is compiled and saved to cache for future use.
/// The WASM file path is: `python/functionstream-runtime/target/functionstream-runtime.wasm`
/// The cache path is: `.cache/python-wasm/functionstream-runtime.cwasm`
fn get_global_python_component() -> anyhow::Result<Arc<Component>> {
    if let Some(component) = GLOBAL_PYTHON_COMPONENT.get() {
        return Ok(Arc::clone(component));
    }

    let component = GLOBAL_PYTHON_COMPONENT.get_or_init(|| {
        let component_start = std::time::Instant::now();
        let engine = get_global_python_engine().unwrap_or_else(|e| {
            panic!("Failed to get global Python engine: {}", e);
        });

        // Try to load precompiled component from cache first
        if let Some(precompiled) = load_precompiled_component(&engine) {
            let component_elapsed = component_start.elapsed().as_secs_f64();
            log::info!("[Python Host] Precompiled component loaded from cache: {:.3}s", component_elapsed);
            return Arc::new(precompiled);
        }

        // Cache doesn't exist or is invalid, compile from WASM file
        log::info!("[Python Host] Loading Python WASM component from built-in path...");
        let wasm_bytes = load_python_wasm_bytes().unwrap_or_else(|e| {
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
        log::info!("[Python Host] Component compiled: {:.3}s (size: {} KB)", 
                   compile_elapsed,
                   wasm_bytes.len() / 1024);

        // Save precompiled component to cache for future use
        if let Err(e) = save_precompiled_component(&engine, &wasm_bytes) {
            log::warn!("[Python Host] Failed to save precompiled component to cache: {}", e);
            log::warn!("[Python Host] Component will be recompiled on next run");
        }

        let component_elapsed = component_start.elapsed().as_secs_f64();
        log::info!("[Python Host] Component ready: {:.3}s", component_elapsed);

        Arc::new(component)
    });

    Ok(Arc::clone(component))
}

/// Get global Python WASM Engine and Component
///
/// This function returns both the engine and component, ensuring they are initialized.
/// The component is loaded from the built-in path on first call.
pub fn get_python_engine_and_component() -> anyhow::Result<(Arc<Engine>, Arc<Component>)> {
    let engine = get_global_python_engine()?;
    let component = get_global_python_component()?;
    Ok((engine, component))
}

