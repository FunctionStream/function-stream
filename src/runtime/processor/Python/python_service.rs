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

// Python Service
//
// This module provides a service for initializing Python WASM runtime at startup
// with configuration support

use crate::config::GlobalConfig;
use anyhow::{Context, Result};
use log::info;

/// Python Service for initializing Python WASM runtime
pub struct PythonService;

impl PythonService {
    /// Initialize Python WASM runtime with configuration
    ///
    /// This method:
    /// 1. Loads the Python WASM component from the configured path
    /// 2. Initializes the engine and component for use
    /// 3. Uses configuration from GlobalConfig if provided
    ///
    /// # Arguments
    /// - `config`: Global configuration containing Python runtime settings
    ///
    /// # Returns
    /// - `Ok(())`: Initialization successful
    /// - `Err(...)`: Initialization failed
    pub fn initialize(config: &GlobalConfig) -> Result<()> {
        info!("Initializing Python WASM runtime...");

        let python_config = &config.python;

        info!(
            "Python WASM configuration: wasm_path={}, cache_dir={}, enable_cache={}",
            python_config.wasm_path,
            python_config.cache_dir,
            python_config.enable_cache
        );

        // Pre-initialize the Python WASM engine and component
        // This will load and compile the WASM component, or load from cache if available
        let (_engine, _component) = super::python_host::get_python_engine_and_component()
            .context("Failed to initialize Python WASM engine and component")?;

        info!("Python WASM runtime initialized successfully");

        Ok(())
    }

    /// Initialize Python WASM runtime with default configuration
    ///
    /// This is a convenience method that uses default configuration values
    ///
    /// # Returns
    /// - `Ok(())`: Initialization successful
    /// - `Err(...)`: Initialization failed
    pub fn initialize_with_defaults() -> Result<()> {
        let config = GlobalConfig::default();
        Self::initialize(&config)
    }
}

