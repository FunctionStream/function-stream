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

// WasmProcessor implementation
//
// This module provides a concrete implementation of the WasmProcessor trait
// that can load and execute WebAssembly modules.

use super::wasm_host::{HostState, Processor};
use super::wasm_processor_trait::WasmProcessor;
use crate::runtime::output::Output;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use wasmtime::{Engine, Store, component::Component};

/// Error types for WasmProcessor
#[derive(Debug)]
pub enum WasmProcessorError {
    /// Failed to load wasm module
    LoadError(String),
    /// Failed to initialize wasm module
    InitError(String),
    /// Failed to execute wasm function
    ExecutionError(String),
    /// wasm module not found
    ModuleNotFound(String),
    /// Invalid wasm module
    InvalidModule(String),
}

impl fmt::Display for WasmProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WasmProcessorError::LoadError(msg) => write!(f, "Failed to load wasm module: {}", msg),
            WasmProcessorError::InitError(msg) => {
                write!(f, "Failed to initialize wasm module: {}", msg)
            }
            WasmProcessorError::ExecutionError(msg) => {
                write!(f, "Failed to execute wasm function: {}", msg)
            }
            WasmProcessorError::ModuleNotFound(path) => {
                write!(f, "wasm module not found: {}", path)
            }
            WasmProcessorError::InvalidModule(msg) => write!(f, "Invalid wasm module: {}", msg),
        }
    }
}

impl Error for WasmProcessorError {}

pub struct WasmProcessorImpl {
    modules: Vec<(String, Vec<u8>)>,
    name: String,
    init_config: std::collections::HashMap<String, String>,
    initialized: bool,
    current_watermark: Option<u64>,
    last_checkpoint_id: Option<u64>,
    is_healthy: bool,
    error_count: u32,
    processor: RefCell<Option<Processor>>,
    store: RefCell<Option<Store<HostState>>>,
    custom_engine: Option<Arc<Engine>>,
    custom_component: Option<Component>,
    use_custom_engine_and_component: bool,
}

unsafe impl Send for WasmProcessorImpl {}
unsafe impl Sync for WasmProcessorImpl {}

impl WasmProcessorImpl {
    pub fn new(
        name: String,
        module_bytes: Vec<u8>,
        init_config: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            modules: vec![(String::new(), module_bytes)],
            init_config,
            initialized: false,
            current_watermark: None,
            last_checkpoint_id: None,
            is_healthy: true,
            error_count: 0,
            processor: RefCell::new(None),
            store: RefCell::new(None),
            custom_engine: None,
            custom_component: None,
            use_custom_engine_and_component: false,
        }
    }

    pub fn new_with_custom_engine_and_component(
        name: String,
        modules: &[(String, Vec<u8>)],
        init_config: std::collections::HashMap<String, String>,
        custom_engine: Arc<Engine>,
        custom_component: Component,
    ) -> Self {
        Self {
            name,
            modules: modules.to_vec(),
            init_config,
            initialized: false,
            current_watermark: None,
            last_checkpoint_id: None,
            is_healthy: true,
            error_count: 0,
            processor: RefCell::new(None),
            store: RefCell::new(None),
            custom_engine: Some(custom_engine),
            custom_component: Some(custom_component),
            use_custom_engine_and_component: true,
        }
    }

    /// Get the processor name
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl WasmProcessor for WasmProcessorImpl {
    fn init_with_context(
        &mut self,
        _init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn Error + Send>> {
        if self.initialized {
            log::warn!("WasmProcessor '{}' already initialized", self.name);
            return Ok(());
        }

        self.initialized = true;
        self.is_healthy = true;
        self.error_count = 0;
        Ok(())
    }

    /// Process input data using the wasm module
    ///
    /// # Arguments
    /// * `data` - Input data as bytes
    /// * `input_index` - Index of the input source (0-based)
    ///
    /// # Note
    /// The actual processed data is sent via collector::emit in wasm
    fn process(&self, data: Vec<u8>, input_index: usize) -> Result<(), Box<dyn Error + Send>> {
        if !self.initialized {
            return Err(Box::new(WasmProcessorError::InitError(
                "Processor not initialized. Call init_with_context() first.".to_string(),
            )));
        }

        // Get mutable references to processor and store
        let processor_ref = self.processor.borrow();
        let processor = processor_ref
            .as_ref()
            .ok_or_else(|| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::InitError(
                    "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
                ))
            })?;

        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        processor
            .call_fs_process(&mut *store, input_index as u32, &data)
            .map_err(|e| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to call wasm process: {}",
                    e
                )))
            })?;

        Ok(())
    }

    /// Process watermark
    ///
    /// # Arguments
    /// * `timestamp` - Watermark timestamp
    /// * `input_index` - Index of the input source that generated the watermark (0-based)
    fn process_watermark(
        &mut self,
        timestamp: u64,
        input_index: usize,
    ) -> Result<(), Box<dyn Error + Send>> {
        if !self.initialized {
            return Err(Box::new(WasmProcessorError::InitError(
                "Processor not initialized. Call init_with_context() first.".to_string(),
            )));
        }

        // Get mutable references to processor and store
        let processor_ref = self.processor.borrow();
        let processor = processor_ref
            .as_ref()
            .ok_or_else(|| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::InitError(
                    "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
                ))
            })?;

        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        // Call wasm process_watermark function
        // WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
        processor
            .call_fs_process_watermark(store, input_index as u32, timestamp)
            .map_err(|e| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to call wasm process_watermark: {}",
                    e
                )))
            })?;

        // Update current watermark
        #[allow(clippy::unnecessary_map_or)]
        if self.current_watermark.map_or(true, |w| timestamp > w) {
            self.current_watermark = Some(timestamp);
            log::debug!(
                "WasmProcessor '{}' processed watermark: {} from input {}",
                self.name,
                timestamp,
                input_index
            );
        } else {
            log::warn!(
                "WasmProcessor '{}' received watermark {} from input {} which is not greater than current {}",
                self.name,
                timestamp,
                input_index,
                self.current_watermark.unwrap_or(0)
            );
        }

        Ok(())
    }

    /// Take a checkpoint
    fn take_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Box<dyn Error + Send>> {
        if !self.initialized {
            return Err(Box::new(WasmProcessorError::InitError(
                "Processor not initialized. Call init_with_context() first.".to_string(),
            )));
        }

        log::info!(
            "WasmProcessor '{}' taking checkpoint: {}",
            self.name,
            checkpoint_id
        );

        // Get mutable references to processor and store
        let processor_ref = self.processor.borrow();
        let processor = processor_ref
            .as_ref()
            .ok_or_else(|| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::InitError(
                    "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
                ))
            })?;

        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        // Call wasm take_checkpoint function
        // WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
        processor
            .call_fs_take_checkpoint(store, checkpoint_id)
            .map_err(|e| -> Box<dyn Error + Send> {
                Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to call wasm take_checkpoint: {}",
                    e
                )))
            })?;

        log::debug!(
            "WasmProcessor '{}' checkpoint {} created",
            self.name,
            checkpoint_id
        );

        // Store checkpoint metadata
        self.last_checkpoint_id = Some(checkpoint_id);

        // TODO: Persist checkpoint_data to storage
        // For now, only log, actual persistence logic should be handled by the caller

        Ok(())
    }

    /// Finish a checkpoint
    fn finish_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Box<dyn Error + Send>> {
        if !self.initialized {
            return Err(Box::new(WasmProcessorError::InitError(
                "Processor not initialized. Call init_with_context() first.".to_string(),
            )));
        }

        if self.last_checkpoint_id != Some(checkpoint_id) {
            return Err(Box::new(WasmProcessorError::ExecutionError(format!(
                "Checkpoint ID mismatch: expected {}, got {}",
                self.last_checkpoint_id.unwrap_or(0),
                checkpoint_id
            ))));
        }

        log::info!(
            "WasmProcessor '{}' finishing checkpoint: {}",
            self.name,
            checkpoint_id
        );

        // TODO: In a real implementation, you would:
        // 1. Finalize the checkpoint
        // 2. Commit checkpoint data to storage
        // 3. Clean up temporary checkpoint files

        Ok(())
    }

    /// Restore state from checkpoint
    fn restore_state(&mut self, checkpoint_id: u64) -> Result<(), Box<dyn Error + Send>> {
        log::info!(
            "WasmProcessor '{}' restoring state from checkpoint: {}",
            self.name,
            checkpoint_id
        );

        self.last_checkpoint_id = Some(checkpoint_id);
        self.is_healthy = true;
        self.error_count = 0;

        Ok(())
    }

    /// Check if the processor is healthy
    fn is_healthy(&self) -> bool {
        if !self.initialized {
            return false;
        }

        // Check if error count exceeds threshold
        if self.error_count > 10 {
            return false;
        }

        self.is_healthy
    }

    /// Close the wasm processor and clean up resources
    ///
    /// 1. Release any allocated resources
    /// 2. Finalize any pending checkpoints
    ///
    /// # Returns
    /// Ok(()) if cleanup succeeds, or an error if it fails
    fn close(&mut self) -> Result<(), Box<dyn Error + Send>> {
        if !self.initialized {
            log::warn!(
                "WasmProcessor '{}' not initialized, nothing to close",
                self.name
            );
            return Ok(());
        }

        log::info!("Closing WasmProcessor '{}'", self.name);

        // Reset state
        self.initialized = false;
        self.is_healthy = false;
        self.current_watermark = None;
        self.error_count = 0;

        log::info!("WasmProcessor '{}' closed successfully", self.name);
        Ok(())
    }

    fn init_wasm_host(
        &mut self,
        outputs: Vec<Box<dyn Output>>,
        init_context: &crate::runtime::taskexecutor::InitContext,
        task_name: String,
        create_time: u64,
    ) -> Result<(), Box<dyn Error + Send>> {
        use super::wasm_host::create_wasm_host;

        if self.processor.borrow().is_some() || self.store.borrow().is_some() {
            log::warn!("WasmHost for processor '{}' already initialized", self.name);
            return Ok(());
        }

        let (processor, store) = if self.use_custom_engine_and_component {
            let engine = self.custom_engine.as_ref().ok_or_else(|| {
                Box::new(WasmProcessorError::InitError(
                    "use_custom_engine_and_component is true but custom_engine is None".to_string(),
                )) as Box<dyn Error + Send>
            })?;
            let component = self.custom_component.as_ref().ok_or_else(|| {
                Box::new(WasmProcessorError::InitError(
                    "use_custom_engine_and_component is true but custom_component is None"
                        .to_string(),
                )) as Box<dyn Error + Send>
            })?;
            use super::wasm_host::create_wasm_host_with_component;
            create_wasm_host_with_component(
                engine,
                component,
                outputs,
                init_context,
                task_name,
                create_time,
            )
            .map_err(|e| -> Box<dyn Error + Send> {
                let error_msg = format!(
                    "Failed to create WasmHost with custom engine/component: {}",
                    e
                );
                log::error!("{}", error_msg);
                let mut full_error = error_msg.clone();
                let mut source = e.source();
                let mut depth = 0;
                while let Some(err) = source {
                    depth += 1;
                    full_error.push_str(&format!("\n  Caused by ({}): {}", depth, err));
                    source = err.source();
                    if depth > 10 {
                        full_error.push_str("\n  ... (error chain too long, truncated)");
                        break;
                    }
                }
                log::error!("Full error chain:\n{}", full_error);
                Box::new(WasmProcessorError::InitError(full_error))
            })?
        } else {
            let first_bytes = self
                .modules
                .first()
                .map(|(_, b)| b.as_slice())
                .unwrap_or(&[]);
            create_wasm_host(first_bytes, outputs, init_context, task_name, create_time).map_err(
                |e| -> Box<dyn Error + Send> {
                    let error_msg = format!("Failed to create WasmHost: {}", e);
                    log::error!("{}", error_msg);
                    let mut full_error = error_msg.clone();
                    let mut source = e.source();
                    let mut depth = 0;
                    while let Some(err) = source {
                        depth += 1;
                        full_error.push_str(&format!("\n  Caused by ({}): {}", depth, err));
                        source = err.source();
                        if depth > 10 {
                            full_error.push_str("\n  ... (error chain too long, truncated)");
                            break;
                        }
                    }
                    log::error!("Full error chain:\n{}", full_error);
                    Box::new(WasmProcessorError::InitError(full_error))
                },
            )?
        };

        *self.processor.borrow_mut() = Some(processor);
        *self.store.borrow_mut() = Some(store);

        let config_list: Vec<(String, String)> = self
            .init_config
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        {
            let processor_ref = self.processor.borrow();
            let processor = processor_ref.as_ref().unwrap();

            if self.use_custom_engine_and_component {
                let mut store_ref = self.store.borrow_mut();
                let store = store_ref.as_mut().unwrap();

                let class_name = self
                    .init_config
                    .get("class_name")
                    .or_else(|| self.init_config.get("processor_class"))
                    .cloned()
                    .unwrap_or_else(|| self.name.clone());

                tokio::task::block_in_place(|| {
                    processor
                        .call_fs_exec(store, &class_name, &self.modules)
                        .map_err(|e| -> Box<dyn Error + Send> {
                            Box::new(WasmProcessorError::InitError(format!(
                                "Failed to call fs_exec with class_name '{}' and modules: {}",
                                class_name, e
                            )))
                        })
                })?;
            }

            let mut store_ref = self.store.borrow_mut();
            let store = store_ref.as_mut().unwrap();
            tokio::task::block_in_place(|| {
                processor
                    .call_fs_init(store, &config_list)
                    .map_err(|e| -> Box<dyn Error + Send> {
                        Box::new(WasmProcessorError::InitError(format!(
                            "Failed to call fs_init: {}",
                            e
                        )))
                    })
            })?;
        }

        Ok(())
    }

    fn start_outputs(&mut self) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter_mut().enumerate() {
            if let Err(e) = out.start() {
                log::error!("Failed to start output {}: {}", idx, e);
                return Err(Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to start output {}: {}",
                    idx, e
                ))));
            }
        }

        log::debug!(
            "All {} outputs started successfully",
            host_state.outputs.len()
        );
        Ok(())
    }

    fn stop_outputs(&mut self) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter_mut().enumerate() {
            if let Err(e) = out.stop() {
                log::warn!("Failed to stop output {}: {}", idx, e);
            }
        }

        log::debug!("All {} outputs stopped", host_state.outputs.len());
        Ok(())
    }

    fn take_checkpoint_outputs(&mut self, checkpoint_id: u64) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter_mut().enumerate() {
            if let Err(e) = out.take_checkpoint(checkpoint_id) {
                log::error!("Failed to checkpoint output {}: {}", idx, e);
                return Err(Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to checkpoint output {}: {}",
                    idx, e
                ))));
            }
        }

        log::debug!(
            "Checkpoint {} taken for all {} outputs",
            checkpoint_id,
            host_state.outputs.len()
        );
        Ok(())
    }

    fn finish_checkpoint_outputs(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter_mut().enumerate() {
            if let Err(e) = out.finish_checkpoint(checkpoint_id) {
                log::error!("Failed to finish checkpoint for output {}: {}", idx, e);
                return Err(Box::new(WasmProcessorError::ExecutionError(format!(
                    "Failed to finish checkpoint for output {}: {}",
                    idx, e
                ))));
            }
        }

        log::debug!(
            "Checkpoint {} finished for all {} outputs",
            checkpoint_id,
            host_state.outputs.len()
        );
        Ok(())
    }

    fn close_outputs(&mut self) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;

        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter_mut().enumerate() {
            if let Err(e) = out.stop() {
                log::warn!("Failed to stop output {} during close: {}", idx, e);
            }
            if let Err(e) = out.close() {
                log::warn!("Failed to close output {}: {}", idx, e);
            }
        }

        log::debug!("All {} outputs closed", host_state.outputs.len());
        Ok(())
    }

    fn set_error_state_outputs(&mut self) -> Result<(), Box<dyn Error + Send>> {
        let mut store_ref = self.store.borrow_mut();
        let store = store_ref.as_mut().ok_or_else(|| -> Box<dyn Error + Send> {
            Box::new(WasmProcessorError::InitError(
                "WasmHost not initialized. Call init_wasm_host() first.".to_string(),
            ))
        })?;
        let host_state = store.data_mut();
        for (idx, out) in host_state.outputs.iter().enumerate() {
            if let Err(e) = out.set_error_state() {
                log::error!("Failed to set error state on output {}: {}", idx, e);
            }
        }
        Ok(())
    }
}
