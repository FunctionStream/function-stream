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

// TaskManager - Task manager
//
// Manages wasm-based task lifecycle, including configuration parsing, file persistence, concurrency control and state transitions.

use crate::config::GlobalConfig;
use crate::runtime::common::ComponentState;
use crate::runtime::processor::wasm::thread_pool::{GlobalTaskThreadPool, TaskThreadPool};
use crate::runtime::task::TaskLifecycle;
use crate::runtime::taskexecutor::init_context::InitContext;
use crate::storage::state_backend::StateStorageServer;
use crate::storage::task::{TaskStorage, TaskStorageFactory};
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, OnceLock, RwLock};

pub struct TaskManager {
    tasks: Arc<RwLock<HashMap<String, Arc<RwLock<Box<dyn TaskLifecycle>>>>>>,
    /// State storage server
    state_storage_server: Arc<StateStorageServer>,
    /// Task storage instance (used to manage information for all tasks)
    task_storage: Arc<dyn TaskStorage>,
    /// Task thread pool
    thread_pool: Arc<TaskThreadPool>,
}

/// Global TaskManager instance
static GLOBAL_TASK_MANAGER: OnceLock<Arc<TaskManager>> = OnceLock::new();

impl TaskManager {
    /// Initialize global task manager instance
    ///
    /// Must be called in the main thread, before server startup.
    /// Can only be called once, repeated calls will return an error.
    ///
    /// # Arguments
    /// - `config`: Global configuration
    ///
    /// # Returns
    /// - `Ok(())`: Initialization successful
    /// - `Err(...)`: Initialization failed or already initialized
    pub fn init(config: &GlobalConfig) -> Result<()> {
        // Check if already initialized
        if GLOBAL_TASK_MANAGER.get().is_some() {
            return Err(anyhow!("TaskManager has already been initialized"));
        }

        // Initialize global thread pool
        let _ = GlobalTaskThreadPool::get_or_create();

        // Initialize
        let manager =
            Arc::new(Self::init_task_manager(config).context("Failed to initialize TaskManager")?);

        // Set global instance
        GLOBAL_TASK_MANAGER
            .set(manager)
            .map_err(|_| anyhow!("Failed to set global TaskManager instance"))?;

        Ok(())
    }

    /// Get global task manager instance
    ///
    /// Must call `init` first before using.
    ///
    /// # Returns
    /// - `Ok(Arc<TaskManager>)`: Successfully obtained
    /// - `Err(...)`: Not yet initialized
    pub fn get() -> Result<Arc<Self>> {
        GLOBAL_TASK_MANAGER.get().map(Arc::clone).ok_or_else(|| {
            anyhow!("TaskManager has not been initialized. Call TaskManager::init() first.")
        })
    }

    /// Initialize task manager
    ///
    /// Create a new task manager using global thread pool.
    ///
    /// # Arguments
    /// - `config`: Global configuration
    ///
    /// # Returns
    /// - `Ok(TaskManager)`: Successfully created
    /// - `Err(...)`: Creation failed
    fn init_task_manager(config: &GlobalConfig) -> Result<Self> {
        // Get global thread pool (already initialized in init)
        let thread_pool = GlobalTaskThreadPool::get_or_create();

        // Get state storage configuration from global configuration
        let state_storage_config = config.state_storage.clone();

        // If state storage configuration has base_dir, ensure directory exists
        if let Some(ref base_dir) = state_storage_config.base_dir {
            let base_path = std::path::Path::new(base_dir);
            if !base_path.exists() {
                fs::create_dir_all(base_path).context("Failed to create base dir")?;
            }
        }

        // Create state storage server
        let state_storage_server = StateStorageServer::new(state_storage_config)
            .map_err(|e| anyhow!("Failed to create state storage server: {}", e))?;

        // Create task storage instance
        // TaskStorage uses task name as key, so one instance can manage all tasks
        // If db_path is specified in config, use that path; otherwise use default path "data/task"
        // TaskStorage uses task name as key, not database path
        let task_storage = TaskStorageFactory::create_storage(&config.task_storage)?;

        Ok(Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            state_storage_server: Arc::new(state_storage_server),
            task_storage: Arc::from(task_storage),
            thread_pool,
        })
    }

    /// Get state storage server
    ///
    /// # Returns
    /// - `Arc<StateStorageServer>`: State storage server
    pub fn state_storage_server(&self) -> Arc<StateStorageServer> {
        self.state_storage_server.clone()
    }

    /// Get task storage instance
    ///
    /// # Returns
    /// - `Arc<dyn TaskStorage>`: Task storage instance
    pub fn task_storage(&self) -> Arc<dyn TaskStorage> {
        self.task_storage.clone()
    }

    /// Get task thread pool
    ///
    /// # Returns
    /// - `Arc<TaskThreadPool>`: Task thread pool
    pub fn thread_pool(&self) -> Arc<TaskThreadPool> {
        self.thread_pool.clone()
    }

    /// Register task from YAML config and module bytes
    pub fn register_task(&self, config_bytes: &[u8], module_bytes: &[u8]) -> Result<()> {
        log::debug!(
            "Registering task: config={} bytes, module={} bytes",
            config_bytes.len(),
            module_bytes.len()
        );

        let task = crate::runtime::task::TaskBuilder::from_yaml_config(config_bytes, module_bytes)
            .map_err(|e| {
                let preview: String = String::from_utf8_lossy(config_bytes).chars().take(500).collect();
                log::error!("Failed to create task. Config preview: {}", preview);
                anyhow!("Failed to create task: {}", e)
            })?;

        self.register_task_internal(task)
    }

    /// Internal method to register and start a task
    fn register_task_internal(&self, task: Box<dyn crate::runtime::task::TaskLifecycle>) -> Result<()> {
        let task_name = task.get_name().to_string();

        // Check for duplicate
        {
            let map = self.tasks.read().map_err(|_| anyhow!("Lock poisoned"))?;
            if map.contains_key(&task_name) {
                return Err(anyhow!("Task '{}' already exists", task_name));
            }
        }

        // Add to map
        let task_arc = {
            let mut map = self.tasks.write().map_err(|_| anyhow!("Lock poisoned"))?;
            let arc = Arc::new(RwLock::new(task));
            map.insert(task_name.clone(), arc.clone());
            arc
        };

        // Initialize and start
        let init_context = InitContext::new(
            self.state_storage_server.clone(),
            self.task_storage.clone(),
            self.thread_pool.clone(),
        );

        {
            let mut guard = task_arc.write().map_err(|_| anyhow!("Lock poisoned"))?;
            guard.init_with_context(&init_context).map_err(|e| {
                log::error!("Failed to initialize task '{}': {}", task_name, e);
                anyhow!("Failed to initialize task '{}': {}", task_name, e)
            })?;
            guard.start().map_err(|e| {
                log::error!("Failed to start task '{}': {}", task_name, e);
                anyhow!("Failed to start task '{}': {}", task_name, e)
            })?;
        }

        log::info!("Task '{}' registered and started successfully", task_name);
        Ok(())
    }

    /// Start task
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(())`: Start successful
    /// - `Err(...)`: Start failed
    pub fn start_task(&self, name: &str) -> Result<()> {
        let task = self.get_task(name)?;
        let mut task_guard = task.write().map_err(|_| anyhow!("Lock poisoned"))?;
        task_guard
            .start()
            .map_err(|e| anyhow!("Failed to start task: {}", e))?;
        Ok(())
    }

    /// Stop task
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(())`: Stop successful
    /// - `Err(...)`: Stop failed
    pub fn stop_task(&self, name: &str) -> Result<()> {
        let task = self.get_task(name)?;
        let mut task_guard = task.write().map_err(|_| anyhow!("Lock poisoned"))?;
        task_guard
            .stop()
            .map_err(|e| anyhow!("Failed to stop task: {}", e))?;
        Ok(())
    }

    /// Close task
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(())`: Close successful
    /// - `Err(...)`: Close failed
    pub fn close_task(&self, name: &str) -> Result<()> {
        let task = self.get_task(name)?;
        let mut task_guard = task.write().map_err(|_| anyhow!("Lock poisoned"))?;
        task_guard
            .close()
            .map_err(|e| anyhow!("Failed to close task: {}", e))?;
        Ok(())
    }

    /// Execute checkpoint
    ///
    /// # Arguments
    /// - `name`: Task name
    /// - `checkpoint_id`: Checkpoint ID
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint successful
    /// - `Err(...)`: Checkpoint failed
    pub fn take_checkpoint(&self, name: &str, checkpoint_id: u64) -> Result<()> {
        let task = self.get_task(name)?;
        let mut task_guard = task.write().map_err(|_| anyhow!("Lock poisoned"))?;
        task_guard
            .take_checkpoint(checkpoint_id)
            .map_err(|e| anyhow!("Failed to take checkpoint: {}", e))?;
        Ok(())
    }

    /// Get task
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(Arc<RwLock<Box<dyn TaskLifecycle>>>)`: Task found
    /// - `Err(...)`: Task does not exist
    fn get_task(&self, name: &str) -> Result<Arc<RwLock<Box<dyn TaskLifecycle>>>> {
        let map = self.tasks.read().map_err(|_| anyhow!("Lock poisoned"))?;
        map.get(name)
            .cloned()
            .ok_or_else(|| anyhow!("Task '{}' not found", name))
    }

    /// Get task status
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(ComponentState)`: Task state
    /// - `Err(...)`: Task does not exist or failed to get state
    pub fn get_task_status(&self, name: &str) -> Result<ComponentState> {
        let task = self.get_task(name)?;
        let task_guard = task.read().map_err(|_| anyhow!("Lock poisoned"))?;
        Ok(task_guard.get_state())
    }

    /// List all task names
    ///
    /// # Returns
    /// - `Vec<String>`: Task name list
    pub fn list_tasks(&self) -> Vec<String> {
        let map = self.tasks.read().unwrap();
        map.keys().cloned().collect()
    }

    /// Remove task (will close first if not already closed)
    ///
    /// # Arguments
    /// - `name`: Task name
    ///
    /// # Returns
    /// - `Ok(())`: Removal successful
    /// - `Err(...)`: Removal failed
    pub fn remove_task(&self, name: &str) -> Result<()> {
        // Get task
        let task = self.get_task(name)?;
        
        // Close task if not already closed
        {
            let task_guard = task.read().map_err(|_| anyhow!("Lock poisoned"))?;
            let status = task_guard.get_state();
            drop(task_guard);
            
            if !status.is_closed() {
                let mut task_guard = task.write().map_err(|_| anyhow!("Lock poisoned"))?;
                task_guard.close().map_err(|e| {
                    log::error!("Failed to close task '{}' before removal. Error: {}", name, e);
                    anyhow!("Failed to close task '{}': {}", name, e)
                })?;
            }
        }

        // Remove from memory
        {
            let mut map = self.tasks.write().map_err(|_| anyhow!("Lock poisoned"))?;
            map.remove(name);
        }

        // TODO: Remove task information from task storage
        // Can use task_storage to remove task information

        log::info!("Manager: Task '{}' removed.", name);
        Ok(())
    }

    /// Register Python function as a task (for fs-exec)
    pub fn register_python_task(&self, config_bytes: &[u8], modules: &[(String, Vec<u8>)]) -> Result<()> {
        #[cfg(feature = "python")]
        {
            log::debug!(
                "Registering Python task: config={} bytes, modules={}",
                config_bytes.len(),
                modules.len()
            );

            let task = crate::runtime::task::TaskBuilder::from_python_config(config_bytes, modules)
                .map_err(|e| {
                    let preview: String = String::from_utf8_lossy(config_bytes).chars().take(500).collect();
                    log::error!("Failed to create Python task. Config preview: {}", preview);
                    anyhow!("Failed to create Python task: {}", e)
                })?;

            self.register_task_internal(task)
        }

        #[cfg(not(feature = "python"))]
        {
            let _ = (config_bytes, modules);
            Err(anyhow!("Python feature is not enabled"))
        }
    }
}
