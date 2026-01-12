// TaskManager - Task manager
//
// Manages WASM-based task lifecycle, including configuration parsing, file persistence, concurrency control and state transitions.

use crate::runtime::task::TaskLifecycle;
use crate::runtime::common::ComponentState;
use crate::runtime::processor::WASM::thread_pool::{GlobalTaskThreadPool, TaskThreadPool};
use crate::storage::state_backend::StateStorageServer;
use crate::storage::task::{TaskStorage, TaskStorageFactory};
use crate::config::GlobalConfig;
use crate::runtime::taskexecutor::init_context::InitContext;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock, OnceLock};

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
        let manager = Arc::new(Self::init_task_manager(config)
            .context("Failed to initialize TaskManager")?);
        
        // Set global instance
        GLOBAL_TASK_MANAGER.set(manager)
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
        GLOBAL_TASK_MANAGER.get()
            .map(Arc::clone)
            .ok_or_else(|| anyhow!("TaskManager has not been initialized. Call TaskManager::init() first."))
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
    fn init_task_manager(
        config: &GlobalConfig,
    ) -> Result<Self> {
        // Get global thread pool (already initialized in init)
        let thread_pool = GlobalTaskThreadPool::get_or_create();
        
        // Get state storage configuration from global configuration
        let state_storage_config = config.state_storage.clone();
        
        // If state storage configuration has base_dir, ensure directory exists
        if let Some(ref base_dir) = state_storage_config.base_dir {
            let base_path = std::path::Path::new(base_dir);
        if !base_path.exists() {
                fs::create_dir_all(base_path)
                .context("Failed to create base dir")?;
        }
        }
        
        // Create state storage server
        let state_storage_server = StateStorageServer::new(state_storage_config)
            .map_err(|e| anyhow!("Failed to create state storage server: {}", e))?;
        
        // Create task storage instance
        // TaskStorage uses task name as key, so one instance can manage all tasks
        // If db_path is specified in config, use that path; otherwise use default path "data/task/shared"
        // Note: "shared" is used as task name to build path, but this storage instance will actually manage all tasks
        // Because TaskStorage uses task name as key, not database path
        let task_storage = TaskStorageFactory::create_storage(&config.task_storage, "shared")?;
        
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

    /// Register task
    /// 
    /// # Arguments
    /// - `config_bytes`: Configuration file byte array (YAML format)
    /// - `wasm_bytes`: WASM binary package byte array
    /// 
    /// # Returns
    /// - `Ok(())`: Registration successful
    /// - `Err(...)`: Registration failed
    pub fn register_task(
        &self,
        config_bytes: &[u8],
        wasm_bytes: &[u8],
    ) -> Result<()> {
        let total_start = std::time::Instant::now();
        log::debug!("Registering task: config size={} bytes, wasm size={} bytes", config_bytes.len(), wasm_bytes.len());
        
        // 1. Create TaskLifecycle using TaskBuilder
        let step1_start = std::time::Instant::now();
        log::debug!("Step 1: Creating task from YAML config and WASM bytes");
        let task = crate::runtime::task::TaskBuilder::from_yaml_config(
            config_bytes,
            wasm_bytes,
        )
        .map_err(|e| {
            let config_str = String::from_utf8_lossy(config_bytes);
            log::error!("Failed to create task from config. Config preview (first 500 chars): {}", 
                config_str.chars().take(500).collect::<String>());
            anyhow!("Failed to create task from config: {}. Error details: {}", e, e)
        })?;
        let step1_elapsed = step1_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 1 - TaskBuilder::from_yaml_config: {:.3}s", step1_elapsed);

        // 2. Get task name
        let step2_start = std::time::Instant::now();
        let task_name = task.get_name().to_string();
        let step2_elapsed = step2_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 2 - Get task name: {:.3}s", step2_elapsed);
        log::debug!("Step 2: Task name extracted: '{}'", task_name);

        // 3. Check if task already exists
        let step3_start = std::time::Instant::now();
        log::debug!("Step 3: Checking if task '{}' already exists", task_name);
        {
            let map = self.tasks.read().map_err(|_| anyhow!("Lock poisoned when reading tasks map"))?;
            if map.contains_key(&task_name) {
                let existing_tasks: Vec<String> = map.keys().cloned().collect();
                log::error!("Task '{}' already exists. Existing tasks: {:?}", task_name, existing_tasks);
                return Err(anyhow!("Task '{}' already exists. Existing tasks: {:?}", task_name, existing_tasks));
            }
        }
        let step3_elapsed = step3_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 3 - Check task exists: {:.3}s", step3_elapsed);

        // 4. Add task to tasks map
        let step4_start = std::time::Instant::now();
        log::debug!("Step 4: Adding task '{}' to tasks map", task_name);
        let task_arc = {
            let mut map = self.tasks.write().map_err(|_| anyhow!("Lock poisoned when writing to tasks map"))?;
            let task_arc = Arc::new(RwLock::new(task));
            map.insert(task_name.clone(), task_arc.clone());
            task_arc
        };
        let step4_elapsed = step4_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 4 - Add to tasks map: {:.3}s", step4_elapsed);

        // 5. Create initialization context (includes thread pool)
        let step5_start = std::time::Instant::now();
        log::debug!("Step 5: Creating init context for task '{}'", task_name);
        let init_context = InitContext::new(
            self.state_storage_server.clone(),
            self.task_storage.clone(),
            self.thread_pool.clone(),
        );
        let step5_elapsed = step5_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 5 - Create init context: {:.3}s", step5_elapsed);

        // 6. Initialize task
        let step6_start = std::time::Instant::now();
        log::debug!("Step 6: Initializing task '{}' with context", task_name);
        {
            let lock_start = std::time::Instant::now();
            let mut task_guard = task_arc.write()
                .map_err(|e| anyhow!("Lock poisoned when getting write lock for task '{}': {}", task_name, e))?;
            let lock_elapsed = lock_start.elapsed().as_secs_f64();
            log::info!("[Timing] Step 6a - Get write lock: {:.3}s", lock_elapsed);
            
            let init_start = std::time::Instant::now();
            task_guard.init_with_context(&init_context)
                .map_err(|e| {
                    log::error!("Failed to initialize task '{}'. Error: {}", task_name, e);
                    log::error!("Error chain: {:?}", e);
                    anyhow!("Failed to initialize task '{}': {}. Full error: {:?}", task_name, e, e)
                })?;
            let init_elapsed = init_start.elapsed().as_secs_f64();
            log::info!("[Timing] Step 6b - init_with_context: {:.3}s", init_elapsed);
        }
        let step6_elapsed = step6_start.elapsed().as_secs_f64();
        log::info!("[Timing] Step 6 - Task initialization (total): {:.3}s", step6_elapsed);

        let total_elapsed = total_start.elapsed().as_secs_f64();
        log::info!("[Timing] register_task total: {:.3}s", total_elapsed);
        log::info!("Manager: Task '{}' registered and initialized successfully.", task_name);
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
        task_guard.start()
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
        task_guard.stop()
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
        task_guard.close()
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
        task_guard.take_checkpoint(checkpoint_id)
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

    /// Remove task (must close first)
    /// 
    /// # Arguments
    /// - `name`: Task name
    /// 
    /// # Returns
    /// - `Ok(())`: Removal successful
    /// - `Err(...)`: Removal failed
    pub fn remove_task(&self, name: &str) -> Result<()> {
        // Check task state
        let task = self.get_task(name)?;
        let task_guard = task.read().map_err(|_| anyhow!("Lock poisoned"))?;
        let status = task_guard.get_state();
        
        if !status.is_closed() {
            return Err(anyhow!("Cannot remove task '{}' with status {}. Close it first.", name, status));
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
}

