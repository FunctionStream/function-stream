// Init Context - Initialization context
//
// Provides various resources needed for task initialization, including state storage, task storage, thread pool, etc.

use crate::storage::state_backend::StateStorageServer;
use crate::storage::task::TaskStorage;
use crate::runtime::processor::WASM::thread_pool::{TaskThreadPool, ThreadGroup};
use std::sync::{Arc, Mutex};

/// Initialization context
/// 
/// Contains various resources needed for task initialization
#[derive(Clone)]
pub struct InitContext {
    /// State storage server
    pub state_storage_server: Arc<StateStorageServer>,
    /// Task storage instance
    pub task_storage: Arc<dyn TaskStorage>,
    /// Task thread pool
    pub thread_pool: Arc<TaskThreadPool>,
    /// Thread group registry (used to collect thread groups from all components)
    pub thread_group_registry: Arc<Mutex<Vec<ThreadGroup>>>,
}

impl InitContext {
    /// Create a new initialization context
    /// 
    /// # Arguments
    /// - `state_storage_server`: State storage server
    /// - `task_storage`: Task storage instance
    /// - `thread_pool`: Task thread pool
    /// 
    /// # Returns
    /// New InitContext instance
    pub fn new(
        state_storage_server: Arc<StateStorageServer>,
        task_storage: Arc<dyn TaskStorage>,
        thread_pool: Arc<TaskThreadPool>,
    ) -> Self {
        Self {
            state_storage_server,
            task_storage,
            thread_pool,
            thread_group_registry: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a thread group
    /// 
    /// Components call this method during initialization to register their thread groups
    /// 
    /// # Arguments
    /// - `thread_group`: Thread group to register
    pub fn register_thread_group(&self, thread_group: ThreadGroup) {
        let mut registry = self.thread_group_registry.lock().unwrap();
        registry.push(thread_group);
    }

    /// Get all registered thread groups
    /// 
    /// # Returns
    /// All registered thread groups (will be removed from the registry)
    pub fn take_thread_groups(&self) -> Vec<ThreadGroup> {
        let mut registry = self.thread_group_registry.lock().unwrap();
        std::mem::take(&mut *registry)
    }
}

