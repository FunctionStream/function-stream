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
use crate::runtime::common::ComponentState;
use crate::runtime::processor::wasm::thread_pool::{GlobalTaskThreadPool, TaskThreadPool};
use crate::runtime::task::{TaskBuilder, TaskLifecycle};
use crate::runtime::taskexecutor::init_context::InitContext;
use crate::storage::state_backend::StateStorageServer;
use crate::storage::task::{
    FunctionInfo, StoredTaskInfo, TaskModuleBytes, TaskStorage, TaskStorageFactory,
};

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

type SharedTask = Arc<RwLock<Box<dyn TaskLifecycle>>>;
type TaskMap = Arc<RwLock<HashMap<String, SharedTask>>>;

pub struct TaskManager {
    tasks: TaskMap,
    state_storage_server: Arc<StateStorageServer>,
    task_storage: Arc<dyn TaskStorage>,
    thread_pool: Arc<TaskThreadPool>,
}

static GLOBAL_INSTANCE: OnceLock<Arc<TaskManager>> = OnceLock::new();

impl TaskManager {
    pub fn init(config: &GlobalConfig) -> Result<()> {
        if GLOBAL_INSTANCE.get().is_some() {
            return Err(anyhow!("TaskManager singleton already initialized"));
        }

        let _ = GlobalTaskThreadPool::get_or_create();

        let manager =
            Arc::new(Self::init_internal(config).context("Failed to construct TaskManager")?);
        manager
            .recover_tasks_from_storage()
            .context("Failed to recover persisted tasks")?;

        GLOBAL_INSTANCE
            .set(manager)
            .map_err(|_| anyhow!("Concurrency error during TaskManager singleton assignment"))?;

        Ok(())
    }

    pub fn get() -> Result<Arc<Self>> {
        GLOBAL_INSTANCE
            .get()
            .cloned()
            .ok_or_else(|| anyhow!("TaskManager not initialized. Call init() first."))
    }

    fn init_internal(config: &GlobalConfig) -> Result<Self> {
        let thread_pool = GlobalTaskThreadPool::get_or_create();

        let state_storage_server = Arc::new(
            StateStorageServer::new(config.state_storage.clone())
                .map_err(|e| anyhow!("Failed to create state storage server: {}", e))?,
        );

        let task_storage = Arc::from(TaskStorageFactory::create_storage(&config.task_storage)?);

        Ok(Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            state_storage_server,
            task_storage,
            thread_pool,
        })
    }
}

impl TaskManager {
    pub fn register_task(&self, config_bytes: &[u8], module_bytes: &[u8]) -> Result<()> {
        let task = TaskBuilder::from_yaml_config(config_bytes, module_bytes)
            .map_err(|e| anyhow!("Failed to build task: {}", e))?;
        let info = task.get_function_info();
        let task_info = StoredTaskInfo {
            name: info.name,
            task_type: info.task_type,
            module_bytes: Some(TaskModuleBytes::Wasm(module_bytes.to_vec())),
            config_bytes: config_bytes.to_vec(),
            state: ComponentState::Initialized,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checkpoint_id: None,
        };
        self.register_task_internal(task, Some(task_info))
    }

    pub fn register_python_task(
        &self,
        config_bytes: &[u8],
        modules: &[(String, Vec<u8>)],
    ) -> Result<()> {
        #[cfg(feature = "python")]
        {
            let task = TaskBuilder::from_python_config(config_bytes, modules)
                .map_err(|e| anyhow!("Failed to build Python task: {}", e))?;
            let (class_name, module_name, module_bytes) = match modules.first() {
                Some((name, bytes)) => (name.clone(), name.clone(), Some(bytes.clone())),
                None => (String::new(), String::new(), None),
            };
            let info = task.get_function_info();
            let task_info = StoredTaskInfo {
                name: info.name,
                task_type: info.task_type,
                module_bytes: Some(TaskModuleBytes::Python {
                    class_name,
                    module: module_name,
                    bytes: module_bytes,
                }),
                config_bytes: config_bytes.to_vec(),
                state: ComponentState::Initialized,
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                checkpoint_id: None,
            };
            self.register_task_internal(task, Some(task_info))
        }
        #[cfg(not(feature = "python"))]
        {
            let _ = (config_bytes, modules);
            Err(anyhow!("Python feature disabled in this build"))
        }
    }

    pub fn start_task(&self, name: &str) -> Result<()> {
        let task = self.get_task_handle(name)?;
        task.write()
            .start()
            .map_err(|e| anyhow!("Failed to start task: {}", e))
    }

    pub fn stop_task(&self, name: &str) -> Result<()> {
        let task = self.get_task_handle(name)?;
        task.write()
            .stop()
            .map_err(|e| anyhow!("Failed to stop task: {}", e))
    }

    pub fn close_task(&self, name: &str) -> Result<()> {
        let task = self.get_task_handle(name)?;
        task.write()
            .close()
            .map_err(|e| anyhow!("Failed to close task: {}", e))
    }

    pub fn remove_task(&self, name: &str) -> Result<()> {
        let task_handle = self.get_task_handle(name)?;

        {
            let mut handle = task_handle.write();
            if !handle.get_state().is_closed() {
                handle
                    .close()
                    .map_err(|e| anyhow!("Failed to close task before removal: {}", e))?;
            }
        }

        self.tasks.write().remove(name);
        self.task_storage
            .delete_task(name)
            .context("Failed to remove task from persistent storage")?;

        log::info!(target: "task_manager", "Task '{}' successfully purged", name);
        Ok(())
    }

    pub fn take_checkpoint(&self, name: &str, checkpoint_id: u64) -> Result<()> {
        let task = self.get_task_handle(name)?;
        task.write()
            .take_checkpoint(checkpoint_id)
            .map_err(|e| anyhow!("Checkpoint failed: {}", e))
    }
}

impl TaskManager {
    pub fn list_all_functions(&self) -> Vec<FunctionInfo> {
        let tasks = self.tasks.read();
        tasks
            .values()
            .map(|task_arc| task_arc.read().get_function_info())
            .collect()
    }

    pub fn get_task_status(&self, name: &str) -> Result<ComponentState> {
        Ok(self.get_task_handle(name)?.read().get_state())
    }

    pub fn state_storage_server(&self) -> Arc<StateStorageServer> {
        Arc::clone(&self.state_storage_server)
    }

    pub fn task_storage(&self) -> Arc<dyn TaskStorage> {
        Arc::clone(&self.task_storage)
    }

    pub fn thread_pool(&self) -> Arc<TaskThreadPool> {
        Arc::clone(&self.thread_pool)
    }
}

impl TaskManager {
    fn register_task_internal(
        &self,
        task: Box<dyn TaskLifecycle>,
        task_info_to_store: Option<StoredTaskInfo>,
    ) -> Result<()> {
        let task_name = task.get_name().to_string();

        if self.tasks.read().contains_key(&task_name) {
            return Err(anyhow!("Task uniqueness violation: '{}'", task_name));
        }

        let task_arc = Arc::new(RwLock::new(task));

        {
            let mut registry = self.tasks.write();
            registry.insert(task_name.clone(), Arc::clone(&task_arc));
        }

        let init_context = InitContext::new(
            self.state_storage_server.clone(),
            self.task_storage.clone(),
            self.thread_pool.clone(),
        );

        let mut handle = task_arc.write();
        handle
            .init_with_context(&init_context)
            .map_err(|e| anyhow!("Failed to init task '{}': {}", task_name, e))?;
        handle
            .start()
            .map_err(|e| anyhow!("Failed to start task '{}': {}", task_name, e))?;

        if let Some(ref info) = task_info_to_store {
            self.task_storage
                .create_task(info)
                .context("Failed to persist task to storage")?;
        }

        log::info!(
            target: "task_manager",
            "Task '{}' initialized and started",
            task_name
        );
        Ok(())
    }

    fn recover_tasks_from_storage(&self) -> Result<()> {
        let stored_tasks = self.task_storage.list_all_tasks()?;

        for stored in stored_tasks {
            if let Err(e) = self.recover_one_task(&stored) {
                log::error!(
                    target: "task_manager",
                    "Recovery failed for {}: {:?}",
                    stored.name,
                    e
                );
            }
        }
        Ok(())
    }

    fn recover_one_task(&self, stored: &StoredTaskInfo) -> Result<()> {
        if self.tasks.read().contains_key(&stored.name) {
            return Ok(());
        }

        let task = match &stored.module_bytes {
            None => TaskBuilder::from_yaml_config(&stored.config_bytes, &[]),
            Some(TaskModuleBytes::Wasm(bytes)) => {
                TaskBuilder::from_yaml_config(&stored.config_bytes, bytes)
            }
            Some(TaskModuleBytes::Python {
                class_name: _,
                module,
                bytes: py_bytes,
            }) => {
                #[cfg(feature = "python")]
                {
                    let modules = [(module.clone(), py_bytes.clone().unwrap_or_default())];
                    TaskBuilder::from_python_config(&stored.config_bytes, &modules)
                }
                #[cfg(not(feature = "python"))]
                {
                    let _ = (module, py_bytes);
                    return Err(anyhow!("Python task recovery skipped: feature disabled"));
                }
            }
        }
        .map_err(|e| anyhow!("Failed to rebuild task from storage: {}", e))?;

        self.register_task_internal(task, None)
    }

    fn get_task_handle(&self, name: &str) -> Result<Arc<RwLock<Box<dyn TaskLifecycle>>>> {
        self.tasks
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("Task '{}' not found in registry", name))
    }
}
