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

// Init Context - Initialization context
//
// Provides various resources needed for task initialization, including state storage, task storage, thread pool, etc.

use crate::runtime::processor::wasm::thread_pool::{TaskThreadPool, ThreadGroup};
use crate::storage::state_backend::StateStorageServer;
use crate::storage::task::TaskStorage;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct InitContext {
    pub state_storage_server: Arc<StateStorageServer>,
    pub task_storage: Arc<dyn TaskStorage>,
    pub thread_pool: Arc<TaskThreadPool>,
    pub thread_group_registry: Arc<Mutex<Vec<ThreadGroup>>>,
}

impl InitContext {
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

    pub fn register_thread_group(&self, thread_group: ThreadGroup) {
        let mut registry = self.thread_group_registry.lock().unwrap();
        registry.push(thread_group);
    }

    pub fn take_thread_groups(&self) -> Vec<ThreadGroup> {
        let mut registry = self.thread_group_registry.lock().unwrap();
        std::mem::take(&mut *registry)
    }
}
