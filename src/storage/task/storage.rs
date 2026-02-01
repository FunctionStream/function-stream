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

use crate::runtime::common::ComponentState;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskModuleBytes {
    Wasm(Vec<u8>),
    Python {
        class_name: String,
        module: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes: Option<Vec<u8>>,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct StoredTaskInfo {
    pub name: String,
    pub task_type: String,
    pub module_bytes: Option<TaskModuleBytes>,
    pub config_bytes: Vec<u8>,
    pub state: ComponentState,
    pub created_at: u64,
    pub checkpoint_id: Option<u64>,
}

#[allow(dead_code)]
pub trait TaskStorage: Send + Sync {
    fn create_task(&self, task_info: &StoredTaskInfo) -> Result<()>;

    fn update_task_state(&self, task_name: &str, new_state: ComponentState) -> Result<()>;

    fn update_task_checkpoint_id(&self, task_name: &str, checkpoint_id: Option<u64>) -> Result<()>;

    fn delete_task(&self, task_name: &str) -> Result<()>;

    fn load_task(&self, task_name: &str) -> Result<StoredTaskInfo>;

    fn task_exists(&self, task_name: &str) -> Result<bool>;

    fn list_all_tasks(&self) -> Result<Vec<StoredTaskInfo>>;
}
