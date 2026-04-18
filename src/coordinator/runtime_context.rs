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

//! Runtime resources for a single coordinator run: [`TaskManager`], [`CatalogManager`], and [`JobManager`].

use std::sync::Arc;

use anyhow::Result;

use crate::runtime::streaming::job::JobManager;
use crate::runtime::wasm::taskexecutor::TaskManager;
use crate::sql::schema::StreamSchemaProvider;
use crate::storage::stream_catalog::CatalogManager;

/// Dependencies shared by analyze / plan / execute, analogous to installing globals in
/// [`TaskManager`], [`CatalogManager`], and [`JobManager`].
#[derive(Clone)]
pub struct CoordinatorRuntimeContext {
    pub task_manager: Arc<TaskManager>,
    pub catalog_manager: Arc<CatalogManager>,
    pub job_manager: Arc<JobManager>,
}

impl CoordinatorRuntimeContext {
    pub fn try_from_globals() -> Result<Self> {
        Ok(Self {
            task_manager: TaskManager::get()
                .map_err(|e| anyhow::anyhow!("Failed to get TaskManager: {}", e))?,
            catalog_manager: CatalogManager::global()
                .map_err(|e| anyhow::anyhow!("Failed to get CatalogManager: {}", e))?,
            job_manager: JobManager::global()
                .map_err(|e| anyhow::anyhow!("Failed to get JobManager: {}", e))?,
        })
    }

    pub fn new(
        task_manager: Arc<TaskManager>,
        catalog_manager: Arc<CatalogManager>,
        job_manager: Arc<JobManager>,
    ) -> Self {
        Self {
            task_manager,
            catalog_manager,
            job_manager,
        }
    }

    /// Schema provider for [`LogicalPlanVisitor`] / [`SqlToRel`].
    pub fn planning_schema_provider(&self) -> StreamSchemaProvider {
        self.catalog_manager.acquire_planning_context()
    }
}
