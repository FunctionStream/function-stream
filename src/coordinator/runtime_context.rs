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
use crate::runtime::taskexecutor::TaskManager;
use crate::sql::schema::column_descriptor::ColumnDescriptor;
use crate::sql::schema::connection_type::ConnectionType;
use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::table::Table as CatalogTable;
use crate::sql::schema::{StreamSchemaProvider, StreamTable};
use crate::storage::stream_catalog::CatalogManager;

/// Dependencies shared by analyze / plan / execute, analogous to installing globals in
/// [`TaskManager`], [`CatalogManager`], and [`JobManager`].
#[derive(Clone)]
pub struct CoordinatorRuntimeContext {
    pub task_manager: Arc<TaskManager>,
    pub catalog_manager: Arc<CatalogManager>,
    pub job_manager: Arc<JobManager>,
    planning_schema_override: Option<StreamSchemaProvider>,
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
            planning_schema_override: None,
        })
    }

    pub fn new(
        task_manager: Arc<TaskManager>,
        catalog_manager: Arc<CatalogManager>,
        job_manager: Arc<JobManager>,
        planning_schema_override: Option<StreamSchemaProvider>,
    ) -> Self {
        Self {
            task_manager,
            catalog_manager,
            job_manager,
            planning_schema_override,
        }
    }

    /// Schema provider for [`LogicalPlanVisitor`] / [`SqlToRel`]: override if set, else catalog snapshot.
    pub fn planning_schema_provider(&self) -> StreamSchemaProvider {
        let mut provider = self.catalog_manager.acquire_planning_context();

        for (name, stream) in provider.tables.streams.clone() {
            let StreamTable::Source {
                name: source_name,
                connector,
                schema,
                event_time_field,
                watermark_field,
                with_options,
            } = stream.as_ref()
            else {
                continue;
            };
            let mut source = SourceTable::new(
                source_name.clone(),
                connector.clone(),
                ConnectionType::Source,
            );
            source.schema_specs = schema
                .fields()
                .iter()
                .map(|f| ColumnDescriptor::new_physical((**f).clone()))
                .collect();
            source.inferred_fields = Some(schema.fields().iter().cloned().collect());
            source.temporal_config.event_column = event_time_field.clone();
            source.temporal_config.watermark_strategy_column = watermark_field.clone();
            source.catalog_with_options = with_options.clone();

            provider
                .tables
                .catalogs
                .insert(name, Arc::new(CatalogTable::ConnectorTable(source)));
        }

        provider
    }
}
