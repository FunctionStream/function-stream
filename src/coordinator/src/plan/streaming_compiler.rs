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

//! `CREATE STREAMING TABLE ... AS SELECT` → [`SinkTable`], egress, [`LogicalProgram`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::sql::sqlparser::ast::{SqlOption, Statement as DFStatement};
use datafusion_common::TableReference;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{Expr, Extension, LogicalPlan, col};
use sqlparser::ast::Statement;
use tracing::debug;

use super::StreamingTable;
use crate::coordinator::statement::StreamingTableStatement;
use crate::coordinator::tool::ConnectorOptions;
use crate::sql::analysis::{StreamSchemaProvider, maybe_add_key_extension_to_sink, rewrite_sinks};
use crate::sql::common::constants::connector_type;
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{Format, JsonCompression, JsonFormat};
use crate::sql::connector::registry::REGISTRY;
use crate::sql::connector::sink::runtime_config::SinkRuntimeConfig;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::logical_node::logical::{LogicalProgram, ProgramConfig};
use crate::sql::logical_node::sink::StreamEgressNode;
use crate::sql::logical_planner::optimizers::{ChainingOptimizer, produce_optimized_plan};
use crate::sql::logical_planner::planner::PlanToGraphVisitor;
use crate::sql::rewrite_plan;
use crate::sql::schema::ColumnDescriptor;
use crate::sql::schema::catalog::{ExternalTable, SinkTable};
use crate::sql::schema::table::CatalogEntity;
use crate::sql::schema::table_role::validate_adapter_availability;

pub struct StreamingCompiler<'a> {
    schema_provider: &'a StreamSchemaProvider,
}

impl<'a> StreamingCompiler<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn compile(&self, stmt: &StreamingTableStatement) -> Result<StreamingTable> {
        let DFStatement::CreateStreamingTable {
            name,
            with_options,
            comment,
            query,
        } = &stmt.statement
        else {
            return plan_err!("Statement mismatch: Expected CREATE STREAMING TABLE AST node");
        };

        let sink_table_name = name.to_string();
        debug!(
            "Initiating streaming sink compilation for identifier: {}",
            sink_table_name
        );

        let mut sink_properties = ConnectorOptions::new(with_options, &None)?;
        let adapter_type = sink_properties
            .pull_opt_str(opt::CONNECTOR)?
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Validation Error: Streaming table '{}' requires the '{}' property",
                    sink_table_name,
                    opt::CONNECTOR
                )
            })?;
        validate_adapter_availability(&adapter_type)?;

        let partition_keys = Self::extract_partitioning_keys(&mut sink_properties)?;
        let catalog_with_options = sink_properties.snapshot_for_catalog();

        let connection_format = Format::from_opts(&mut sink_properties)?;
        Self::assert_format_compatibility(&connection_format, &adapter_type)?;

        let sink_description = comment
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("sink `{}` ({adapter_type})", sink_table_name));

        let mut query_logical_plan = rewrite_plan(
            produce_optimized_plan(&Statement::Query(query.clone()), self.schema_provider)?,
            self.schema_provider,
        )?;

        if query_logical_plan
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            query_logical_plan =
                serialize_outgoing_json(self.schema_provider, Arc::new(query_logical_plan));
        }

        let output_schema_fields: Vec<ColumnDescriptor> = query_logical_plan
            .schema()
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::from((**f).clone()))
            .collect();

        let runtime_props =
            SinkRuntimeConfig::extract_from_options(&mut sink_properties)?.to_runtime_properties();
        let provider = REGISTRY.get_sink(&adapter_type)?;
        let connector_config =
            provider.build_sink_config(&mut sink_properties, &connection_format, &runtime_props)?;

        if !sink_properties.is_empty() {
            let unknown_keys: Vec<String> = sink_properties.keys().cloned().collect();
            return plan_err!(
                "Unknown options for streaming sink connector '{adapter_type}': {unknown_keys:?}"
            );
        }

        let sink_table = SinkTable {
            table_identifier: sink_table_name.clone(),
            adapter_type,
            schema_specs: output_schema_fields,
            connector_config,
            partition_exprs: Arc::new(partition_keys),
            key_constraints: Vec::new(),
            connection_format,
            description: sink_description,
            catalog_with_options,
        };

        let output_schema = query_logical_plan.schema().clone();
        let sink_plan_node = StreamEgressNode::try_new(
            TableReference::bare(sink_table_name.clone()),
            CatalogEntity::external(ExternalTable::Sink(sink_table)),
            output_schema,
            query_logical_plan,
        )?;

        let mut rewritten_plans = rewrite_sinks(vec![maybe_add_key_extension_to_sink(
            LogicalPlan::Extension(Extension {
                node: Arc::new(sink_plan_node),
            }),
        )?])?;

        let final_logical_plan = rewritten_plans.remove(0);
        let validated_program = self.validate_graph_topology(&final_logical_plan)?;

        Ok(StreamingTable {
            name: sink_table_name,
            comment: comment.clone(),
            program: validated_program,
            with_options: Self::echo_with_options(with_options),
        })
    }

    /// Compile the final logical plan into the executable streaming program,
    /// disabling DataFusion's batch-oriented repartition heuristics.
    fn validate_graph_topology(&self, logical_plan: &LogicalPlan) -> Result<LogicalProgram> {
        let mut session_config = SessionConfig::new();
        let opts = session_config.options_mut();
        opts.optimizer.enable_round_robin_repartition = false;
        opts.optimizer.repartition_aggregations = false;
        opts.optimizer.repartition_windows = false;
        opts.optimizer.repartition_sorts = false;
        opts.optimizer.repartition_joins = false;
        opts.execution.target_partitions = 1;

        let session_state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build();

        let mut graph_compiler = PlanToGraphVisitor::new(self.schema_provider, &session_state);
        graph_compiler.add_plan(logical_plan.clone())?;

        let mut executable_program =
            LogicalProgram::new(graph_compiler.into_graph(), ProgramConfig::default());
        executable_program.optimize(&ChainingOptimizer {});

        Ok(executable_program)
    }

    fn extract_partitioning_keys(options: &mut ConnectorOptions) -> Result<Option<Vec<Expr>>> {
        options
            .pull_opt_str(opt::PARTITION_BY)?
            .map(|raw_cols| raw_cols.split(',').map(|c| col(c.trim())).collect())
            .map(Ok)
            .transpose()
    }

    fn assert_format_compatibility(format: &Option<Format>, adapter_type: &str) -> Result<()> {
        if let Some(Format::Json(JsonFormat { compression, .. })) = format
            && !matches!(compression, JsonCompression::Uncompressed)
            && adapter_type != connector_type::FILESYSTEM
        {
            return plan_err!("'json.compression' is only supported for the filesystem connector");
        }
        Ok(())
    }

    /// Snapshot the original WITH options so the streaming-table catalog
    /// can later reproduce the exact DDL for `SHOW CREATE STREAMING TABLE`.
    fn echo_with_options(with_options: &[SqlOption]) -> Option<HashMap<String, String>> {
        if with_options.is_empty() {
            return None;
        }
        let map: HashMap<String, String> = with_options
            .iter()
            .filter_map(|o| match o {
                SqlOption::KeyValue { key, value } => Some((
                    key.value.clone(),
                    value.to_string().trim_matches('\'').to_string(),
                )),
                _ => None,
            })
            .collect();
        if map.is_empty() { None } else { Some(map) }
    }
}
