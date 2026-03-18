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

use std::sync::Arc;

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::sql::sqlparser::ast::{SqlOption, Statement as DFStatement};
use datafusion_common::TableReference;
use datafusion_expr::{Expr, Extension, LogicalPlan, col};
use sqlparser::ast::Statement;
use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, DropFunctionPlan, PlanNode,
    ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan, StreamingTable,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, ShowFunctions, StartFunction,
    StatementVisitor, StatementVisitorContext, StatementVisitorResult, StopFunction,
    StreamingTableStatement,
};
use crate::coordinator::tool::ConnectorOptions;
use crate::sql::catalog::Table;
use crate::sql::catalog::connector::ConnectionType;
use crate::sql::catalog::connector_table::ConnectorTable;
use crate::sql::catalog::field_spec::FieldSpec;
use crate::sql::catalog::optimizer::produce_optimized_plan;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::planner::extension::sink::SinkExtension;
use crate::sql::planner::{StreamSchemaProvider, maybe_add_key_extension_to_sink};
use crate::sql::rewrite_plan;

const CONNECTOR: &str = "connector";
const PARTITION_BY: &str = "partition_by";
const IDLE_MICROS: &str = "idle_time";

/// 将 WITH 选项列表转为 key-value map，便于读取 connector 等配置。
fn with_options_to_map(options: &[SqlOption]) -> std::collections::HashMap<String, String> {
    options
        .iter()
        .filter_map(|opt| match opt {
            SqlOption::KeyValue { key, value } => Some((
                key.value.clone(),
                value.to_string().trim_matches('\'').to_string(),
            )),
            _ => None,
        })
        .collect()
}

pub struct LogicalPlanVisitor {
    schema_provider: StreamSchemaProvider,
}

impl LogicalPlanVisitor {
    pub fn new(schema_provider: StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn visit(&self, analysis: &Analysis) -> Box<dyn PlanNode> {
        let context = StatementVisitorContext::Empty;
        let stmt = analysis.statement();

        let result = stmt.accept(self, &context);

        match result {
            StatementVisitorResult::Plan(plan) => plan,
            _ => panic!("LogicalPlanVisitor should return Plan"),
        }
    }
    fn build_create_streaming_table_plan(
        &self,
        stmt: &StreamingTableStatement,
    ) -> Result<Box<dyn PlanNode>> {
        let statement = &stmt.statement;
        match statement {
            DFStatement::CreateStreamingTable {
                name,
                with_options,
                comment,
                query,
            } => {
                let name_str = name.to_string();

                let mut connector_opts = ConnectorOptions::new(with_options, &None)?;
                let connector_type = connector_opts.pull_opt_str(CONNECTOR)?.ok_or_else(|| {
                    plan_datafusion_err!(
                        "Streaming Table '{}' must specify '{}' option",
                        name_str,
                        CONNECTOR
                    )
                })?;

                let synthetic_statement = Statement::Query(query.clone());
                let base_plan =
                    produce_optimized_plan(&synthetic_statement, &self.schema_provider)?;

                let mut plan_rewrite = rewrite_plan(base_plan, &self.schema_provider)?;

                if plan_rewrite
                    .schema()
                    .fields()
                    .iter()
                    .any(|f| is_json_union(f.data_type()))
                {
                    plan_rewrite =
                        serialize_outgoing_json(&self.schema_provider, Arc::new(plan_rewrite));
                }

                let fields: Vec<FieldSpec> = plan_rewrite
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| FieldSpec::Struct((**f).clone()))
                    .collect();

                let partition_exprs =
                    if let Some(partition_cols) = connector_opts.pull_opt_str(PARTITION_BY)? {
                        let cols: Vec<Expr> =
                            partition_cols.split(',').map(|c| col(c.trim())).collect();
                        Some(cols)
                    } else {
                        None
                    };

                let connector_table = ConnectorTable {
                    id: None,
                    connector: connector_type,
                    name: name_str.clone(),
                    connection_type: ConnectionType::Sink,
                    fields,
                    config: "".to_string(),
                    description: comment.clone().unwrap_or_default(),
                    event_time_field: None,
                    watermark_field: None,
                    idle_time: connector_opts.pull_opt_duration(IDLE_MICROS)?,
                    primary_keys: Arc::new(vec![]),
                    inferred_fields: None,
                    partition_exprs: Arc::new(partition_exprs),
                };

                let sink_extension = SinkExtension::new(
                    TableReference::bare(name_str.clone()),
                    Table::ConnectorTable(connector_table.clone()),
                    plan_rewrite.schema().clone(),
                    Arc::new(plan_rewrite),
                )?;

                let final_plan =
                    maybe_add_key_extension_to_sink(LogicalPlan::Extension(Extension {
                        node: Arc::new(sink_extension),
                    }))?;

                Ok(Box::new(StreamingTable {
                    name: name_str,
                    comment: comment.clone(),
                    connector_table,
                    logical_plan: final_plan,
                }))
            }
            _ => plan_err!("Only CREATE STREAMING TABLE supported"),
        }
    }
}

impl StatementVisitor for LogicalPlanVisitor {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let function_source = stmt.get_function_source().clone();
        let config_source = stmt.get_config_source().cloned();
        let extra_props = stmt.get_extra_properties().clone();

        StatementVisitorResult::Plan(Box::new(CreateFunctionPlan::new(
            function_source,
            config_source,
            extra_props,
        )))
    }

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StartFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StopFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_show_functions(
        &self,
        _stmt: &ShowFunctions,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowFunctionsPlan::new()))
    }

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let class_name = stmt.get_class_name().to_string();
        let modules = stmt.get_modules().to_vec();
        let config_content = stmt.get_config_content().to_string();

        StatementVisitorResult::Plan(Box::new(CreatePythonFunctionPlan::new(
            class_name,
            modules,
            config_content,
        )))
    }

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let sql_to_rel = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);

        match sql_to_rel.sql_statement_to_plan(stmt.statement.clone()) {
            Ok(plan) => {
                debug!("Create table plan:\n{}", plan.display_graphviz());
                StatementVisitorResult::Plan(Box::new(CreateTablePlan::new(plan)))
            }
            Err(e) => {
                panic!("Failed to convert CREATE TABLE to logical plan: {e}");
            }
        }
    }

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        match self.build_create_streaming_table_plan(stmt) {
            Ok(plan) => StatementVisitorResult::Plan(plan),
            Err(e) => panic!("Failed to build CreateStreamingTable plan: {e}"),
        }
    }
}
