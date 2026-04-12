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
use datafusion::execution::SessionStateBuilder;
use datafusion::sql::sqlparser::ast::{
    CreateTable as SqlCreateTable, Expr as SqlExpr, ObjectType, SqlOption,
    Statement as DFStatement, TableConstraint,
};
use datafusion_common::TableReference;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{Expr, Extension, LogicalPlan, col};
use sqlparser::ast::Statement;
use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, DropFunctionPlan,
    DropStreamingTablePlan, DropTablePlan, PlanNode, ShowCatalogTablesPlan,
    ShowCreateStreamingTablePlan, ShowCreateTablePlan, ShowFunctionsPlan, ShowStreamingTablesPlan,
    StartFunctionPlan, StopFunctionPlan, StreamingTable,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, DropStreamingTableStatement,
    DropTableStatement, ShowCatalogTables, ShowCreateStreamingTable, ShowCreateTable,
    ShowFunctions, ShowStreamingTables, StartFunction, StatementVisitor, StatementVisitorContext,
    StatementVisitorResult, StopFunction, StreamingTableStatement,
};
use crate::coordinator::tool::ConnectorOptions;
use crate::sql::analysis::{StreamSchemaProvider, maybe_add_key_extension_to_sink, rewrite_sinks};
use crate::sql::common::with_option_keys as opt;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::logical_node::logical::{LogicalProgram, ProgramConfig};
use crate::sql::logical_node::sink::StreamEgressNode;
use crate::sql::logical_planner::optimizers::{ChainingOptimizer, produce_optimized_plan};
use crate::sql::logical_planner::planner::PlanToGraphVisitor;
use crate::sql::rewrite_plan;
use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::{ColumnDescriptor, ConnectionType, Table};

#[derive(Clone)]
pub struct LogicalPlanVisitor {
    schema_provider: StreamSchemaProvider,
}

impl LogicalPlanVisitor {
    pub fn new(schema_provider: StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn visit(&self, analysis: &Analysis) -> Box<dyn PlanNode> {
        let stmt = analysis.statement();
        let context = StatementVisitorContext::Empty;

        match stmt.accept(self, &context) {
            StatementVisitorResult::Plan(plan) => plan,
            _ => panic!("Fatal: LogicalPlanVisitor must yield a PlanNode variant"),
        }
    }

    pub fn build_streaming_table(
        schema_provider: &StreamSchemaProvider,
        stmt: &StreamingTableStatement,
    ) -> Result<StreamingTable> {
        Self::new(schema_provider.clone()).compile_streaming_sink(stmt)
    }

    fn compile_streaming_sink(&self, stmt: &StreamingTableStatement) -> Result<StreamingTable> {
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
        let connector_type = sink_properties
            .pull_opt_str(opt::CONNECTOR)?
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Validation Error: Streaming table '{}' requires the '{}' property",
                    sink_table_name,
                    opt::CONNECTOR
                )
            })?;

        let partition_keys = Self::extract_partitioning_keys(&mut sink_properties)?;

        let sink_description = comment
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("sink `{}` ({connector_type})", sink_table_name));

        let mut query_logical_plan = rewrite_plan(
            produce_optimized_plan(&Statement::Query(query.clone()), &self.schema_provider)?,
            &self.schema_provider,
        )?;

        if query_logical_plan
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            query_logical_plan =
                serialize_outgoing_json(&self.schema_provider, Arc::new(query_logical_plan));
        }

        let output_schema_fields = query_logical_plan
            .schema()
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::from((**f).clone()))
            .collect::<Vec<_>>();

        let mut sink_definition = SourceTable::from_options(
            &sink_table_name,
            &connector_type,
            false,
            output_schema_fields,
            vec![],
            None,
            &mut sink_properties,
            None,
            &self.schema_provider,
            Some(ConnectionType::Sink),
            sink_description,
        )?;
        sink_definition.partition_exprs = Arc::new(partition_keys);

        let output_schema = query_logical_plan.schema().clone();
        let sink_plan_node = StreamEgressNode::try_new(
            TableReference::bare(sink_table_name.clone()),
            Table::ConnectorTable(sink_definition.clone()),
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

        let streaming_with_options: Option<std::collections::HashMap<String, String>> =
            if with_options.is_empty() {
                None
            } else {
                let map: std::collections::HashMap<String, String> = with_options
                    .iter()
                    .filter_map(|opt| match opt {
                        SqlOption::KeyValue { key, value } => Some((
                            key.value.clone(),
                            value.to_string().trim_matches('\'').to_string(),
                        )),
                        _ => None,
                    })
                    .collect();
                if map.is_empty() { None } else { Some(map) }
            };

        Ok(StreamingTable {
            name: sink_table_name,
            comment: comment.clone(),
            program: validated_program,
            with_options: streaming_with_options,
        })
    }

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

        let mut graph_compiler = PlanToGraphVisitor::new(&self.schema_provider, &session_state);
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

    fn contains_connector_property(options: &[SqlOption]) -> bool {
        options.iter().any(|opt| match opt {
            SqlOption::KeyValue { key, .. } => key.value.eq_ignore_ascii_case(opt::CONNECTOR),
            _ => false,
        })
    }

    fn parse_primary_keys(constraints: &[TableConstraint]) -> Result<Vec<String>> {
        let mut keys = None;
        for constraint in constraints {
            if let TableConstraint::PrimaryKey { columns, .. } = constraint {
                if keys.is_some() {
                    return plan_err!(
                        "Constraint Violation: Multiple PRIMARY KEY constraints are forbidden"
                    );
                }
                keys = Some(columns.iter().map(|ident| ident.value.clone()).collect());
            }
        }
        Ok(keys.unwrap_or_default())
    }

    fn parse_watermark_strategy(
        constraints: &[TableConstraint],
    ) -> Result<Option<(String, Option<SqlExpr>)>> {
        let mut strategy = None;
        for constraint in constraints {
            if let TableConstraint::Watermark {
                column_name,
                watermark_expr,
            } = constraint
            {
                if strategy.is_some() {
                    return plan_err!(
                        "Constraint Violation: Only a single WATERMARK FOR clause is permitted"
                    );
                }
                strategy = Some((column_name.value.clone(), watermark_expr.clone()));
            }
        }
        Ok(strategy)
    }

    fn compile_connector_source_plan(&self, stmt: &SqlCreateTable) -> Result<CreateTablePlan> {
        if stmt.query.is_some() {
            return plan_err!(
                "Syntax Error: CREATE TABLE ... AS SELECT combined with WITH ('connector'=...) is invalid. Use CREATE STREAMING TABLE instead."
            );
        }
        if stmt.or_replace {
            return plan_err!(
                "Syntax Error: OR REPLACE is not supported for external connector tables."
            );
        }
        if stmt.temporary {
            return plan_err!(
                "Syntax Error: TEMPORARY is not supported for external connector tables."
            );
        }
        if stmt.external {
            return plan_err!(
                "Syntax Error: EXTERNAL keyword is redundant and unsupported for connector configurations."
            );
        }

        let target_name = stmt.name.to_string();
        let table_description = stmt
            .comment
            .clone()
            .map(|c| c.to_string())
            .unwrap_or_default();

        let schema_compiler = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);
        let arrow_schema = schema_compiler.build_schema(stmt.columns.clone())?;

        let schema_descriptors = arrow_schema
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::from((**f).clone()))
            .collect::<Vec<_>>();

        let mut connector_options = ConnectorOptions::new(&stmt.with_options, &None)?;
        let adapter_type = connector_options
            .pull_opt_str(opt::CONNECTOR)?
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Configuration Error: Missing required property '{}' in WITH clause",
                    opt::CONNECTOR
                )
            })?;

        let pk_constraints = Self::parse_primary_keys(&stmt.constraints)?;
        let watermark_strategy = Self::parse_watermark_strategy(&stmt.constraints)?;

        let source_definition = SourceTable::from_options(
            &target_name,
            &adapter_type,
            false,
            schema_descriptors,
            pk_constraints,
            watermark_strategy,
            &mut connector_options,
            None,
            &self.schema_provider,
            Some(ConnectionType::Source),
            table_description,
        )?;

        Ok(CreateTablePlan::connector_source(
            source_definition,
            stmt.if_not_exists,
        ))
    }
}

impl StatementVisitor for LogicalPlanVisitor {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(CreateFunctionPlan::new(
            stmt.get_function_source().clone(),
            stmt.get_config_source().cloned(),
            stmt.get_extra_properties().clone(),
        )))
    }

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StartFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StopFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_show_functions(
        &self,
        _stmt: &ShowFunctions,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowFunctionsPlan::new()))
    }

    fn visit_show_catalog_tables(
        &self,
        _stmt: &ShowCatalogTables,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCatalogTablesPlan::new()))
    }

    fn visit_show_create_table(
        &self,
        stmt: &ShowCreateTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCreateTablePlan::new(stmt.table_name.clone())))
    }

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(CreatePythonFunctionPlan::new(
            stmt.get_class_name().to_string(),
            stmt.get_modules().to_vec(),
            stmt.get_config_content().to_string(),
        )))
    }

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        if let Statement::CreateTable(ast_node) = &stmt.statement
            && ast_node.query.is_none()
            && Self::contains_connector_property(&ast_node.with_options)
        {
            let execution_plan =
                self.compile_connector_source_plan(ast_node)
                    .unwrap_or_else(|err| {
                        panic!(
                            "Fatal Compiler Error: Connector source resolution failed - {err:#}"
                        );
                    });
            return StatementVisitorResult::Plan(Box::new(execution_plan));
        }

        let schema_compiler = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);
        match schema_compiler.sql_statement_to_plan(stmt.statement.clone()) {
            Ok(logical_plan) => {
                debug!(
                    "Successfully compiled logical DDL topology:\n{}",
                    logical_plan.display_graphviz()
                );
                StatementVisitorResult::Plan(Box::new(CreateTablePlan::new(logical_plan)))
            }
            Err(err) => panic!("Fatal Compiler Error: Logical plan translation failed - {err}"),
        }
    }

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let execution_plan = self.compile_streaming_sink(stmt).unwrap_or_else(|err| {
            panic!("Fatal Compiler Error: Streaming sink compilation aborted - {err}");
        });
        StatementVisitorResult::Plan(Box::new(execution_plan))
    }

    fn visit_drop_table_statement(
        &self,
        stmt: &DropTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let DFStatement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } = &stmt.statement
        else {
            panic!("Fatal Compiler Error: AST mismatch on DropTableStatement");
        };

        if *object_type != ObjectType::Table {
            panic!("Fatal Compiler Error: Drop target must be of type TABLE");
        }
        if names.len() != 1 {
            panic!(
                "Fatal Compiler Error: Bulk drop operations are not supported. Specify exactly one table."
            );
        }

        StatementVisitorResult::Plan(Box::new(DropTablePlan::new(
            names[0].to_string(),
            *if_exists,
        )))
    }

    fn visit_show_streaming_tables(
        &self,
        _stmt: &ShowStreamingTables,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowStreamingTablesPlan::new()))
    }

    fn visit_show_create_streaming_table(
        &self,
        stmt: &ShowCreateStreamingTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCreateStreamingTablePlan::new(
            stmt.table_name.clone(),
        )))
    }

    fn visit_drop_streaming_table(
        &self,
        stmt: &DropStreamingTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropStreamingTablePlan::new(
            stmt.table_name.clone(),
            stmt.if_exists,
        )))
    }
}
