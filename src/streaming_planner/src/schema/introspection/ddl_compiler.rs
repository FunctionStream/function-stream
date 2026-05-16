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

//! Compiles `CREATE TABLE ... WITH (...)` into Source or Lookup tables.

use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, Result, plan_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::sql::TableReference;
use datafusion::sql::planner::{PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::CreateTable as SqlCreateTable;
use datafusion::sql::sqlparser::ast::Statement as DfStatement;
use datafusion_expr::ExprSchemable;
use tracing::warn;

use crate::common::ast_utils::AstUtils;
use crate::common::constants::{connection_table_role, connector_type, sql_field};
use crate::common::with_option_keys as opt;
use crate::common::{BadData, ConnectorOptions, Format, Framing, JsonCompression, JsonFormat};
use crate::connector::registry::REGISTRY;
use crate::schema::ColumnDescriptor;
use crate::schema::catalog::{ExternalTable, LookupTable, SourceTable};
use crate::schema::data_encoding_format::DataEncodingFormat;
use crate::schema::schema_provider::StreamSchemaProvider;
use crate::schema::table_role::{apply_adapter_specific_rules, validate_adapter_availability};
use crate::schema::temporal_pipeline_config::TemporalPipelineConfig;

pub struct DdlCompiler<'a> {
    schema_provider: &'a StreamSchemaProvider,
}

impl<'a> DdlCompiler<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn compile(
        &self,
        stmt: &SqlCreateTable,
        declared_role: Option<&str>,
    ) -> Result<ExternalTable> {
        Self::assert_ddl_flags(stmt)?;

        match declared_role {
            Some(connection_table_role::LOOKUP) => self.compile_lookup(stmt),
            Some(connection_table_role::SOURCE) | None => self.compile_source(stmt),
            Some(connection_table_role::SINK) => plan_err!(
                "`CREATE TABLE ... WITH (type='sink')` is not supported; use `CREATE STREAMING TABLE ... AS SELECT`"
            ),
            Some(other) => {
                plan_err!("Invalid connection type '{other}' — expected 'source' or 'lookup'")
            }
        }
    }

    fn compile_source(&self, stmt: &SqlCreateTable) -> Result<ExternalTable> {
        let target_name = stmt.name.to_string();
        let description = stmt
            .comment
            .clone()
            .map(|c| c.to_string())
            .unwrap_or_default();

        let mut columns = self.extract_columns(stmt)?;

        let mut options = ConnectorOptions::new(&stmt.with_options, &None)?;
        let adapter_type = Self::extract_adapter(&mut options)?;
        Self::assert_connector_match(&mut options, &adapter_type)?;
        Self::absorb_type_option(&mut options, connection_table_role::SOURCE)?;

        validate_adapter_availability(&adapter_type)?;

        let pk_constraints = AstUtils::parse_primary_keys(&stmt.constraints)?;
        let catalog_with_options = options.snapshot_for_catalog();

        let format = Format::from_opts(&mut options)?;
        Self::assert_format_compatibility(&format, &adapter_type)?;
        let _framing = Framing::from_opts(&mut options)?;
        let bad_data = BadData::from_opts(&mut options)?;

        let encoding = DataEncodingFormat::from_format(format.as_ref());

        columns = apply_adapter_specific_rules(&adapter_type, columns);
        columns = encoding.apply_envelope(columns)?;

        if encoding.supports_delta_updates() && pk_constraints.is_empty() {
            return plan_err!("CDC source requires at least one PRIMARY KEY field");
        }

        let watermark = AstUtils::parse_watermark_strategy(&stmt.constraints)?;
        let mut temporal_config = resolve_source_watermark(
            &target_name,
            &mut columns,
            watermark,
            &mut options,
            self.schema_provider,
        )?;

        let idle_from_micros = options
            .pull_opt_i64(opt::IDLE_MICROS)?
            .filter(|t| *t > 0)
            .map(|t| Duration::from_micros(t as u64));
        let idle_from_duration = options.pull_opt_duration(opt::IDLE_TIME)?;
        temporal_config.liveness_timeout = idle_from_micros.or(idle_from_duration);

        let provider = REGISTRY.get_source(&adapter_type)?;
        let connector_config = provider.build_source_config(&mut options, &format, bad_data)?;

        Self::assert_options_fully_consumed(
            &options,
            connection_table_role::SOURCE,
            &adapter_type,
        )?;

        Ok(ExternalTable::Source(SourceTable {
            table_identifier: target_name,
            adapter_type,
            schema_specs: columns,
            connector_config,
            temporal_config,
            key_constraints: pk_constraints,
            payload_format: Some(encoding),
            connection_format: format,
            description,
            catalog_with_options,
            registry_id: None,
            inferred_fields: None,
        }))
    }

    fn compile_lookup(&self, stmt: &SqlCreateTable) -> Result<ExternalTable> {
        let target_name = stmt.name.to_string();

        if AstUtils::parse_watermark_strategy(&stmt.constraints)?.is_some() {
            return plan_err!(
                "Syntax Error: WATERMARK FOR cannot be defined on a Lookup table (`{}`)",
                target_name
            );
        }

        let description = stmt
            .comment
            .clone()
            .map(|c| c.to_string())
            .unwrap_or_default();

        let columns = self.extract_columns(stmt)?;

        let mut options = ConnectorOptions::new(&stmt.with_options, &None)?;
        let adapter_type = Self::extract_adapter(&mut options)?;
        Self::assert_connector_match(&mut options, &adapter_type)?;
        Self::absorb_type_option(&mut options, connection_table_role::LOOKUP)?;

        validate_adapter_availability(&adapter_type)?;

        let pk_constraints = AstUtils::parse_primary_keys(&stmt.constraints)?;
        let catalog_with_options = options.snapshot_for_catalog();

        let connection_format = Format::from_opts(&mut options)?;
        Self::assert_format_compatibility(&connection_format, &adapter_type)?;
        let bad_data = BadData::from_opts(&mut options)?;

        let lookup_cache_max_bytes = options.pull_opt_u64(opt::LOOKUP_CACHE_MAX_BYTES)?;
        let lookup_cache_ttl = options.pull_opt_duration(opt::LOOKUP_CACHE_TTL)?;

        let provider = REGISTRY.get_source(&adapter_type)?;
        let connector_config =
            provider.build_source_config(&mut options, &connection_format, bad_data)?;

        Self::assert_options_fully_consumed(
            &options,
            connection_table_role::LOOKUP,
            &adapter_type,
        )?;

        Ok(ExternalTable::Lookup(LookupTable {
            table_identifier: target_name,
            adapter_type,
            schema_specs: columns,
            connector_config,
            key_constraints: pk_constraints,
            lookup_cache_max_bytes,
            lookup_cache_ttl,
            connection_format,
            description,
            catalog_with_options,
            registry_id: None,
            inferred_fields: None,
        }))
    }

    fn extract_adapter(options: &mut ConnectorOptions) -> Result<String> {
        options.pull_opt_str(opt::CONNECTOR)?.ok_or_else(|| {
            plan_datafusion_err!(
                "Configuration Error: Missing required property '{}' in WITH clause",
                opt::CONNECTOR
            )
        })
    }

    fn extract_columns(&self, stmt: &SqlCreateTable) -> Result<Vec<ColumnDescriptor>> {
        let schema_compiler = datafusion::sql::planner::SqlToRel::new(self.schema_provider);
        let arrow_schema = schema_compiler.build_schema(stmt.columns.clone())?;
        Ok(arrow_schema
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::from((**f).clone()))
            .collect())
    }

    fn assert_ddl_flags(stmt: &SqlCreateTable) -> Result<()> {
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
        Ok(())
    }

    fn assert_connector_match(options: &mut ConnectorOptions, connector_name: &str) -> Result<()> {
        if let Some(c) = options.pull_opt_str(opt::CONNECTOR)?
            && c != connector_name
        {
            return plan_err!(
                "WITH option `connector` is '{c}' but table uses connector '{connector_name}'"
            );
        }
        Ok(())
    }

    fn absorb_type_option(options: &mut ConnectorOptions, expected_role: &str) -> Result<()> {
        let Some(raw) = options.pull_opt_str(opt::TYPE)? else {
            return Ok(());
        };
        if !raw.eq_ignore_ascii_case(expected_role) {
            return plan_err!(
                "Role mismatch: WITH option 'type' = '{raw}' is incompatible with the compiled role '{expected_role}'"
            );
        }
        Ok(())
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

    fn assert_options_fully_consumed(
        options: &ConnectorOptions,
        role: &str,
        adapter_type: &str,
    ) -> Result<()> {
        if !options.is_empty() {
            let unknown_keys: Vec<String> = options.keys().cloned().collect();
            return plan_err!(
                "Unknown options for {role} connector '{adapter_type}': {unknown_keys:?}"
            );
        }
        Ok(())
    }
}

/// If `stmt` is `CREATE TABLE ... WITH (connector = ...)`, compile it to [`ExternalTable`].
pub fn try_compile_connector_create_table(
    schema_provider: &StreamSchemaProvider,
    stmt: &DfStatement,
) -> Option<Result<(ExternalTable, bool)>> {
    let DfStatement::CreateTable(ast_node) = stmt else {
        return None;
    };
    if ast_node.query.is_some() {
        return None;
    }
    if !AstUtils::contains_connector_property(&ast_node.with_options) {
        return None;
    }
    let declared_role = AstUtils::peek_table_role(&ast_node.with_options);
    let if_not_exists = ast_node.if_not_exists;
    let compiler = DdlCompiler::new(schema_provider);
    Some(
        compiler
            .compile(ast_node, declared_role.as_deref())
            .map(|table| (table, if_not_exists)),
    )
}

fn resolve_source_watermark(
    table_identifier: &str,
    columns: &mut Vec<ColumnDescriptor>,
    watermark: Option<(String, Option<ast::Expr>)>,
    options: &mut ConnectorOptions,
    schema_provider: &StreamSchemaProvider,
) -> Result<TemporalPipelineConfig> {
    let mut config = TemporalPipelineConfig::default();

    if let Some(event_time_field) = options.pull_opt_field(opt::EVENT_TIME_FIELD)? {
        warn!("`event_time_field` WITH option is deprecated; use WATERMARK FOR syntax");
        config.event_column = Some(event_time_field);
    }
    if let Some(watermark_field) = options.pull_opt_field(opt::WATERMARK_FIELD)? {
        warn!("`watermark_field` WITH option is deprecated; use WATERMARK FOR syntax");
        config.watermark_strategy_column = Some(watermark_field);
    }

    let Some((time_field, watermark_expr)) = watermark else {
        return Ok(config);
    };

    let declared_field = columns
        .iter()
        .find(|c| c.arrow_field().name().as_str() == time_field.as_str())
        .ok_or_else(|| {
            plan_datafusion_err!(
                "WATERMARK FOR field `{}` does not exist in table",
                time_field
            )
        })?;

    if !matches!(
        declared_field.arrow_field().data_type(),
        DataType::Timestamp(_, None)
    ) {
        return plan_err!(
            "WATERMARK FOR field `{time_field}` has type {}, but expected TIMESTAMP",
            declared_field.arrow_field().data_type()
        );
    }

    for col in columns.iter_mut() {
        if col.arrow_field().name().as_str() == time_field.as_str() {
            col.set_nullable(false);
            break;
        }
    }

    config.event_column = Some(time_field.clone());

    match watermark_expr {
        Some(expr) => {
            let table_ref = TableReference::bare(table_identifier.to_string());
            let physical_schema = Schema::new(
                columns
                    .iter()
                    .filter(|c| !c.is_computed())
                    .map(|c| c.arrow_field().clone())
                    .collect::<Vec<_>>(),
            );
            let df_schema = DFSchema::try_from_qualified_schema(table_ref, &physical_schema)?;

            let logical_expr =
                plan_generating_expr(&expr, &df_schema, schema_provider).map_err(|e| {
                    DataFusionError::Plan(format!("could not plan watermark expression: {e}"))
                })?;

            let (data_type, _nullable) = logical_expr.data_type_and_nullable(&df_schema)?;
            if !matches!(data_type, DataType::Timestamp(_, _)) {
                return plan_err!(
                    "the type of the WATERMARK FOR expression must be TIMESTAMP, but was {data_type}"
                );
            }

            columns.push(ColumnDescriptor::new_computed(
                Field::new(
                    sql_field::COMPUTED_WATERMARK,
                    logical_expr.get_type(&df_schema)?,
                    false,
                ),
                logical_expr,
            ));
            config.watermark_strategy_column = Some(sql_field::COMPUTED_WATERMARK.to_string());
        }
        None => {
            config.watermark_strategy_column = Some(time_field);
        }
    }

    Ok(config)
}

fn plan_generating_expr(
    ast_expr: &ast::Expr,
    df_schema: &DFSchema,
    schema_provider: &StreamSchemaProvider,
) -> Result<Expr> {
    let planner = SqlToRel::new(schema_provider);
    let mut ctx = PlannerContext::new();
    planner.sql_to_expr(ast_expr.clone(), df_schema, &mut ctx)
}
