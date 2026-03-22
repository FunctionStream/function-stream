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
use std::time::Instant;

use anyhow::{Context, Result};

use crate::coordinator::analyze::Analyzer;
use crate::coordinator::dataset::ExecuteResult;
use crate::coordinator::execution::Executor;
use crate::coordinator::plan::{LogicalPlanVisitor, LogicalPlanner, PlanNode};
use crate::coordinator::statement::Statement;
use crate::sql::schema::StreamSchemaProvider;

use super::execution_context::ExecutionContext;
use super::runtime_context::CoordinatorRuntimeContext;

#[derive(Default)]
pub struct Coordinator {}

impl Coordinator {
    pub fn new() -> Self {
        Self {}
    }

    // ========================================================================
    // Plan compilation
    // ========================================================================

    pub fn compile_plan(
        &self,
        stmt: &dyn Statement,
        schema_provider: StreamSchemaProvider,
    ) -> Result<Box<dyn PlanNode>> {
        self.compile_plan_internal(&ExecutionContext::new(), stmt, schema_provider)
    }

    /// Internal pipeline: Analyze → build logical plan → optimize.
    fn compile_plan_internal(
        &self,
        context: &ExecutionContext,
        stmt: &dyn Statement,
        schema_provider: StreamSchemaProvider,
    ) -> Result<Box<dyn PlanNode>> {
        let exec_id = context.execution_id;
        let start = Instant::now();

        let analysis = Analyzer::new(context)
            .analyze(stmt)
            .map_err(|e| anyhow::anyhow!(e))
            .context("Analyzer phase failed")?;
        log::debug!(
            "[{}] Analyze phase finished in {}ms",
            exec_id,
            start.elapsed().as_millis()
        );

        let plan = LogicalPlanVisitor::new(schema_provider).visit(&analysis);

        let opt_start = Instant::now();
        let optimized = LogicalPlanner::new().optimize(plan, &analysis);
        log::debug!(
            "[{}] Optimizer phase finished in {}ms",
            exec_id,
            opt_start.elapsed().as_millis()
        );

        Ok(optimized)
    }

    // ========================================================================
    // Execution
    // ========================================================================

    pub fn execute(&self, stmt: &dyn Statement) -> ExecuteResult {
        match CoordinatorRuntimeContext::try_from_globals() {
            Ok(ctx) => self.execute_with_runtime_context(stmt, &ctx),
            Err(e) => ExecuteResult::err(e.to_string()),
        }
    }

    pub async fn execute_with_stream_catalog(&self, stmt: &dyn Statement) -> ExecuteResult {
        self.execute(stmt)
    }

    /// Same as [`Self::execute`], but uses an explicit [`CoordinatorRuntimeContext`] (e.g. tests or custom wiring).
    pub fn execute_with_runtime_context(
        &self,
        stmt: &dyn Statement,
        runtime: &CoordinatorRuntimeContext,
    ) -> ExecuteResult {
        let start = Instant::now();
        let context = ExecutionContext::new();
        let exec_id = context.execution_id;
        let schema_provider = runtime.planning_schema_provider();

        let result = (|| -> Result<ExecuteResult> {
            let plan = self.compile_plan_internal(&context, stmt, schema_provider)?;

            let exec_start = Instant::now();
            let res = Executor::new(
                Arc::clone(&runtime.task_manager),
                runtime.catalog_manager.clone(),
            )
            .execute(plan.as_ref())
            .map_err(|e| anyhow::anyhow!(e))
            .context("Executor phase failed")?;

            log::debug!(
                "[{}] Executor phase finished in {}ms",
                exec_id,
                exec_start.elapsed().as_millis()
            );
            Ok(res)
        })();

        match result {
            Ok(res) => {
                log::debug!(
                    "[{}] Execution completed in {}ms",
                    exec_id,
                    start.elapsed().as_millis()
                );
                res
            }
            Err(e) => {
                log::error!(
                    "[{}] Execution failed after {}ms. Error: {:#}",
                    exec_id,
                    start.elapsed().as_millis(),
                    e
                );
                ExecuteResult::err(format!("Execution failed: {:#}", e))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test-only helpers (used by `create_streaming_table_coordinator_tests` below)
// ---------------------------------------------------------------------------

#[cfg(test)]
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[cfg(test)]
use crate::sql::common::TIMESTAMP_FIELD;
#[cfg(test)]
use crate::sql::parse::parse_sql;

#[cfg(test)]
fn fake_stream_schema_provider() -> StreamSchemaProvider {
    let mut provider = StreamSchemaProvider::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    provider.add_source_table(
        "src".to_string(),
        schema,
        Some(TIMESTAMP_FIELD.to_string()),
        None,
    );
    provider
}

#[cfg(test)]
fn fake_stream_schema_provider_with_v() -> StreamSchemaProvider {
    let mut provider = StreamSchemaProvider::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    provider.add_source_table(
        "src".to_string(),
        schema,
        Some(TIMESTAMP_FIELD.to_string()),
        None,
    );
    provider
}

#[cfg(test)]
fn fake_src_dim_provider() -> StreamSchemaProvider {
    let mut provider = fake_stream_schema_provider_with_v();
    let dim = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amt", DataType::Float64, true),
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    provider.add_source_table(
        "dim".to_string(),
        dim,
        Some(TIMESTAMP_FIELD.to_string()),
        None,
    );
    provider
}

#[cfg(test)]
fn assert_coordinator_streaming_build_ok(
    sql: &str,
    provider: StreamSchemaProvider,
    expect_sink_substring: &str,
    expect_connector_substring: &str,
) {
    let stmts = parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    assert_eq!(stmts.len(), 1);
    let plan = Coordinator::new()
        .compile_plan(stmts[0].as_ref(), provider)
        .unwrap_or_else(|e| panic!("compile_plan {sql:?}: {e:#}"));
    let rendered = format!("{plan:?}");
    assert!(rendered.contains("StreamingTable"), "{rendered}");
    assert!(
        rendered.contains(expect_sink_substring),
        "expected sink name fragment {expect_sink_substring:?} in:\n{rendered}"
    );
    assert!(
        rendered.contains(expect_connector_substring),
        "expected connector fragment {expect_connector_substring:?} in:\n{rendered}"
    );
}

#[cfg(test)]
mod create_streaming_table_coordinator_tests {
    use super::{
        assert_coordinator_streaming_build_ok, fake_src_dim_provider,
        fake_stream_schema_provider, fake_stream_schema_provider_with_v,
    };
    use crate::sql::common::TIMESTAMP_FIELD;

    #[test]
    fn coordinator_build_create_streaming_table_select_star_kafka() {
        assert_coordinator_streaming_build_ok(
            concat!(
                "CREATE STREAMING TABLE my_sink ",
                "WITH ('connector' = 'kafka') ",
                "AS SELECT * FROM src",
            ),
            fake_stream_schema_provider(),
            "my_sink",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_memory_connector() {
        assert_coordinator_streaming_build_ok(
            "CREATE STREAMING TABLE mem_out WITH ('connector'='memory') AS SELECT * FROM src",
            fake_stream_schema_provider(),
            "mem_out",
            "memory",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_postgres_connector() {
        assert_coordinator_streaming_build_ok(
            "CREATE STREAMING TABLE pg_out WITH ('connector'='postgres') AS SELECT id FROM src",
            fake_stream_schema_provider(),
            "pg_out",
            "postgres",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_partition_by_and_idle_time() {
        assert_coordinator_streaming_build_ok(
            concat!(
                "CREATE STREAMING TABLE part_idle ",
                "WITH ('connector'='kafka', 'partition_by'='id', 'idle_time'='30 seconds') ",
                "AS SELECT * FROM src",
            ),
            fake_stream_schema_provider(),
            "part_idle",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_project_timestamp_columns() {
        let sql = format!(
            "CREATE STREAMING TABLE ts_cols WITH ('connector'='kafka') AS SELECT id, {ts} FROM src",
            ts = TIMESTAMP_FIELD
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider(),
            "ts_cols",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_where_filters() {
        let p = fake_stream_schema_provider_with_v();
        for (label, body) in [
            ("eq", "SELECT * FROM src WHERE id = 1"),
            ("range", "SELECT * FROM src WHERE id > 0 AND id < 100"),
            ("in_list", "SELECT * FROM src WHERE id IN (1, 2, 3)"),
            ("between", "SELECT * FROM src WHERE id BETWEEN 1 AND 10"),
            ("like", "SELECT * FROM src WHERE v LIKE 'a%'"),
            ("null", "SELECT * FROM src WHERE v IS NULL"),
        ] {
            let sql = format!(
                "CREATE STREAMING TABLE sink_w_{label} WITH ('connector'='kafka') AS {body}"
            );
            assert_coordinator_streaming_build_ok(
                &sql,
                p.clone(),
                &format!("sink_w_{label}"),
                "kafka",
            );
        }
    }

    #[test]
    fn coordinator_build_create_streaming_table_case_coalesce_cast() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_expr WITH ('connector'='kafka') AS \
             SELECT CASE WHEN id < 0 THEN 0 ELSE id END AS c, COALESCE(v, 'x') AS v2, \
             CAST(id AS DOUBLE) AS id_f, {ts} FROM src"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider_with_v(),
            "sink_expr",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_row_time_projection() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_rt WITH ('connector'='kafka') AS \
             SELECT row_time(), id, {ts} FROM src"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider(),
            "sink_rt",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_scalar_funcs_projection() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_scalar WITH ('connector'='kafka') AS \
             SELECT ABS(id), UPPER(v), LOWER(v), BTRIM(v), CHARACTER_LENGTH(v), {ts} FROM src"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider_with_v(),
            "sink_scalar",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_cte() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_cte WITH ('connector'='kafka') AS \
             WITH t AS (SELECT id, {ts} FROM src WHERE id > 0) SELECT * FROM t"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider(),
            "sink_cte",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_cte_chain() {
        let sql = "CREATE STREAMING TABLE sink_cte2 WITH ('connector'='kafka') AS \
             WITH a AS (SELECT id FROM src), b AS (SELECT id FROM a WHERE id > 1) SELECT * FROM b";
        assert_coordinator_streaming_build_ok(
            sql,
            fake_stream_schema_provider(),
            "sink_cte2",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_sink_name_with_digits() {
        assert_coordinator_streaming_build_ok(
            "CREATE STREAMING TABLE out_sink_01 WITH ('connector'='kafka') AS SELECT * FROM src",
            fake_stream_schema_provider(),
            "out_sink_01",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_subquery_in_from() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_sq WITH ('connector'='kafka') AS \
             SELECT * FROM (SELECT id, {ts} FROM src WHERE id >= 0) AS x"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider(),
            "sink_sq",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_nested_subqueries() {
        let sql = "CREATE STREAMING TABLE sink_nest WITH ('connector'='kafka') AS \
             SELECT * FROM (SELECT * FROM (SELECT id FROM src) AS i2) AS i1";
        assert_coordinator_streaming_build_ok(
            sql,
            fake_stream_schema_provider(),
            "sink_nest",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_union_all() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_union WITH ('connector'='kafka') AS \
             SELECT id, v, {ts} FROM src \
             UNION ALL \
             SELECT id, name AS v, {ts} FROM dim"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_src_dim_provider(),
            "sink_union",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_nullif_regexp() {
        let ts = TIMESTAMP_FIELD;
        let sql = format!(
            "CREATE STREAMING TABLE sink_re WITH ('connector'='kafka') AS \
             SELECT id, NULLIF(v, ''), REGEXP_LIKE(v, '^x'), {ts} FROM src"
        );
        assert_coordinator_streaming_build_ok(
            &sql,
            fake_stream_schema_provider_with_v(),
            "sink_re",
            "kafka",
        );
    }

    #[test]
    fn coordinator_build_create_streaming_table_not_and_or_where() {
        let p = fake_stream_schema_provider_with_v();
        assert_coordinator_streaming_build_ok(
            "CREATE STREAMING TABLE sink_bool WITH ('connector'='kafka') AS \
             SELECT * FROM src WHERE NOT (id = 0) AND (v IS NOT NULL OR id > 0)",
            p,
            "sink_bool",
            "kafka",
        );
    }

    #[test]
    fn coordinator_sql_create_streaming_table_compiles_full_pipeline() {
        assert_coordinator_streaming_build_ok(
            concat!(
                "CREATE STREAMING TABLE my_sink ",
                "WITH ('connector' = 'kafka') ",
                "AS SELECT * FROM src",
            ),
            fake_stream_schema_provider(),
            "my_sink",
            "kafka",
        );
    }
}
