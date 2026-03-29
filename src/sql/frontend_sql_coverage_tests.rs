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

//! SQL parse and streaming-related tests.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::sql::sqlparser::ast::Statement as DFStatement;
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::coordinator::Coordinator;
use crate::sql::common::TIMESTAMP_FIELD;
use crate::sql::parse::parse_sql;
use crate::sql::rewrite_plan;
use crate::sql::logical_planner::optimizers::produce_optimized_plan;
use crate::sql::schema::StreamSchemaProvider;

fn assert_parses_as(sql: &str, type_prefix: &str) {
    let stmts = parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for {sql:?}: {e}"));
    assert!(!stmts.is_empty(), "{sql}");
    let dbg = format!("{:?}", stmts[0]);
    assert!(
        dbg.starts_with(type_prefix),
        "sql={sql:?} expected prefix {type_prefix}, got {dbg}"
    );
}

fn assert_parse_fails(sql: &str) {
    assert!(
        parse_sql(sql).is_err(),
        "expected parse/classify failure for {sql:?}"
    );
}

fn fake_src_stream_provider() -> StreamSchemaProvider {
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

fn compile_first(coordinator: &Coordinator, sql: &str, provider: StreamSchemaProvider) {
    let stmts = parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    coordinator
        .compile_plan(stmts[0].as_ref(), provider)
        .unwrap_or_else(|e| panic!("compile_plan {sql:?}: {e:#}"));
}

fn compile_first_streaming(sql: &str) {
    compile_first(
        &Coordinator::new(),
        sql,
        fake_src_stream_provider(),
    );
}

fn fake_src_dim_stream_provider() -> StreamSchemaProvider {
    let mut provider = fake_src_stream_provider();
    let dim_schema = Arc::new(Schema::new(vec![
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
        dim_schema,
        Some(TIMESTAMP_FIELD.to_string()),
        None,
    );
    provider
}

fn compile_streaming_select_body(body: &str, provider: StreamSchemaProvider) {
    let sql = format!(
        "CREATE STREAMING TABLE sink_shape_cov WITH ('connector'='kafka') AS {body}"
    );
    compile_first(&Coordinator::new(), &sql, provider);
}

fn assert_streaming_select_logical_rewrites(body: &str, provider: &StreamSchemaProvider) {
    let sql = format!(
        "CREATE STREAMING TABLE sink_lr WITH ('connector'='kafka') AS {body}"
    );
    let dialect = FunctionStreamDialect {};
    let stmts = Parser::parse_sql(&dialect, &sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    let DFStatement::CreateStreamingTable { query, .. } = &stmts[0] else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    let plan = produce_optimized_plan(&DFStatement::Query(query.clone()), provider)
        .unwrap_or_else(|e| panic!("produce_optimized_plan {sql:?}: {e:#}"));
    rewrite_plan(plan, provider).unwrap_or_else(|e| panic!("rewrite_plan {sql:?}: {e:#}"));
}

fn assert_streaming_select_logical_rewrite_err_contains(
    body: &str,
    provider: &StreamSchemaProvider,
    needle: &str,
) {
    let sql = format!(
        "CREATE STREAMING TABLE sink_lr WITH ('connector'='kafka') AS {body}"
    );
    let dialect = FunctionStreamDialect {};
    let stmts = Parser::parse_sql(&dialect, &sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    let DFStatement::CreateStreamingTable { query, .. } = &stmts[0] else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    let plan = produce_optimized_plan(&DFStatement::Query(query.clone()), provider)
        .unwrap_or_else(|e| panic!("produce_optimized_plan {sql:?}: {e:#}"));
    let err = rewrite_plan(plan, provider).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains(needle),
        "expected '{needle}' in rewrite error, got: {msg}"
    );
}

#[test]
fn parse_create_function_double_quoted_path_style() {
    assert_parses_as(
        r#"CREATE FUNCTION WITH ("function_path"='./a.wasm', "config_path"='./b.yml')"#,
        "CreateFunction",
    );
}

#[test]
fn parse_create_function_extra_numeric_and_bool_like_strings() {
    assert_parses_as(
        r#"CREATE FUNCTION WITH (
            'function_path'='./f.wasm',
            'config_path'='./c.yml',
            'parallelism'='8',
            'dry_run'='false'
        )"#,
        "CreateFunction",
    );
}

#[test]
fn parse_create_function_fails_without_function_path() {
    let err = parse_sql("CREATE FUNCTION WITH ('config_path'='./only.yml')").unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("function_path") || s.contains("CREATE FUNCTION"),
        "{s}"
    );
}

#[test]
fn parse_drop_function_quoted_name() {
    assert_parses_as(r#"DROP FUNCTION "my-pipeline""#, "DropFunction");
}

#[test]
fn parse_start_stop_function_dotted_style_name() {
    assert_parses_as("START FUNCTION job.v1.main", "StartFunction");
    assert_parses_as("STOP FUNCTION job.v1.main", "StopFunction");
}

#[test]
fn parse_show_functions_extra_whitespace() {
    assert_parses_as("  SHOW   FUNCTIONS  ", "ShowFunctions");
}

#[test]
fn parse_create_table_multiple_columns_types() {
    assert_parses_as(
        "CREATE TABLE metrics (ts TIMESTAMP, name VARCHAR, val DOUBLE, ok BOOLEAN)",
        "CreateTable",
    );
}

#[test]
fn parse_create_table_with_not_null_and_precision() {
    assert_parses_as(
        "CREATE TABLE t (id BIGINT NOT NULL, code DECIMAL(10,2))",
        "CreateTable",
    );
}

#[test]
fn parse_create_table_if_not_exists_if_dialect_accepts() {
    if let Ok(stmts) = parse_sql("CREATE TABLE IF NOT EXISTS guard (id INT)") {
        assert!(format!("{:?}", stmts[0]).starts_with("CreateTable"));
    }
}

#[test]
fn parse_streaming_table_select_star() {
    assert_parses_as(
        "CREATE STREAMING TABLE s1 WITH ('connector'='kafka') AS SELECT * FROM src",
        "StreamingTableStatement",
    );
}

#[test]
fn parse_streaming_table_select_columns() {
    assert_parses_as(
        "CREATE STREAMING TABLE s2 WITH ('connector'='memory') AS SELECT id, v FROM src",
        "StreamingTableStatement",
    );
}

#[test]
fn parse_streaming_table_with_partition_by() {
    let sql = format!(
        "CREATE STREAMING TABLE s3 WITH ('connector' = 'kafka', 'partition_by' = 'id') AS SELECT id, {} FROM src",
        TIMESTAMP_FIELD
    );
    assert_parses_as(&sql, "StreamingTableStatement");
}

#[test]
fn parse_streaming_table_with_idle_time_option() {
    assert_parses_as(
        "CREATE STREAMING TABLE s4 WITH ('connector'='kafka', 'idle_time'='30s') AS SELECT * FROM src",
        "StreamingTableStatement",
    );
}

#[test]
fn parse_streaming_table_sink_name_snake_and_digits() {
    assert_parses_as(
        "CREATE STREAMING TABLE sink_01_out WITH ('connector'='memory') AS SELECT 1",
        "StreamingTableStatement",
    );
}

#[test]
fn parse_streaming_table_comment_before_as_if_supported() {
    let sql = "CREATE STREAMING TABLE c1 WITH ('connector'='kafka') COMMENT 'out' AS SELECT * FROM src";
    if let Ok(stmts) = parse_sql(sql) {
        assert!(
            format!("{:?}", stmts[0]).starts_with("StreamingTableStatement"),
            "{stmts:?}"
        );
    }
}

#[test]
fn parse_three_semicolon_separated_statements() {
    let sql = concat!(
        "CREATE FUNCTION WITH ('function_path'='./x.wasm'); ",
        "CREATE TABLE meta (id INT); ",
        "CREATE STREAMING TABLE out1 WITH ('connector'='kafka') AS SELECT 1",
    );
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 3);
    assert!(format!("{:?}", stmts[0]).starts_with("CreateFunction"));
    assert!(format!("{:?}", stmts[1]).starts_with("CreateTable"));
    assert!(format!("{:?}", stmts[2]).starts_with("StreamingTableStatement"));
}

#[test]
fn parse_rejects_insert_with_columns_list() {
    assert_parse_fails("INSERT INTO t (a,b) VALUES (1,2)");
}

#[test]
fn parse_rejects_update_delete() {
    assert_parse_fails("UPDATE src SET id = 1");
    assert_parse_fails("DELETE FROM src WHERE id = 0");
}

#[test]
fn parse_rejects_merge_explain() {
    assert_parse_fails("EXPLAIN SELECT 1");
    assert_parse_fails("MERGE INTO t USING s ON true WHEN MATCHED THEN UPDATE SET x=1");
}

#[test]
fn parse_rejects_create_schema_database() {
    assert_parse_fails("CREATE SCHEMA s");
    assert_parse_fails("CREATE DATABASE d");
}

#[test]
fn compile_streaming_select_star_from_src() {
    compile_first_streaming(concat!(
        "CREATE STREAMING TABLE kafka_all ",
        "WITH ('connector'='kafka') ",
        "AS SELECT * FROM src",
    ));
}

#[test]
fn compile_streaming_select_id_v_from_src() {
    let sql = format!(
        "CREATE STREAMING TABLE kafka_cols WITH ('connector'='kafka') AS SELECT id, v, {} FROM src",
        TIMESTAMP_FIELD
    );
    compile_first_streaming(&sql);
}

#[test]
fn compile_streaming_memory_connector() {
    compile_first_streaming(
        "CREATE STREAMING TABLE mem_sink WITH ('connector'='memory') AS SELECT * FROM src",
    );
}

#[test]
fn compile_streaming_with_partition_by_id() {
    compile_first_streaming(concat!(
        "CREATE STREAMING TABLE part_sink ",
        "WITH ('connector'='kafka', 'partition_by'='id') ",
        "AS SELECT * FROM src",
    ));
}

#[test]
fn compile_streaming_connector_postgres_string() {
    compile_first_streaming(
        "CREATE STREAMING TABLE pg_sink WITH ('connector'='postgres') AS SELECT id FROM src",
    );
}

#[test]
#[should_panic(expected = "connector")]
fn compile_streaming_fails_without_connector() {
    let sql = "CREATE STREAMING TABLE bad WITH ('partition_by'='id') AS SELECT * FROM src";
    let stmts = parse_sql(sql).unwrap();
    let _ = Coordinator::new().compile_plan(stmts[0].as_ref(), fake_src_stream_provider());
}

#[test]
fn compile_plan_show_functions() {
    let stmts = parse_sql("SHOW FUNCTIONS").unwrap();
    Coordinator::new()
        .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
        .expect("ShowFunctions plan");
}

#[test]
fn compile_plan_show_tables() {
    let stmts = parse_sql("SHOW TABLES").unwrap();
    Coordinator::new()
        .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
        .expect("ShowCatalogTables plan");
}

#[test]
fn compile_plan_show_create_table() {
    let stmts = parse_sql("SHOW CREATE TABLE my_table").unwrap();
    Coordinator::new()
        .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
        .expect("ShowCreateTable plan");
}

#[test]
fn compile_plan_start_stop_drop_function() {
    for sql in [
        "START FUNCTION t1",
        "STOP FUNCTION t1",
        "DROP FUNCTION t1",
    ] {
        let stmts = parse_sql(sql).unwrap();
        Coordinator::new()
            .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
            .unwrap_or_else(|e| panic!("{sql}: {e:#}"));
    }
}

#[test]
fn compile_plan_create_function() {
    let sql =
        "CREATE FUNCTION WITH ('function_path'='./x.wasm', 'config_path'='./c.yml')";
    let stmts = parse_sql(sql).unwrap();
    Coordinator::new()
        .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
        .expect("CreateFunction plan");
}

#[test]
fn compile_plan_create_table_simple_ddl() {
    let sql = "CREATE TABLE local_only (id INT, name VARCHAR)";
    let stmts = parse_sql(sql).unwrap();
    Coordinator::new()
        .compile_plan(stmts[0].as_ref(), StreamSchemaProvider::new())
        .expect("CreateTable plan");
}

#[test]
fn streaming_where_eq_ne_and_or_not() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE id = 1 AND (v <> 'x' OR NOT (id < 0))"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE id > 0 AND id <= 100 AND id >= 1"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT id, v, {ts} FROM src WHERE (id = 2 OR id = 3) AND v IS NOT NULL"),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_where_in_between_like_null() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE id IN (1, 2, 3)"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE id NOT IN (99, 100)"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE id BETWEEN 1 AND 10"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE v LIKE 'pre%'"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT * FROM src WHERE v IS NULL"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT id, v, {ts} FROM src WHERE v IS NOT NULL OR id = 0"),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_where_scalar_subquery() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_dim_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT src.id, src.v, src.{ts} FROM src \
             WHERE src.id = (SELECT MAX(dim.id) FROM dim)"
        ),
        &p,
    );
}

#[test]
#[should_panic(expected = "window")]
fn streaming_where_in_subquery_currently_panics() {
    let p = fake_src_dim_stream_provider();
    compile_streaming_select_body(
        "SELECT * FROM src WHERE id IN (SELECT id FROM dim WHERE amt IS NOT NULL)",
        p,
    );
}

#[test]
#[should_panic(expected = "window")]
fn streaming_where_exists_correlated_currently_panics() {
    let p = fake_src_dim_stream_provider();
    compile_streaming_select_body(
        "SELECT * FROM src WHERE EXISTS (SELECT 1 FROM dim WHERE dim.id = src.id)",
        p,
    );
}

#[test]
fn streaming_select_case_coalesce_cast() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!(
            "SELECT CASE WHEN id < 0 THEN 0 WHEN id > 1000 THEN 1000 ELSE id END AS c, v, {ts} FROM src"
        ),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT COALESCE(v, 'na') AS v2, id, {ts} FROM src"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!("SELECT CAST(id AS DOUBLE) AS id_f, {ts} FROM src"),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_select_row_time_distinct() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!("SELECT row_time(), id, v, {ts} FROM src"),
        fake_src_stream_provider(),
    );
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites("SELECT DISTINCT id FROM src", &p);
}

#[test]
fn streaming_from_subquery_nested() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!("SELECT * FROM (SELECT id, v, {ts} FROM src WHERE id > 0) AS t"),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!(
            "SELECT * FROM (SELECT * FROM (SELECT id FROM src) AS i2) AS i1"
        ),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_with_cte_single_and_chain() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!(
            "WITH a AS (SELECT id, v, {ts} FROM src WHERE id > 0) SELECT * FROM a"
        ),
        fake_src_stream_provider(),
    );
    compile_streaming_select_body(
        &format!(
            "WITH a AS (SELECT id FROM src), b AS (SELECT id FROM a WHERE id > 1) SELECT * FROM b"
        ),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_group_by_updating_aggregate_bundle() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, COUNT(*), SUM(id), AVG(id), MIN(v), MAX(v) FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_group_by_count_distinct_and_stats() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, COUNT(DISTINCT v), STDDEV_POP(id), VAR_POP(id) FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_group_by_having() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, COUNT(*) AS c FROM src GROUP BY id HAVING COUNT(*) >= 0",
        &p,
    );
}

#[test]
fn streaming_group_by_tumble_window() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT tumble(INTERVAL '1' MINUTE) AS w, id, COUNT(*) AS c, MAX({ts}) AS max_evt \
             FROM src GROUP BY tumble(INTERVAL '1' MINUTE), id"
        ),
        &p,
    );
}

#[test]
fn streaming_group_by_hop_window() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT hop(INTERVAL '1' MINUTE, INTERVAL '3' MINUTE) AS w, id, SUM(id), MAX({ts}) AS max_evt \
             FROM src GROUP BY hop(INTERVAL '1' MINUTE, INTERVAL '3' MINUTE), id"
        ),
        &p,
    );
}

#[test]
fn streaming_window_row_number_over_tumble_aggregate() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT ROW_NUMBER() OVER (PARTITION BY w ORDER BY max_evt) AS rn, id, w, max_evt \
             FROM ( \
               SELECT tumble(INTERVAL '1' MINUTE) AS w, id, MAX({ts}) AS max_evt \
               FROM src \
               GROUP BY tumble(INTERVAL '1' MINUTE), id \
             ) AS x"
        ),
        &p,
    );
}

#[test]
fn streaming_inner_join_eq_and_compound_on() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_dim_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT src.id, src.v, dim.name, src.{ts} \
             FROM src INNER JOIN dim ON src.id = dim.id"
        ),
        &p,
    );
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT src.id, dim.amt, src.{ts} \
             FROM src JOIN dim ON src.id = dim.id AND dim.amt > CAST(0 AS DOUBLE)"
        ),
        &p,
    );
}

#[test]
#[ignore]
fn streaming_self_join_inner_ignored() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!(
            "SELECT a.id, b.v, a.{ts} \
             FROM src AS a JOIN src AS b ON a.id = b.id AND a.v = b.v"
        ),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_join_subquery_branch() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_dim_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT src.id, src.v, j.name, src.{ts} \
             FROM src JOIN (SELECT id, name FROM dim) AS j ON src.id = j.id"
        ),
        &p,
    );
}

#[test]
fn streaming_union_all_compatible_schemas() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_dim_stream_provider();
    compile_streaming_select_body(
        &format!(
            "SELECT id, v, {ts} FROM src \
             UNION ALL \
             SELECT id, name AS v, {ts} FROM dim"
        ),
        p,
    );
}

#[test]
fn streaming_logical_group_by_two_keys_and_filter_agg() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, v, COUNT(*) AS c FROM src GROUP BY id, v",
        &p,
    );
    assert_streaming_select_logical_rewrites(
        "SELECT id, SUM(id) FILTER (WHERE v IS NOT NULL) AS s FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_logical_more_builtin_aggregates() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, STDDEV_POP(CAST(id AS DOUBLE)), COVAR_SAMP(CAST(id AS DOUBLE), CAST(id AS DOUBLE)), \
         COVAR_POP(CAST(id AS DOUBLE), CAST(id AS DOUBLE)) \
         FROM src GROUP BY id",
        &p,
    );
    assert_streaming_select_logical_rewrites(
        "SELECT id, CORR(CAST(id AS DOUBLE), CAST(id AS DOUBLE)) FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_logical_bit_and_bool_aggregates() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, BIT_AND(id), BIT_OR(id), BIT_XOR(id) FROM src GROUP BY id",
        &p,
    );
    assert_streaming_select_logical_rewrites(
        "SELECT id, BOOL_AND(id > 0), BOOL_OR(id < 100000) FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_logical_array_agg_and_list_union() {
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        "SELECT id, ARRAY_AGG(v) FROM src GROUP BY id",
        &p,
    );
}

#[test]
fn streaming_logical_scalar_funcs_on_projection() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!(
            "SELECT ABS(id), POWER(CAST(id AS DOUBLE), 2.0), UPPER(v), LOWER(v), BTRIM(v), \
             CHARACTER_LENGTH(v), CONCAT(v, '_x'), {ts} FROM src"
        ),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_logical_nullif_regexp() {
    let ts = TIMESTAMP_FIELD;
    compile_streaming_select_body(
        &format!(
            "SELECT id, NULLIF(v, ''), REGEXP_LIKE(v, '^a'), {ts} FROM src WHERE v IS NOT NULL OR id = 0"
        ),
        fake_src_stream_provider(),
    );
}

#[test]
fn streaming_window_first_value_over_tumbled_subquery() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT FIRST_VALUE(id) OVER (PARTITION BY w ORDER BY max_evt) AS fv, w, id \
             FROM ( \
               SELECT tumble(INTERVAL '1' MINUTE) AS w, id, MAX({ts}) AS max_evt \
               FROM src GROUP BY tumble(INTERVAL '1' MINUTE), id \
             ) AS x"
        ),
        &p,
    );
}

#[test]
fn streaming_window_lag_over_tumbled_subquery() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT LAG(id, 1) OVER (PARTITION BY w ORDER BY max_evt) AS prev_id, w, id \
             FROM ( \
               SELECT tumble(INTERVAL '2' MINUTE) AS w, id, MAX({ts}) AS max_evt \
               FROM src GROUP BY tumble(INTERVAL '2' MINUTE), id \
             ) AS x"
        ),
        &p,
    );
}

#[test]
fn streaming_window_lead_over_tumbled_subquery() {
    let ts = TIMESTAMP_FIELD;
    let p = fake_src_stream_provider();
    assert_streaming_select_logical_rewrites(
        &format!(
            "SELECT LEAD(id, 1) OVER (PARTITION BY w ORDER BY max_evt) AS next_id, w \
             FROM ( \
               SELECT tumble(INTERVAL '2' MINUTE) AS w, id, MAX({ts}) AS max_evt \
               FROM src GROUP BY tumble(INTERVAL '2' MINUTE), id \
             ) AS x"
        ),
        &p,
    );
}

#[test]
fn streaming_logical_full_outer_join_errors() {
    let p = fake_src_dim_stream_provider();
    assert_streaming_select_logical_rewrite_err_contains(
        "SELECT src.id, dim.name FROM src FULL OUTER JOIN dim ON src.id = dim.id",
        &p,
        "inner",
    );
}

#[test]
#[should_panic(expected = "Non-inner")]
fn streaming_left_join_errors_without_window() {
    let ts = TIMESTAMP_FIELD;
    let sql = format!(
        "CREATE STREAMING TABLE sink_left WITH ('connector'='kafka') AS \
         SELECT src.id, dim.name, src.{ts} FROM src LEFT JOIN dim ON src.id = dim.id"
    );
    let stmts = parse_sql(&sql).unwrap();
    let _ = Coordinator::new().compile_plan(stmts[0].as_ref(), fake_src_dim_stream_provider());
}
