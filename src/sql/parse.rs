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

//! Coordinator-facing SQL parsing (`parse_sql`).
//!
//! **Data-definition / pipeline shape (this entry point)**  
//! Only these table-related forms are supported:
//! - **`CREATE TABLE ... (cols [, WATERMARK FOR ...]) WITH ('connector' = '...', 'format' = '...', ...)`**  
//!   connector-backed **source** DDL (no `AS SELECT`; `connector` in `WITH` selects this path)
//! - **`CREATE TABLE ...`** other forms (including `CREATE TABLE ... AS SELECT` where DataFusion accepts it)
//! - **`CREATE STREAMING TABLE ... WITH (...) AS SELECT ...`** (streaming sink DDL)
//! - **`DROP TABLE`** / **`DROP TABLE IF EXISTS`** / **`DROP STREAMING TABLE`** (alias for `DROP TABLE` on the stream catalog)
//! - **`SHOW TABLES`** — list stream catalog tables (connector sources and streaming sinks)
//! - **`SHOW CREATE TABLE <name>`** — best-effort DDL text (full `WITH` / `AS SELECT` may not be stored)
//!
//! **`INSERT` is not supported** here — use `CREATE TABLE ... AS SELECT` or
//! `CREATE STREAMING TABLE ... AS SELECT` to define the query shape instead.
//!
//! Other supported statements include function lifecycle (`CREATE FUNCTION WITH`, `START FUNCTION`, …).

use std::collections::HashMap;

use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    ObjectType, ShowCreateObject, SqlOption, Statement as DFStatement,
};
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::coordinator::{
    CreateFunction, CreateTable, DropFunction, DropTableStatement, ShowCatalogTables,
    ShowCreateTable, ShowFunctions, StartFunction, Statement as CoordinatorStatement, StopFunction,
    StreamingTableStatement,
};

/// `DROP STREAMING TABLE t` is accepted as sugar for `DROP TABLE t` against the same catalog.
fn rewrite_drop_streaming_table(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.len() >= 4
        && tokens[0].eq_ignore_ascii_case("drop")
        && tokens[1].eq_ignore_ascii_case("streaming")
        && tokens[2].eq_ignore_ascii_case("table")
    {
        let rest = tokens[3..].join(" ");
        return format!("DROP TABLE {rest}");
    }
    sql.to_string()
}

pub fn parse_sql(query: &str) -> Result<Vec<Box<dyn CoordinatorStatement>>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return plan_err!("Query is empty");
    }

    let dialect = FunctionStreamDialect {};
    let to_parse = rewrite_drop_streaming_table(trimmed);
    let statements = Parser::parse_sql(&dialect, &to_parse)
        .map_err(|e| DataFusionError::Plan(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return plan_err!("No SQL statements found");
    }

    statements.into_iter().map(classify_statement).collect()
}

fn classify_statement(stmt: DFStatement) -> Result<Box<dyn CoordinatorStatement>> {
    match stmt {
        DFStatement::CreateFunctionWith { options } => {
            let properties = sql_options_to_map(&options);
            let create_fn = CreateFunction::from_properties(properties)
                .map_err(|e| DataFusionError::Plan(format!("CREATE FUNCTION: {e}")))?;
            Ok(Box::new(create_fn))
        }
        DFStatement::StartFunction { name } => Ok(Box::new(StartFunction::new(name.to_string()))),
        DFStatement::StopFunction { name } => Ok(Box::new(StopFunction::new(name.to_string()))),
        DFStatement::DropFunction { func_desc, .. } => {
            let name = func_desc
                .first()
                .map(|d| d.name.to_string())
                .unwrap_or_default();
            Ok(Box::new(DropFunction::new(name)))
        }
        DFStatement::ShowFunctions { .. } => Ok(Box::new(ShowFunctions::new())),
        DFStatement::ShowTables { .. } => Ok(Box::new(ShowCatalogTables::new())),
        DFStatement::ShowCreate { obj_type, obj_name } => {
            if obj_type != ShowCreateObject::Table {
                return plan_err!(
                    "SHOW CREATE {obj_type} is not supported; use SHOW CREATE TABLE <name>"
                );
            }
            Ok(Box::new(ShowCreateTable::new(obj_name.to_string())))
        },
        s @ DFStatement::CreateTable(_) => Ok(Box::new(CreateTable::new(s))),
        s @ DFStatement::CreateStreamingTable { .. } => {
            Ok(Box::new(StreamingTableStatement::new(s)))
        }
        stmt @ DFStatement::Drop { .. } => {
            {
                let DFStatement::Drop {
                    object_type,
                    names,
                    ..
                } = &stmt
                else {
                    unreachable!()
                };
                if *object_type != ObjectType::Table {
                    return plan_err!("Only DROP TABLE is supported in this SQL frontend");
                }
                if names.len() != 1 {
                    return plan_err!("DROP TABLE supports exactly one table name per statement");
                }
            }
            Ok(Box::new(DropTableStatement::new(stmt)))
        }
        DFStatement::Insert { .. } => plan_err!(
            "INSERT is not supported; only CREATE TABLE and CREATE STREAMING TABLE (with AS SELECT) \
             are supported for defining table/query pipelines in this SQL frontend"
        ),
        other => plan_err!("Unsupported SQL statement: {other}"),
    }
}

/// Convert Vec<SqlOption> (KeyValue pairs) into HashMap.
fn sql_options_to_map(options: &[SqlOption]) -> HashMap<String, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn first_stmt(sql: &str) -> Box<dyn CoordinatorStatement> {
        let mut stmts = parse_sql(sql).unwrap();
        assert!(!stmts.is_empty());
        stmts.remove(0)
    }

    fn is_type(stmt: &dyn CoordinatorStatement, prefix: &str) -> bool {
        format!("{:?}", stmt).starts_with(prefix)
    }

    #[test]
    fn test_parse_create_function() {
        let sql =
            "CREATE FUNCTION WITH ('function_path'='./test.wasm', 'config_path'='./config.yml')";
        let stmt = first_stmt(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }

    #[test]
    fn test_parse_create_function_minimal() {
        let sql = "CREATE FUNCTION WITH ('function_path'='./processor.wasm')";
        let stmt = first_stmt(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }

    #[test]
    fn test_parse_drop_function() {
        let stmt = first_stmt("DROP FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "DropFunction"));
    }

    #[test]
    fn test_parse_start_function() {
        let stmt = first_stmt("START FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "StartFunction"));
    }

    #[test]
    fn test_parse_stop_function() {
        let stmt = first_stmt("STOP FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "StopFunction"));
    }

    #[test]
    fn test_parse_show_functions() {
        let stmt = first_stmt("SHOW FUNCTIONS");
        assert!(is_type(stmt.as_ref(), "ShowFunctions"));
    }

    #[test]
    fn test_parse_show_tables() {
        let stmt = first_stmt("SHOW TABLES");
        assert!(is_type(stmt.as_ref(), "ShowCatalogTables"));
    }

    #[test]
    fn test_parse_show_create_table() {
        let stmt = first_stmt("SHOW CREATE TABLE my_src");
        assert!(is_type(stmt.as_ref(), "ShowCreateTable"));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = first_stmt("CREATE TABLE foo (id INT, name VARCHAR)");
        assert!(is_type(stmt.as_ref(), "CreateTable"));
    }

    #[test]
    fn test_parse_create_table_connector_source_ddl() {
        let sql = concat!(
            "CREATE TABLE kafka_src (id BIGINT, ts TIMESTAMP NOT NULL, WATERMARK FOR ts) ",
            "WITH ('connector' = 'kafka', 'format' = 'json', 'topic' = 'events')",
        );
        let stmt = first_stmt(sql);
        assert!(is_type(stmt.as_ref(), "CreateTable"));
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = first_stmt("DROP TABLE foo");
        assert!(is_type(stmt.as_ref(), "DropTableStatement"));
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt = first_stmt("DROP TABLE IF EXISTS foo");
        assert!(is_type(stmt.as_ref(), "DropTableStatement"));
    }

    #[test]
    fn test_parse_drop_streaming_table_rewritten() {
        let stmt = first_stmt("DROP STREAMING TABLE my_sink");
        assert!(is_type(stmt.as_ref(), "DropTableStatement"));
    }

    /// `CREATE STREAMING TABLE` is the sink DDL supported by FunctionStream (not `CREATE STREAM TABLE`).
    #[test]
    fn test_parse_create_streaming_table() {
        let sql = concat!(
            "CREATE STREAMING TABLE my_sink ",
            "WITH ('connector' = 'kafka') ",
            "AS SELECT id FROM src",
        );
        let stmt = first_stmt(sql);
        assert!(
            is_type(stmt.as_ref(), "StreamingTableStatement"),
            "expected StreamingTableStatement, got {:?}",
            stmt
        );
    }

    #[test]
    fn test_parse_create_streaming_table_case_insensitive() {
        let sql = concat!(
            "create streaming table out_q ",
            "with ('connector' = 'memory') ",
            "as select 1 as x",
        );
        let stmt = first_stmt(sql);
        assert!(is_type(stmt.as_ref(), "StreamingTableStatement"));
    }

    #[test]
    fn test_parse_case_insensitive() {
        assert!(is_type(
            first_stmt("create function with ('function_path'='./test.wasm')").as_ref(),
            "CreateFunction"
        ));
        assert!(is_type(
            first_stmt("show functions").as_ref(),
            "ShowFunctions"
        ));
        assert!(is_type(
            first_stmt("start function my_task").as_ref(),
            "StartFunction"
        ));
    }

    #[test]
    fn test_parse_multiple_statements() {
        let sql = concat!(
            "CREATE TABLE t1 (id INT); ",
            "CREATE STREAMING TABLE sk WITH ('connector' = 'kafka') AS SELECT id FROM t1",
        );
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(is_type(stmts[0].as_ref(), "CreateTable"));
        assert!(is_type(stmts[1].as_ref(), "StreamingTableStatement"));
    }

    #[test]
    fn test_parse_empty() {
        assert!(parse_sql("").is_err());
        assert!(parse_sql("  ").is_err());
    }

    #[test]
    fn test_parse_unsupported_statement() {
        let result = parse_sql("SELECT 1");
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_not_supported() {
        let err = parse_sql("INSERT INTO sink SELECT * FROM src").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("INSERT") && msg.contains("not supported"),
            "expected explicit INSERT rejection, got: {msg}"
        );
        assert!(
            msg.contains("CREATE TABLE") || msg.contains("CREATE STREAMING TABLE"),
            "error should mention supported alternatives, got: {msg}"
        );
    }

    #[test]
    fn test_parse_with_extra_properties() {
        let sql = r#"CREATE FUNCTION WITH (
            'function_path'='./test.wasm',
            'config_path'='./config.yml',
            'parallelism'='4',
            'memory-limit'='256mb'
        )"#;
        let stmt = first_stmt(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }
}
