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

//! Map sqlparser [`Statement`](datafusion::sql::sqlparser::ast::Statement) values into
//! coordinator [`Statement`](super::statement::Statement) trait objects.

use std::collections::HashMap;

use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    ObjectType, ShowCreateObject, SqlOption, Statement as DFStatement,
};

use super::{
    CreateFunction, CreateTable, DropFunction, DropStreamingTableStatement, DropTableStatement,
    ShowCatalogTables, ShowCreateStreamingTable, ShowCreateTable, ShowFunctions,
    ShowStreamingTables, StartFunction, Statement, StopFunction, StreamingTableStatement,
};

/// Convert [`DFStatement`] from the FunctionStream SQL dialect into a coordinator statement.
pub fn classify_statement(stmt: DFStatement) -> Result<Box<dyn Statement>> {
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
        DFStatement::ShowStreamingTable => Ok(Box::new(ShowStreamingTables::new())),
        DFStatement::ShowCreate { obj_type, obj_name } => match obj_type {
            ShowCreateObject::Table => Ok(Box::new(ShowCreateTable::new(obj_name.to_string()))),
            ShowCreateObject::StreamingTable => Ok(Box::new(ShowCreateStreamingTable::new(
                obj_name.to_string(),
            ))),
            _ => plan_err!(
                "SHOW CREATE {obj_type} is not supported; use SHOW CREATE TABLE or SHOW CREATE STREAMING TABLE <name>"
            ),
        },
        s @ DFStatement::CreateTable(_) => Ok(Box::new(CreateTable::new(s))),
        s @ DFStatement::CreateStreamingTable { .. } => {
            Ok(Box::new(StreamingTableStatement::new(s)))
        }
        stmt @ DFStatement::Drop { .. } => {
            let DFStatement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } = &stmt
            else {
                unreachable!()
            };
            match object_type {
                ObjectType::Table => {
                    if names.len() != 1 {
                        return plan_err!(
                            "DROP TABLE supports exactly one table name per statement"
                        );
                    }
                    Ok(Box::new(DropTableStatement::new(stmt)))
                }
                ObjectType::StreamingTable => {
                    if names.len() != 1 {
                        return plan_err!(
                            "DROP STREAMING TABLE supports exactly one table name per statement"
                        );
                    }
                    let table_name = names[0].to_string();
                    Ok(Box::new(DropStreamingTableStatement::new(
                        table_name, *if_exists,
                    )))
                }
                _ => plan_err!(
                    "Only DROP TABLE and DROP STREAMING TABLE are supported in this SQL frontend"
                ),
            }
        }
        DFStatement::Insert { .. } => plan_err!(
            "INSERT is not supported; only CREATE TABLE and CREATE STREAMING TABLE (with AS SELECT) \
             are supported for defining table/query pipelines in this SQL frontend"
        ),
        other => plan_err!("Unsupported SQL statement: {other}"),
    }
}

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
    use crate::sql::parse::parse_sql;

    fn first_classified(sql: &str) -> Box<dyn Statement> {
        let mut stmts = parse_sql(sql).unwrap();
        assert!(!stmts.is_empty());
        classify_statement(stmts.remove(0)).unwrap()
    }

    fn is_type(stmt: &dyn Statement, prefix: &str) -> bool {
        format!("{stmt:?}").starts_with(prefix)
    }

    #[test]
    fn test_parse_create_function() {
        let sql =
            "CREATE FUNCTION WITH ('function_path'='./test.wasm', 'config_path'='./config.yml')";
        let stmt = first_classified(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }

    #[test]
    fn test_parse_create_function_minimal() {
        let sql = "CREATE FUNCTION WITH ('function_path'='./processor.wasm')";
        let stmt = first_classified(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }

    #[test]
    fn test_parse_drop_function() {
        let stmt = first_classified("DROP FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "DropFunction"));
    }

    #[test]
    fn test_parse_start_function() {
        let stmt = first_classified("START FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "StartFunction"));
    }

    #[test]
    fn test_parse_stop_function() {
        let stmt = first_classified("STOP FUNCTION my_task");
        assert!(is_type(stmt.as_ref(), "StopFunction"));
    }

    #[test]
    fn test_parse_show_functions() {
        let stmt = first_classified("SHOW FUNCTIONS");
        assert!(is_type(stmt.as_ref(), "ShowFunctions"));
    }

    #[test]
    fn test_parse_show_tables() {
        let stmt = first_classified("SHOW TABLES");
        assert!(is_type(stmt.as_ref(), "ShowCatalogTables"));
    }

    #[test]
    fn test_parse_show_create_table() {
        let stmt = first_classified("SHOW CREATE TABLE my_src");
        assert!(is_type(stmt.as_ref(), "ShowCreateTable"));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = first_classified("CREATE TABLE foo (id INT, name VARCHAR)");
        assert!(is_type(stmt.as_ref(), "CreateTable"));
    }

    #[test]
    fn test_parse_create_table_connector_source_ddl() {
        let sql = concat!(
            "CREATE TABLE kafka_src (id BIGINT, ts TIMESTAMP NOT NULL, WATERMARK FOR ts) ",
            "WITH ('connector' = 'kafka', 'format' = 'json', 'topic' = 'events')",
        );
        let stmt = first_classified(sql);
        assert!(is_type(stmt.as_ref(), "CreateTable"));
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = first_classified("DROP TABLE foo");
        assert!(is_type(stmt.as_ref(), "DropTableStatement"));
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt = first_classified("DROP TABLE IF EXISTS foo");
        assert!(is_type(stmt.as_ref(), "DropTableStatement"));
    }

    #[test]
    fn test_parse_drop_streaming_table() {
        let stmt = first_classified("DROP STREAMING TABLE my_sink");
        assert!(is_type(stmt.as_ref(), "DropStreamingTableStatement"));
    }

    #[test]
    fn test_parse_drop_streaming_table_if_exists() {
        let stmt = first_classified("DROP STREAMING TABLE IF EXISTS my_sink");
        assert!(is_type(stmt.as_ref(), "DropStreamingTableStatement"));
    }

    #[test]
    fn test_parse_show_streaming_tables() {
        let stmt = first_classified("SHOW STREAMING TABLES");
        assert!(is_type(stmt.as_ref(), "ShowStreamingTables"));
    }

    #[test]
    fn test_parse_show_create_streaming_table() {
        let stmt = first_classified("SHOW CREATE STREAMING TABLE my_sink");
        assert!(is_type(stmt.as_ref(), "ShowCreateStreamingTable"));
    }

    #[test]
    fn test_parse_create_streaming_table() {
        let sql = concat!(
            "CREATE STREAMING TABLE my_sink ",
            "WITH ('connector' = 'kafka') ",
            "AS SELECT id FROM src",
        );
        let stmt = first_classified(sql);
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
        let stmt = first_classified(sql);
        assert!(is_type(stmt.as_ref(), "StreamingTableStatement"));
    }

    #[test]
    fn test_parse_case_insensitive() {
        assert!(is_type(
            first_classified("create function with ('function_path'='./test.wasm')").as_ref(),
            "CreateFunction"
        ));
        assert!(is_type(
            first_classified("show functions").as_ref(),
            "ShowFunctions"
        ));
        assert!(is_type(
            first_classified("start function my_task").as_ref(),
            "StartFunction"
        ));
    }

    #[test]
    fn test_parse_multiple_statements() {
        let sql = concat!(
            "CREATE TABLE t1 (id INT); ",
            "CREATE STREAMING TABLE sk WITH ('connector' = 'kafka') AS SELECT id FROM t1",
        );
        let mut ast = parse_sql(sql).unwrap();
        assert_eq!(ast.len(), 2);
        let s0 = classify_statement(ast.remove(0)).unwrap();
        let s1 = classify_statement(ast.remove(0)).unwrap();
        assert!(is_type(s0.as_ref(), "CreateTable"));
        assert!(is_type(s1.as_ref(), "StreamingTableStatement"));
    }

    #[test]
    fn test_classify_unsupported_statement() {
        let mut stmts = parse_sql("SELECT 1").unwrap();
        assert!(classify_statement(stmts.remove(0)).is_err());
    }

    #[test]
    fn test_insert_not_supported() {
        let mut stmts = parse_sql("INSERT INTO sink SELECT * FROM src").unwrap();
        let err = classify_statement(stmts.remove(0)).unwrap_err();
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
        let stmt = first_classified(sql);
        assert!(is_type(stmt.as_ref(), "CreateFunction"));
    }
}
