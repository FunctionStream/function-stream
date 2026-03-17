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

use std::collections::HashMap;

use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{SqlOption, Statement as DFStatement};
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::coordinator::{
    CreateFunction, CreateTable, DropFunction, InsertStatement, ShowFunctions, StartFunction,
    Statement as CoordinatorStatement, StopFunction,
};

/// Stage 1: String → Vec<Box<dyn Statement>>
///
/// Parses SQL using FunctionStreamDialect (from sqlparser-rs), then classifies
/// each statement into a concrete coordinator Statement type.
/// A single SQL input may contain multiple statements (separated by `;`).
pub fn parse_sql(query: &str) -> Result<Vec<Box<dyn CoordinatorStatement>>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return plan_err!("Query is empty");
    }

    let dialect = FunctionStreamDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed)
        .map_err(|e| DataFusionError::Plan(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return plan_err!("No SQL statements found");
    }

    statements.into_iter().map(classify_statement).collect()
}

/// Classify a parsed DataFusion Statement into the coordinator's Statement type.
///
/// Statement classification mirrors the analysis flow from `parse_and_get_arrow_program`:
///   - FunctionStream DDL → concrete coordinator types (CreateFunction, DropFunction, etc.)
///   - CREATE TABLE / CREATE VIEW → CreateTable (catalog registration)
///   - INSERT INTO → InsertStatement (streaming pipeline)
///   - Everything else → error (unsupported)
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
        s @ DFStatement::CreateTable(_) | s @ DFStatement::CreateView { .. } => {
            Ok(Box::new(CreateTable::new(s)))
        }
        s @ DFStatement::Insert(_) => Ok(Box::new(InsertStatement::new(s))),
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
    fn test_parse_create_table() {
        let stmt = first_stmt("CREATE TABLE foo (id INT, name VARCHAR)");
        assert!(is_type(stmt.as_ref(), "CreateTable"));
    }

    #[test]
    fn test_parse_insert_statement() {
        let stmt = first_stmt("INSERT INTO sink SELECT * FROM source");
        assert!(is_type(stmt.as_ref(), "InsertStatement"));
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
        let sql = "CREATE TABLE t1 (id INT); INSERT INTO sink SELECT * FROM t1";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(is_type(stmts[0].as_ref(), "CreateTable"));
        assert!(is_type(stmts[1].as_ref(), "InsertStatement"));
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
