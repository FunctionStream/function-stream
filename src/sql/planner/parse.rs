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
    CreateFunction, DropFunction, ShowFunctions, StartFunction, Statement as CoordinatorStatement,
    StopFunction, StreamingSql,
};

/// Stage 1: String → Box<dyn Statement>
///
/// Parses SQL using FunctionStreamDialect (from sqlparser-rs), then classifies
/// the result into either a FunctionStream DDL statement or a StreamingSql,
/// both unified under the coordinator's Statement trait.
pub fn parse_sql(query: &str) -> Result<Box<dyn CoordinatorStatement>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return plan_err!("Query is empty");
    }

    let dialect = FunctionStreamDialect {};
    let mut statements = Parser::parse_sql(&dialect, trimmed)
        .map_err(|e| DataFusionError::Plan(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return plan_err!("No SQL statements found");
    }

    let stmt = statements.remove(0);
    classify_statement(stmt)
}

/// Classify a parsed DataFusion Statement into the coordinator's Statement type.
///
/// FunctionStream DDL (CREATE/DROP/START/STOP FUNCTION, SHOW FUNCTIONS)
/// is converted to concrete coordinator types; everything else is wrapped
/// in StreamingSql.
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
        other => Ok(Box::new(StreamingSql::new(other))),
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

    fn is_streaming_sql(stmt: &dyn CoordinatorStatement) -> bool {
        let debug = format!("{:?}", stmt);
        debug.starts_with("StreamingSql")
    }

    fn is_ddl(stmt: &dyn CoordinatorStatement) -> bool {
        !is_streaming_sql(stmt)
    }

    #[test]
    fn test_parse_create_function() {
        let sql =
            "CREATE FUNCTION WITH ('function_path'='./test.wasm', 'config_path'='./config.yml')";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_create_function_minimal() {
        let sql = "CREATE FUNCTION WITH ('function_path'='./processor.wasm')";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_drop_function() {
        let sql = "DROP FUNCTION my_task";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_start_function() {
        let sql = "START FUNCTION my_task";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_stop_function() {
        let sql = "STOP FUNCTION my_task";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_show_functions() {
        let sql = "SHOW FUNCTIONS";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }

    #[test]
    fn test_parse_case_insensitive() {
        let sql1 = "create function with ('function_path'='./test.wasm')";
        assert!(is_ddl(parse_sql(sql1).unwrap().as_ref()));

        let sql2 = "show functions";
        assert!(is_ddl(parse_sql(sql2).unwrap().as_ref()));

        let sql3 = "start function my_task";
        assert!(is_ddl(parse_sql(sql3).unwrap().as_ref()));
    }

    #[test]
    fn test_parse_streaming_sql() {
        let sql =
            "SELECT count(*), tumble(interval '1 minute') as window FROM events GROUP BY window";
        let stmt = parse_sql(sql).unwrap();
        assert!(is_streaming_sql(stmt.as_ref()));
    }

    #[test]
    fn test_parse_empty() {
        assert!(parse_sql("").is_err());
        assert!(parse_sql("  ").is_err());
    }

    #[test]
    fn test_parse_with_extra_properties() {
        let sql = r#"CREATE FUNCTION WITH (
            'function_path'='./test.wasm',
            'config_path'='./config.yml',
            'parallelism'='4',
            'memory-limit'='256mb'
        )"#;
        let stmt = parse_sql(sql).unwrap();
        assert!(is_ddl(stmt.as_ref()));
    }
}
