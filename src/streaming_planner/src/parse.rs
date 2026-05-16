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

//! FunctionStream SQL parsing (`parse_sql`).
//!
//! This module only performs lexical/syntactic parsing into sqlparser
//! [`Statement`](datafusion::sql::sqlparser::ast::Statement) values using
//! [`FunctionStreamDialect`]. Mapping those AST nodes to coordinator `Statement`
//! values is done by `classify_statement` in the `function-stream` coordinator module.
//!
//! **Data-definition / pipeline shape (supported forms in the dialect)**  
//! - **`CREATE TABLE ... (cols [, WATERMARK FOR ...]) WITH (...)`** — connector-backed source DDL  
//! - **`CREATE TABLE ...`** other forms (including `AS SELECT` where the dialect accepts it)  
//! - **`CREATE STREAMING TABLE ... WITH (...) AS SELECT ...`**  
//! - **`DROP TABLE`** / **`DROP STREAMING TABLE`**  
//! - **`SHOW TABLES`**, **`SHOW STREAMING TABLE(S)`**, **`SHOW CREATE TABLE`**, **`SHOW CREATE STREAMING TABLE`**  
//!
//! **`INSERT` is not supported** at the coordinator layer — use `CREATE TABLE ... AS SELECT` or
//! `CREATE STREAMING TABLE ... AS SELECT` instead (see coordinator classification).

use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::Statement as DFStatement;
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// Parse SQL text into zero or more dialect [`Statement`](DFStatement) nodes.
pub fn parse_sql(query: &str) -> Result<Vec<DFStatement>> {
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

    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_multiple_statements_ast() {
        let sql = concat!(
            "CREATE TABLE t1 (id INT); ",
            "CREATE STREAMING TABLE sk WITH ('connector' = 'kafka') AS SELECT id FROM t1",
        );
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], DFStatement::CreateTable(_)));
        assert!(matches!(stmts[1], DFStatement::CreateStreamingTable { .. }));
    }

    #[test]
    fn test_parse_empty() {
        assert!(parse_sql("").is_err());
        assert!(parse_sql("  ").is_err());
    }

    #[test]
    fn test_parse_select_yields_query_ast() {
        let stmts = parse_sql("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], DFStatement::Query(_)));
    }
}
