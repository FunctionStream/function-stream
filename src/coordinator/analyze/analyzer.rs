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

use super::Analysis;
use crate::coordinator::execution_context::ExecutionContext;
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction,
    DropStreamingTableStatement, DropTableStatement, ShowCatalogTables,
    ShowCreateStreamingTable, ShowCreateTable, ShowFunctions, ShowStreamingTables,
    StartFunction, Statement, StatementVisitor, StatementVisitorContext,
    StatementVisitorResult, StopFunction, StreamingTableStatement,
};
use std::fmt;

#[derive(Debug, Clone)]
pub struct AnalyzeError {
    pub message: String,
}

impl AnalyzeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AnalyzeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Analyze error: {}", self.message)
    }
}

impl std::error::Error for AnalyzeError {}

/// Analyzer performs semantic analysis
pub struct Analyzer<'a> {
    #[allow(dead_code)]
    context: &'a ExecutionContext,
}

impl<'a> Analyzer<'a> {
    pub fn new(context: &'a ExecutionContext) -> Self {
        Self { context }
    }

    /// Analyze Statement and return Analysis
    pub fn analyze(&self, stmt: &dyn Statement) -> Result<Analysis, AnalyzeError> {
        let visitor_context = StatementVisitorContext::Empty;
        let analyzed_stmt = match stmt.accept(self, &visitor_context) {
            StatementVisitorResult::Analyze(result) => result,
            _ => return Err(AnalyzeError::new("Analyzer should return Analyze result")),
        };
        Ok(Analysis::new(analyzed_stmt))
    }
}

impl StatementVisitor for Analyzer<'_> {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        // Function source is already validated during parsing (from_properties)
        // So we just need to check if it exists
        let _function_source = stmt.get_function_source();

        // Note: name is read from config file, not from SQL statement
        // So we don't validate name here - it will be validated when config file is read
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_show_functions(
        &self,
        stmt: &ShowFunctions,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_show_catalog_tables(
        &self,
        stmt: &ShowCatalogTables,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_show_create_table(
        &self,
        stmt: &ShowCreateTable,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(CreateTable::new(stmt.statement.clone())))
    }

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(StreamingTableStatement::new(
            stmt.statement.clone(),
        )))
    }

    fn visit_drop_table_statement(
        &self,
        stmt: &DropTableStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(DropTableStatement::new(stmt.statement.clone())))
    }

    fn visit_show_streaming_tables(
        &self,
        stmt: &ShowStreamingTables,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_show_create_streaming_table(
        &self,
        stmt: &ShowCreateStreamingTable,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_drop_streaming_table(
        &self,
        stmt: &DropStreamingTableStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }
}
