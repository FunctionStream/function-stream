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

use super::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, DropStreamingTableStatement,
    DropTableStatement, ShowCatalogTables, ShowCreateStreamingTable, ShowCreateTable,
    ShowFunctions, ShowStreamingTables, StartFunction, StopFunction, StreamingTableStatement,
};
use crate::coordinator::plan::PlanNode;
use crate::coordinator::statement::Statement;

/// Context passed to StatementVisitor methods
///
/// This enum can be extended in the future to include additional context variants
/// needed by different visitors, such as analysis context, execution context, etc.
#[derive(Debug, Clone, Default)]
pub enum StatementVisitorContext {
    /// Empty context (default)
    #[default]
    Empty,
    // Future: Add more context variants as needed, e.g.:
    // Analyze(AnalyzeContext),
    // Execute(ExecuteContext),
}

impl StatementVisitorContext {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Result returned by StatementVisitor methods
///
/// This enum represents all possible return types from StatementVisitor implementations.
/// Different visitors can return different types, which are wrapped in this enum.
#[derive(Debug)]
pub enum StatementVisitorResult {
    /// Statement (from Analyzer)
    Analyze(Box<dyn Statement>),

    /// Plan node result (from LogicalPlanVisitor)
    Plan(Box<dyn PlanNode>),
    // Future: Add more result variants as needed, e.g.:
    // Execute(ExecuteResult),
}

pub trait StatementVisitor {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_functions(
        &self,
        stmt: &ShowFunctions,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_catalog_tables(
        &self,
        stmt: &ShowCatalogTables,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_create_table(
        &self,
        stmt: &ShowCreateTable,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_drop_table_statement(
        &self,
        stmt: &DropTableStatement,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_streaming_tables(
        &self,
        stmt: &ShowStreamingTables,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_create_streaming_table(
        &self,
        stmt: &ShowCreateStreamingTable,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_drop_streaming_table(
        &self,
        stmt: &DropStreamingTableStatement,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;
}
