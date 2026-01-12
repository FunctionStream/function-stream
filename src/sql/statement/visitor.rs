use super::{CreateWasmTask, DropWasmTask, ShowWasmTasks, StartWasmTask, StopWasmTask};
use crate::sql::plan::PlanNode;
use crate::sql::statement::Statement;

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
    fn visit_create_wasm_task(
        &self,
        stmt: &CreateWasmTask,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_drop_wasm_task(
        &self,
        stmt: &DropWasmTask,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_start_wasm_task(
        &self,
        stmt: &StartWasmTask,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_stop_wasm_task(
        &self,
        stmt: &StopWasmTask,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;

    fn visit_show_wasm_tasks(
        &self,
        stmt: &ShowWasmTasks,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;
}
