use super::Analysis;
use crate::sql::coordinator::ExecutionContext;
use crate::sql::statement::{
    CreateWasmTask, DropWasmTask, ShowWasmTasks, StartWasmTask, Statement, StatementVisitor,
    StatementVisitorContext, StatementVisitorResult, StopWasmTask,
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

/// Analyzer 负责语义分析
///
/// 类似 IoTDB 的 Analyzer 设计
pub struct Analyzer<'a> {
    #[allow(dead_code)]
    context: &'a ExecutionContext,
}

impl<'a> Analyzer<'a> {
    pub fn new(context: &'a ExecutionContext) -> Self {
        Self { context }
    }

    /// 分析 Statement，返回 Analysis
    pub fn analyze(&self, stmt: &dyn Statement) -> Result<Analysis, AnalyzeError> {
        let visitor_context = StatementVisitorContext::Empty;
        let analyzed_stmt = match stmt.accept(self, &visitor_context) {
            StatementVisitorResult::Analyze(result) => result,
            _ => return Err(AnalyzeError::new("Analyzer should return Analyze result")),
        };
        Ok(Analysis::new(analyzed_stmt))
    }

    /// 静态方法：分析 Statement
    pub fn analyze_statement(
        stmt: &dyn Statement,
        context: &ExecutionContext,
    ) -> Result<Analysis, AnalyzeError> {
        Analyzer::new(context).analyze(stmt)
    }
}

impl StatementVisitor for Analyzer<'_> {
    fn visit_create_wasm_task(
        &self,
        stmt: &CreateWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        // Note: name is read from config file, not from SQL statement
        // So we don't validate name here - it will be validated when config file is read
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_drop_wasm_task(
        &self,
        stmt: &DropWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_start_wasm_task(
        &self,
        stmt: &StartWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_stop_wasm_task(
        &self,
        stmt: &StopWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }

    fn visit_show_wasm_tasks(
        &self,
        stmt: &ShowWasmTasks,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Analyze(Box::new(stmt.clone()))
    }
}
