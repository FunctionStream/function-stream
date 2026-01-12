use crate::sql::analyze::analysis::Analysis;
use crate::sql::plan::{
    CreateWasmTaskPlan, DropWasmTaskPlan, PlanNode, ShowWasmTasksPlan, StartWasmTaskPlan,
    StopWasmTaskPlan,
};
use crate::sql::statement::{
    CreateWasmTask, DropWasmTask, ShowWasmTasks, StartWasmTask, StatementVisitor,
    StatementVisitorContext, StatementVisitorResult, StopWasmTask,
};

#[derive(Debug, Default)]
pub struct LogicalPlanVisitor;

impl LogicalPlanVisitor {
    pub fn new() -> Self {
        Self
    }

    pub fn visit(&self, analysis: &Analysis) -> Box<dyn PlanNode> {
        let context = StatementVisitorContext::Empty;
        let stmt = analysis.statement();

        let result = stmt.accept(self, &context);

        match result {
            StatementVisitorResult::Plan(plan) => plan,
            _ => panic!("LogicalPlanVisitor should return Plan"),
        }
    }
}

impl StatementVisitor for LogicalPlanVisitor {
    fn visit_create_wasm_task(
        &self,
        stmt: &CreateWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let wasm_path = stmt
            .get_wasm_path()
            .expect("wasm-path should be validated in analyzer");
        let config_path = stmt.get_config_path();
        let extra_props = stmt.get_extra_properties();

        StatementVisitorResult::Plan(Box::new(CreateWasmTaskPlan::new(
            stmt.name.clone(),
            wasm_path,
            config_path,
            extra_props,
        )))
    }

    fn visit_drop_wasm_task(
        &self,
        stmt: &DropWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropWasmTaskPlan::new(stmt.name.clone())))
    }

    fn visit_start_wasm_task(
        &self,
        stmt: &StartWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StartWasmTaskPlan::new(stmt.name.clone())))
    }

    fn visit_stop_wasm_task(
        &self,
        stmt: &StopWasmTask,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StopWasmTaskPlan::new(stmt.name.clone())))
    }

    fn visit_show_wasm_tasks(
        &self,
        _stmt: &ShowWasmTasks,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowWasmTasksPlan::new()))
    }
}
