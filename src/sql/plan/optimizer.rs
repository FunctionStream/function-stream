use crate::sql::analyze::Analysis;
use crate::sql::plan::PlanNode;
use std::fmt;

pub trait PlanOptimizer: fmt::Debug + Send + Sync {
    fn optimize(&self, plan: Box<dyn PlanNode>, analysis: &Analysis) -> Box<dyn PlanNode>;
    
    fn name(&self) -> &str;
}

#[derive(Debug, Default)]
pub struct NoOpOptimizer;

impl PlanOptimizer for NoOpOptimizer {
    fn optimize(&self, plan: Box<dyn PlanNode>, _analysis: &Analysis) -> Box<dyn PlanNode> {
        plan
    }
    
    fn name(&self) -> &str {
        "NoOpOptimizer"
    }
}

#[derive(Debug)]
pub struct LogicalPlanner {
    optimizers: Vec<Box<dyn PlanOptimizer>>,
}

impl LogicalPlanner {
    pub fn new() -> Self {
        Self {
            optimizers: Vec::new(),
        }
    }
    
    pub fn with_optimizers(optimizers: Vec<Box<dyn PlanOptimizer>>) -> Self {
        Self { optimizers }
    }
    
    pub fn add_optimizer(&mut self, optimizer: Box<dyn PlanOptimizer>) {
        self.optimizers.push(optimizer);
    }
    
    pub fn optimize(&self, plan: Box<dyn PlanNode>, analysis: &Analysis) -> Box<dyn PlanNode> {
        let mut optimized_plan = plan;
        
        for optimizer in &self.optimizers {
            log::debug!("Applying optimizer: {}", optimizer.name());
            optimized_plan = optimizer.optimize(optimized_plan, analysis);
        }
        
        optimized_plan
    }
}

impl Default for LogicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

