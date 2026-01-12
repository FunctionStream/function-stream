use crate::sql::statement::Statement;

#[derive(Debug)]
pub struct Analysis {
    pub statement: Box<dyn Statement>,
}

impl Analysis {
    pub fn new(statement: Box<dyn Statement>) -> Self {
        Self { statement }
    }

    pub fn statement(&self) -> &dyn Statement {
        self.statement.as_ref()
    }
}
