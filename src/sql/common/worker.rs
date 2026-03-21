use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub struct WorkerId(pub u64);

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct MachineId(pub Arc<String>);

impl Display for MachineId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
