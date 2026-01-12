pub mod analysis;
pub mod analyzer;

pub use analysis::Analysis;
pub use analyzer::{AnalyzeError, Analyzer};

pub type AnalyzeResult = Result<Analysis, AnalyzeError>;
