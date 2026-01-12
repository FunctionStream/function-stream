pub mod analyzer;
pub mod analysis;

pub use analyzer::{Analyzer, AnalyzeError};
pub use analysis::Analysis;

pub type AnalyzeResult = Result<Analysis, AnalyzeError>;
