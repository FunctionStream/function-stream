use std::fmt;

/// Result type for streaming operators and collectors.
pub type DataflowResult<T> = std::result::Result<T, DataflowError>;

/// Unified error type for streaming dataflow operations.
#[derive(Debug)]
pub enum DataflowError {
    Arrow(arrow_schema::ArrowError),
    DataFusion(datafusion::error::DataFusionError),
    Operator(String),
    State(String),
    Connector(String),
    Internal(String),
}

impl fmt::Display for DataflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataflowError::Arrow(e) => write!(f, "Arrow error: {e}"),
            DataflowError::DataFusion(e) => write!(f, "DataFusion error: {e}"),
            DataflowError::Operator(msg) => write!(f, "Operator error: {msg}"),
            DataflowError::State(msg) => write!(f, "State error: {msg}"),
            DataflowError::Connector(msg) => write!(f, "Connector error: {msg}"),
            DataflowError::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for DataflowError {}

impl DataflowError {
    pub fn with_operator(self, operator_id: impl Into<String>) -> Self {
        let id = operator_id.into();
        match self {
            DataflowError::Operator(m) => DataflowError::Operator(format!("{id}: {m}")),
            other => DataflowError::Operator(format!("{id}: {other}")),
        }
    }
}

impl From<arrow_schema::ArrowError> for DataflowError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        DataflowError::Arrow(e)
    }
}

impl From<datafusion::error::DataFusionError> for DataflowError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        DataflowError::DataFusion(e)
    }
}

/// Macro for creating connector errors.
#[macro_export]
macro_rules! connector_err {
    ($($arg:tt)*) => {
        $crate::sql::common::errors::DataflowError::Connector(format!($($arg)*))
    };
}

/// State-related errors.
#[derive(Debug)]
pub enum StateError {
    KeyNotFound(String),
    SerializationError(String),
    BackendError(String),
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateError::KeyNotFound(key) => write!(f, "Key not found: {key}"),
            StateError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            StateError::BackendError(msg) => write!(f, "State backend error: {msg}"),
        }
    }
}

impl std::error::Error for StateError {}
