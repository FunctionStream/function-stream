// State Backend Error - 状态存储后端错误类型
//
// 定义状态存储后端操作中可能出现的错误

/// 状态存储后端错误
#[derive(Debug, Clone)]
pub enum BackendError {
    /// 键不存在
    KeyNotFound(String),
    /// IO 错误
    IoError(String),
    /// 序列化错误
    SerializationError(String),
    /// 其他错误
    Other(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::KeyNotFound(key) => write!(f, "Key not found: {}", key),
            BackendError::IoError(msg) => write!(f, "IO error: {}", msg),
            BackendError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            BackendError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for BackendError {}

