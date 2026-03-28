use std::fmt::Display;
use thiserror::Error;

/// 流水线 / 子任务运行期间的错误定义。
#[derive(Debug, Error)]
pub enum RunError {
    /// 算子内部业务逻辑抛出的错误
    #[error("Operator execution failed: {0:#}")]
    Operator(#[from] anyhow::Error),

    /// 向下游 Task 发送数据/信号时通道阻塞或断开
    #[error("Downstream send failed: {0}")]
    DownstreamSend(String),

    /// 引擎内部状态机错误或拓扑规划错误（如：DAG 为空、在链条中间发生 Shuffle）
    #[error("Internal engine error: {0}")]
    Internal(String),

    /// Checkpoint 状态持久化或恢复时发生的错误
    #[error("State backend error: {0}")]
    State(String),

    /// 底层网络或文件 I/O 错误
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl RunError {
    /// 快捷构造器：引擎内部错误（常用于防御性编程和边界校验）
    pub fn internal<T: Display>(msg: T) -> Self {
        Self::Internal(msg.to_string())
    }

    /// 快捷构造器：下游发送异常
    pub fn downstream<T: Display>(msg: T) -> Self {
        Self::DownstreamSend(msg.to_string())
    }

    /// 快捷构造器：状态后端异常
    pub fn state<T: Display>(msg: T) -> Self {
        Self::State(msg.to_string())
    }
}