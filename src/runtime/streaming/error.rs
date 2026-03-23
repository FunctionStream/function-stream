use thiserror::Error;

/// 子任务 / 源任务运行中的错误。
#[derive(Debug, Error)]
pub enum RunError {
    #[error("operator error: {0:#}")]
    Operator(#[from] anyhow::Error),
    #[error("downstream send: {0}")]
    DownstreamSend(String),
}
