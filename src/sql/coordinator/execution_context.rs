use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static EXECUTION_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);

/// 执行上下文
///
/// 包含任务执行所需的所有上下文信息
#[derive(Debug)]
pub struct ExecutionContext {
    /// 全局唯一的执行 ID
    pub execution_id: u64,

    /// 原始 SQL 语句（可选）
    pub sql: Option<String>,

    pub start_time: Instant,

    /// 执行超时时间
    pub timeout: Duration,
}

impl ExecutionContext {
    /// 创建新的执行上下文
    pub fn new() -> Self {
        Self {
            execution_id: EXECUTION_ID_GENERATOR.fetch_add(1, Ordering::SeqCst),
            sql: None,
            start_time: Instant::now(),
            timeout: Duration::from_secs(30), // 默认 30 秒超时
        }
    }

    /// 使用指定的 SQL 创建执行上下文
    pub fn with_sql(sql: impl Into<String>) -> Self {
        let mut ctx = Self::new();
        ctx.sql = Some(sql.into());
        ctx
    }

    /// 设置超时时间
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// 获取已经过的时间
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// 检查是否超时
    pub fn is_timeout(&self) -> bool {
        self.elapsed() >= self.timeout
    }

    /// 获取剩余超时时间
    pub fn remaining_timeout(&self) -> Duration {
        self.timeout.saturating_sub(self.elapsed())
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}
