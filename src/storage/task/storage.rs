// Task Storage - 任务存储接口
//
// 定义任务信息存储的接口

use crate::runtime::common::ComponentState;
use anyhow::Result;

/// 存储的任务信息（仅包含存储的字段）
#[derive(Debug, Clone)]
pub struct StoredTaskInfo {
    /// 任务名称
    pub name: String,
    /// WASM 字节数组（可能为 None）
    pub wasm_bytes: Option<Vec<u8>>,
    /// 配置字节数组
    pub config_bytes: Vec<u8>,
    /// 运行时状态
    pub state: ComponentState,
    /// 第一次创建时间（Unix 时间戳）
    pub created_at: u64,
    /// 检查点 ID（可能为 None，表示还没有检查点）
    pub checkpoint_id: Option<u64>,
}

/// 任务存储接口
pub trait TaskStorage: Send + Sync {
    /// 创建任务信息
    ///
    /// # 参数
    /// - `task_info`: 任务信息
    ///
    /// # 返回值
    /// - `Ok(())`: 创建成功
    /// - `Err(...)`: 创建失败
    fn create_task(&self, task_info: &StoredTaskInfo) -> Result<()>;

    /// 更新任务状态（使用 merge 操作）
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    /// - `new_state`: 新状态
    ///
    /// # 返回值
    /// - `Ok(())`: 更新成功
    /// - `Err(...)`: 更新失败
    fn update_task_state(&self, task_name: &str, new_state: ComponentState) -> Result<()>;

    /// 更新任务检查点 ID（使用 merge 操作）
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    /// - `checkpoint_id`: 新的检查点 ID
    ///
    /// # 返回值
    /// - `Ok(())`: 更新成功
    /// - `Err(...)`: 更新失败
    fn update_task_checkpoint_id(&self, task_name: &str, checkpoint_id: Option<u64>) -> Result<()>;

    /// 删除任务信息
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    ///
    /// # 返回值
    /// - `Ok(())`: 删除成功
    /// - `Err(...)`: 删除失败
    fn delete_task(&self, task_name: &str) -> Result<()>;

    /// 加载任务信息
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    ///
    /// # 返回值
    /// - `Ok(TaskInfo)`: 加载成功
    /// - `Err(...)`: 加载失败（任务不存在等）
    fn load_task(&self, task_name: &str) -> Result<StoredTaskInfo>;

    /// 检查任务是否存在
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    ///
    /// # 返回值
    /// - `Ok(true)`: 任务存在
    /// - `Ok(false)`: 任务不存在
    /// - `Err(...)`: 检查失败
    fn task_exists(&self, task_name: &str) -> Result<bool>;
}
