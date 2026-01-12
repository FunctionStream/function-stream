// Storage module
// 
// 提供存储功能，包括状态存储后端和任务存储

pub mod state_backend;
pub mod task;

pub use state_backend::{BackendError, StateStoreFactory, MemoryStateStoreFactory, RocksDBStateStoreFactory};
pub use task::{TaskStorage, StoredTaskInfo, TaskStorageFactory};

