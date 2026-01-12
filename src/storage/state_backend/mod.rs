// State Backend module
//
// 状态存储后端模块，提供状态存储后端的抽象接口和实现

/// 状态存储目录名称
pub const STATE_DIR_NAME: &str = "state";

pub mod error;
pub mod factory;
pub mod key_builder;
pub mod memory_factory;
pub mod memory_store;
pub mod rocksdb_factory;
pub mod rocksdb_store;
pub mod server;
pub mod store;

pub use factory::StateStoreFactory;
pub use server::StateStorageServer;
pub use store::{StateIterator, StateStore};
