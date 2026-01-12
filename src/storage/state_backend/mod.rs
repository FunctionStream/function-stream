// State Backend module
// 
// 状态存储后端模块，提供状态存储后端的抽象接口和实现

/// 状态存储目录名称
pub const STATE_DIR_NAME: &str = "state";

pub mod error;
pub mod factory;
pub mod store;
pub mod memory_store;
pub mod memory_factory;
pub mod rocksdb_store;
pub mod rocksdb_factory;
pub mod key_builder;
pub mod server;

pub use error::BackendError;
pub use factory::{StateStoreFactory, FactoryType, get_factory_for_task};
pub use store::{StateStore, StateIterator};
pub use memory_store::MemoryStateStore;
pub use memory_factory::MemoryStateStoreFactory;
pub use rocksdb_store::RocksDBStateStore;
pub use rocksdb_factory::{RocksDBStateStoreFactory, RocksDBConfig};
pub use key_builder::{build_key, increment_key, is_all_0xff};
pub use server::StateStorageServer;


