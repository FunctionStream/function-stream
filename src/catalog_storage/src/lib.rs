//! Persistent catalog storage implementations.
//!
//! The stream catalog manager and task persistence (`stream_catalog/`, `task/`) live in this
//! package and are compiled as part of `function-stream` via `#[path]` in `src/lib.rs` / `src/main.rs`.

pub mod memory;
pub mod rocksdb;

pub use memory::InMemoryMetaStore;
pub use rocksdb::RocksDbMetaStore;
