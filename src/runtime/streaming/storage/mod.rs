use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub mod backend;
pub mod manager;
pub mod table;

#[async_trait]
pub trait StorageProvider: Send + Sync + 'static {
    async fn get(&self, _path: &str) -> Result<Vec<u8>>;
    async fn put(&self, _path: &str, _data: Vec<u8>) -> Result<()>;
    async fn delete_if_present(&self, _path: &str) -> Result<()>;
}

pub type StorageProviderRef = Arc<dyn StorageProvider>;

/// 空的存储实现，供测试和占位使用
pub struct DummyStorageProvider;

#[async_trait]
impl StorageProvider for DummyStorageProvider {
    async fn get(&self, _path: &str) -> Result<Vec<u8>> {
        Ok(vec![])
    }
    async fn put(&self, _path: &str, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
    async fn delete_if_present(&self, _path: &str) -> Result<()> {
        Ok(())
    }
}
