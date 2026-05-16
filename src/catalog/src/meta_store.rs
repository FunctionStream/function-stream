use crate::CatalogResult;

/// Synchronous metadata key-value backend for catalog records.
pub trait MetaStore: Send + Sync {
    fn put(&self, key: &str, value: Vec<u8>) -> CatalogResult<()>;
    fn get(&self, key: &str) -> CatalogResult<Option<Vec<u8>>>;
    fn delete(&self, key: &str) -> CatalogResult<()>;
    fn scan_prefix(&self, prefix: &str) -> CatalogResult<Vec<(String, Vec<u8>)>>;

    /// Atomic apply of many puts (`Some(value)`) and deletes (`None`).
    ///
    /// Backends should override this with a single transaction or write batch
    /// when the storage engine supports it.
    fn write_batch(&self, batch: Vec<(String, Option<Vec<u8>>)>) -> CatalogResult<()> {
        for (key, value) in batch {
            match value {
                Some(value) => self.put(&key, value)?,
                None => self.delete(&key)?,
            }
        }
        Ok(())
    }
}
