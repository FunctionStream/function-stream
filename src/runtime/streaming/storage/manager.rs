use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use super::table::TaskInfo;
use super::{DummyStorageProvider, StorageProviderRef};

#[derive(Default)]
pub struct GlobalKeyedView<K, V> {
    data: HashMap<K, V>,
}

impl<K: Eq + std::hash::Hash, V> GlobalKeyedView<K, V> {
    pub async fn insert(&mut self, key: K, value: V) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    pub fn get_all(&self) -> &HashMap<K, V> {
        &self.data
    }
}

#[derive(Default)]
pub struct ExpiringTimeKeyView;

impl ExpiringTimeKeyView {
    pub fn insert(&mut self, _timestamp: SystemTime, _batch: arrow_array::RecordBatch) {}

    pub fn all_batches_for_watermark(
        &self,
        _watermark: Option<SystemTime>,
    ) -> std::iter::Empty<(&SystemTime, &Vec<arrow_array::RecordBatch>)> {
        std::iter::empty()
    }

    pub async fn flush(&mut self, _watermark: Option<SystemTime>) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct KeyTimeView;

impl KeyTimeView {
    pub async fn insert(
        &mut self,
        _batch: arrow_array::RecordBatch,
    ) -> Result<Vec<arrow_array::types::UInt64Type>> {
        Ok(vec![])
    }

    pub fn get_batch(&self, _key: &[u8]) -> Result<Option<arrow_array::RecordBatch>> {
        Ok(None)
    }
}

pub struct BackendWriter {}

pub struct TableManager {
    epoch: u32,
    min_epoch: u32,
    writer: BackendWriter,
    task_info: Arc<TaskInfo>,
    storage: StorageProviderRef,
    caches: HashMap<String, Box<dyn std::any::Any + Send>>,
}

impl TableManager {
    /// 加载状态后端（返回默认的空 Manager）
    pub async fn load(task_info: Arc<TaskInfo>) -> Result<(Self, Option<SystemTime>)> {
        let manager = Self {
            epoch: 1,
            min_epoch: 1,
            writer: BackendWriter {},
            task_info,
            storage: Arc::new(DummyStorageProvider),
            caches: HashMap::new(),
        };
        Ok((manager, None))
    }

    /// 接收到 CheckpointBarrier 时（空操作）
    pub async fn checkpoint(
        &mut self,
        _epoch: u32,
        _watermark: Option<SystemTime>,
        _then_stop: bool,
    ) {
    }

    /// 面向算子的 API：获取全局 Key-Value 表
    pub async fn get_global_keyed_state<
        K: Eq + std::hash::Hash + Send + 'static,
        V: Send + 'static,
    >(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>> {
        if !self.caches.contains_key(table_name) {
            let view: Box<dyn std::any::Any + Send> =
                Box::new(GlobalKeyedView::<K, V> { data: HashMap::new() });
            self.caches.insert(table_name.to_string(), view);
        }

        let cache = self.caches.get_mut(table_name).unwrap();

        let view = cache
            .downcast_mut::<GlobalKeyedView<K, V>>()
            .ok_or_else(|| anyhow!("Table type mismatch for {}", table_name))?;

        Ok(view)
    }

    /// 面向算子的 API：获取带 TTL 的时间键值表
    pub async fn get_expiring_time_key_table(
        &mut self,
        table_name: &str,
        _watermark: Option<SystemTime>,
    ) -> Result<&mut ExpiringTimeKeyView> {
        if !self.caches.contains_key(table_name) {
            let view: Box<dyn std::any::Any + Send> = Box::new(ExpiringTimeKeyView::default());
            self.caches.insert(table_name.to_string(), view);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let view = cache
            .downcast_mut::<ExpiringTimeKeyView>()
            .ok_or_else(|| anyhow!("Table type mismatch for {}", table_name))?;

        Ok(view)
    }

    /// 面向算子的 API：获取标准的 Key-Time 双重映射表
    pub async fn get_key_time_table(
        &mut self,
        table_name: &str,
        _watermark: Option<SystemTime>,
    ) -> Result<&mut KeyTimeView> {
        if !self.caches.contains_key(table_name) {
            let view: Box<dyn std::any::Any + Send> = Box::new(KeyTimeView::default());
            self.caches.insert(table_name.to_string(), view);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let view = cache
            .downcast_mut::<KeyTimeView>()
            .ok_or_else(|| anyhow!("Table type mismatch for {}", table_name))?;

        Ok(view)
    }
}
