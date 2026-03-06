// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "incremental-cache")]
use wasmtime::CacheStore;

#[derive(Debug, Clone)]
pub struct WasmCacheConfig {
    pub enabled: bool,
    pub cache_dir: PathBuf,
    pub max_size: u64,
}

static GLOBAL_CACHE_CONFIG: OnceLock<WasmCacheConfig> = OnceLock::new();

pub fn set_cache_config(config: WasmCacheConfig) {
    let _ = GLOBAL_CACHE_CONFIG.set(config);
}

pub fn get_cache_config() -> Option<&'static WasmCacheConfig> {
    GLOBAL_CACHE_CONFIG.get()
}

pub fn is_cache_enabled() -> bool {
    GLOBAL_CACHE_CONFIG
        .get()
        .map(|config| config.enabled)
        .unwrap_or(false)
}

#[cfg(feature = "incremental-cache")]
#[derive(Debug)]
struct CacheEntry {
    size: u64,
    last_accessed: u64,
}

#[cfg(feature = "incremental-cache")]
struct LruCacheState {
    entries: lru::LruCache<Vec<u8>, CacheEntry>,
    total_size: u64,
    max_size: u64,
}

impl std::fmt::Debug for LruCacheState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruCacheState")
            .field("total_size", &self.total_size)
            .field("max_size", &self.max_size)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[cfg(feature = "incremental-cache")]
#[derive(Debug)]
pub struct FileCacheStore {
    cache_dir: PathBuf,
    state: Arc<RwLock<LruCacheState>>,
}

#[cfg(feature = "incremental-cache")]
impl FileCacheStore {
    pub fn new<P: AsRef<Path>>(cache_dir: P, max_size: u64) -> anyhow::Result<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();

        fs::create_dir_all(&cache_dir).map_err(|e| {
            anyhow::anyhow!(
                "Failed to create cache directory {}: {}",
                cache_dir.display(),
                e
            )
        })?;

        let state = Arc::new(RwLock::new(LruCacheState {
            entries: lru::LruCache::unbounded(),
            total_size: 0,
            max_size,
        }));

        let store = Self {
            cache_dir: cache_dir.clone(),
            state,
        };
        store.scan_existing_files()?;

        log::info!(
            "FileCacheStore initialized at: {} (max_size: {} bytes)",
            cache_dir.display(),
            max_size
        );

        Ok(store)
    }

    fn scan_existing_files(&self) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
        let mut total_size = 0u64;

        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file()
                    && path.extension() != Some(std::ffi::OsStr::new("tmp"))
                    && let Ok(metadata) = fs::metadata(&path)
                {
                    let size = metadata.len();
                    let modified = metadata
                        .modified()
                        .unwrap_or_else(|_| SystemTime::now())
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                        && let Some(key) = self.filename_to_key(file_name)
                    {
                        state.entries.put(
                            key,
                            CacheEntry {
                                size,
                                last_accessed: modified,
                            },
                        );
                        total_size += size;
                    }
                }
            }
        }

        state.total_size = total_size;
        log::info!(
            "Scanned existing cache files: {} entries, {} bytes",
            state.entries.len(),
            total_size
        );

        Ok(())
    }

    fn filename_to_key(&self, filename: &str) -> Option<Vec<u8>> {
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::URL_SAFE_NO_PAD.decode(filename).ok()
    }

    fn key_to_filename(&self, key: &[u8]) -> String {
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::URL_SAFE_NO_PAD.encode(key)
    }

    fn get_file_path(&self, key: &[u8]) -> PathBuf {
        let filename = self.key_to_filename(key);
        self.cache_dir.join(filename)
    }

    fn evict_until_under_limit(&self, required_space: u64) {
        let mut files_to_delete = Vec::new();

        {
            let mut state = self.state.write().unwrap();
            let target_size = state.max_size.saturating_sub(required_space);

            while state.total_size > target_size {
                if let Some((key, entry)) = state.entries.pop_lru() {
                    let file_path = self.get_file_path(&key);
                    files_to_delete.push((file_path, entry.size));
                    state.total_size = state.total_size.saturating_sub(entry.size);
                } else {
                    break;
                }
            }
        }

        for (file_path, size) in files_to_delete {
            if fs::remove_file(&file_path).is_ok() {
                log::debug!(
                    "Evicted cache entry: {} ({} bytes)",
                    file_path.display(),
                    size
                );
            } else {
                log::warn!("Failed to remove cache file: {}", file_path.display());
                let mut state = self.state.write().unwrap();
                state.total_size = state.total_size.saturating_add(size);
            }
        }
    }

    fn update_access_time(&self, key: &[u8]) {
        let mut state = self.state.write().unwrap();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_accessed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }
    }
}

#[cfg(feature = "incremental-cache")]
impl CacheStore for FileCacheStore {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let file_path = self.get_file_path(key);

        if !file_path.exists() {
            return None;
        }

        let data = match fs::read(&file_path) {
            Ok(data) => data,
            Err(e) => {
                log::warn!("Failed to read cache file {}: {}", file_path.display(), e);
                return None;
            }
        };

        self.update_access_time(key);
        log::debug!("Cache hit: {} ({} bytes)", file_path.display(), data.len());
        Some(Cow::Owned(data))
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let value_size = value.len() as u64;
        let file_path = self.get_file_path(key);
        let temp_path = file_path.with_extension(".tmp");

        loop {
            let mut state = self.state.write().unwrap();

            if value_size > state.max_size {
                log::warn!(
                    "Cache entry size {} exceeds max cache size {}, skipping",
                    value_size,
                    state.max_size
                );
                return false;
            }

            let existing_size = state.entries.get(key).map(|e| e.size).unwrap_or(0);

            let additional_size = value_size.saturating_sub(existing_size);

            if state.total_size + additional_size <= state.max_size {
                drop(state);
                break;
            }

            drop(state);
            self.evict_until_under_limit(value_size);
        }

        if let Some(parent) = file_path.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            log::warn!(
                "Failed to create cache directory {}: {}",
                parent.display(),
                e
            );
            return false;
        }

        let write_result = fs::File::create(&temp_path)
            .and_then(|mut file| file.write_all(&value))
            .and_then(|_| fs::rename(&temp_path, &file_path));

        match write_result {
            Ok(_) => {
                let mut state = self.state.write().unwrap();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let existing_size_final = state.entries.get(key).map(|e| e.size).unwrap_or(0);

                state.total_size =
                    state.total_size.saturating_sub(existing_size_final) + value_size;
                state.entries.put(
                    key.to_vec(),
                    CacheEntry {
                        size: value_size,
                        last_accessed: now,
                    },
                );

                log::debug!(
                    "Cache insert: {} ({} bytes), total: {} bytes",
                    file_path.display(),
                    value_size,
                    state.total_size
                );
                true
            }
            Err(e) => {
                log::warn!("Failed to write cache file {}: {}", file_path.display(), e);
                let _ = fs::remove_file(&temp_path);
                false
            }
        }
    }
}

#[cfg(feature = "incremental-cache")]
pub fn create_cache_store() -> Option<Arc<FileCacheStore>> {
    if !is_cache_enabled() {
        log::info!("wasm cache is disabled in configuration");
        return None;
    }

    let config = get_cache_config()?;
    match FileCacheStore::new(&config.cache_dir, config.max_size) {
        Ok(store) => {
            log::info!(
                "wasm cache store created at: {} (max_size: {} bytes)",
                config.cache_dir.display(),
                config.max_size
            );
            Some(Arc::new(store))
        }
        Err(e) => {
            log::warn!("Failed to create wasm cache store: {}", e);
            None
        }
    }
}

#[cfg(not(feature = "incremental-cache"))]
pub fn create_cache_store() -> Option<()> {
    None
}
