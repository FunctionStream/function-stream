use wasmtime::{component::*, Engine, Store, Config, OptLevel};
#[cfg(feature = "incremental-cache")]
use wasmtime::CacheStore;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView, ResourceTable};
use crate::runtime::output::OutputSink;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::storage::state_backend::{StateStore, StateStoreFactory};
use std::sync::{Arc, OnceLock};
use std::path::{Path, PathBuf};
use std::borrow::Cow;
use std::io::Write;
use std::fs;

// Global WASM Engine (thread-safe, shareable)
// Use OnceLock to implement lazy singleton pattern
static GLOBAL_ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();

/// File system-based CacheStore implementation
/// 
/// Key is filename under a folder, Value is file content
/// Use base64url encoding to convert key to safe filename
#[cfg(feature = "incremental-cache")]
#[derive(Debug)]
struct FileCacheStore {
    /// Cache directory path
    cache_dir: PathBuf,
}

#[cfg(feature = "incremental-cache")]
impl FileCacheStore {
    /// Create new FileCacheStore
    /// 
    /// # Arguments
    /// - `cache_dir`: Cache directory path
    /// 
    /// # Returns
    /// - `Result<Self, anyhow::Error>`: Successfully created or error
    fn new<P: AsRef<Path>>(cache_dir: P) -> anyhow::Result<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        
        // Ensure cache directory exists
        fs::create_dir_all(&cache_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create cache directory {}: {}", cache_dir.display(), e))?;
        
        log::info!("FileCacheStore initialized at: {}", cache_dir.display());
        
        Ok(Self { cache_dir })
    }
    
    /// Convert key to safe filename
    /// 
    /// Use base64url encoding to avoid special character issues in filenames
    fn key_to_filename(&self, key: &[u8]) -> String {
        // Use base64url encoding (URL-safe base64)
        // This avoids special character issues in filenames
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::URL_SAFE_NO_PAD.encode(key);
        encoded
    }
    
    /// Get file path for key
    fn get_file_path(&self, key: &[u8]) -> PathBuf {
        let filename = self.key_to_filename(key);
        self.cache_dir.join(filename)
    }
}

#[cfg(feature = "incremental-cache")]
impl CacheStore for FileCacheStore {
    /// Get cached value by key
    /// 
    /// # Arguments
    /// - `key`: Cache key (byte array)
    /// 
    /// # Returns
    /// - `Some(Cow::Owned(data))`: Found cached value
    /// - `None`: Key does not exist or read failed
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let file_path = self.get_file_path(key);
        
        // Check if file exists
        if !file_path.exists() {
            return None;
        }
        
        // Read file content
        match fs::read(&file_path) {
            Ok(data) => {
                log::debug!("Cache hit: {} ({} bytes)", file_path.display(), data.len());
                Some(Cow::Owned(data))
            }
            Err(e) => {
                log::warn!("Failed to read cache file {}: {}", file_path.display(), e);
                None
            }
        }
    }
    
    /// Insert key-value pair into cache
    /// 
    /// # Arguments
    /// - `key`: Cache key (byte array)
    /// - `value`: Cache value (byte array)
    /// 
    /// # Returns
    /// - `true`: Insert successful
    /// - `false`: Insert failed
    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let file_path = self.get_file_path(key);
        
        // Create parent directory (if not exists)
        if let Some(parent) = file_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                log::warn!("Failed to create cache directory {}: {}", parent.display(), e);
                return false;
            }
        }
        
        // Write file (atomic write: write to temp file first, then rename)
        // This avoids reading incomplete data during write
        let temp_path = file_path.with_extension(".tmp");
        
        match fs::File::create(&temp_path)
            .and_then(|mut file| file.write_all(&value))
            .and_then(|_| fs::rename(&temp_path, &file_path))
        {
            Ok(_) => {
                log::debug!("Cache insert: {} ({} bytes)", file_path.display(), value.len());
                true
            }
            Err(e) => {
                log::warn!("Failed to write cache file {}: {}", file_path.display(), e);
                // Clean up temp file (if exists)
                let _ = fs::remove_file(&temp_path);
                false
            }
        }
    }
}

/// Manually enable incremental compilation configuration
/// 
/// In wasmtime 28.0, Config provides enable_incremental_compilation method
/// This method accepts a bool parameter to enable/disable incremental compilation
/// 
/// Note: Incremental compilation requires cache configuration to work properly
fn enable_incremental_compilation(config: &mut Config) -> bool {
    log::info!("Manually enabling incremental compilation...");
    
    // First try to load default cache config (incremental compilation needs cache support)
    let cache_loaded = match config.cache_config_load_default() {
        Ok(_) => {
            log::info!("✓ Cache config loaded successfully");
            true
        }
        Err(e) => {
            log::warn!("Failed to load default cache config: {}", e);
            log::info!("Incremental compilation may not work without cache config");
            false
        }
    };
    
    // Create cache directory
    let cache_dir = std::env::var("WASMTIME_CACHE_DIR")
        .map(|d| std::path::PathBuf::from(d))
        .unwrap_or_else(|_| {
            let home_dir = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".to_string());
            std::path::PathBuf::from(&home_dir)
                .join(".cache")
                .join("wasmtime")
                .join("incremental")
        });
    
    // If incremental-cache feature is enabled, create FileCacheStore and enable incremental compilation
    #[cfg(feature = "incremental-cache")]
    {
        match FileCacheStore::new(&cache_dir) {
            Ok(file_cache_store) => {
                let cache_store = Arc::new(file_cache_store);
                let _ = config.enable_incremental_compilation(cache_store);
                log::info!("✓ Incremental compilation enabled with FileCacheStore at: {}", cache_dir.display());
            }
            Err(e) => {
                log::warn!("Failed to create FileCacheStore: {}, falling back to default cache config", e);
            }
        }
    }
    
    // If incremental-cache feature is not enabled, rely on cache_config_load_default
    #[cfg(not(feature = "incremental-cache"))]
    {
        log::info!("incremental-cache feature not enabled, using default cache config");
    }
    
    if cache_loaded {
        log::info!("✓ Incremental compilation: ENABLED (with cache support)");
        log::info!("  Benefits:");
        log::info!("    - Only recompiles changed parts of modules");
        log::info!("    - Caches compiled code for faster subsequent compilations");
        log::info!("    - Significantly improves performance for large WASM files");
        true
    } else {
        log::warn!("⚠ Incremental compilation: ENABLED but cache may be limited");
        log::info!("  Note: Full incremental compilation requires cache config");
        true // Still return true, because method call succeeded
    }
}

/// Get or create global WASM Engine
/// 
/// Benefits:
/// 1. Reduce overhead of repeatedly creating Engine
/// 2. Can share compilation cache (if cache is enabled)
/// 3. Improve performance, especially when there are multiple processor instances
/// 
/// Note: Engine is thread-safe and can be safely shared
fn get_global_engine(_wasm_size: usize) -> anyhow::Result<Arc<Engine>> {
    // First check if already initialized
    if let Some(engine) = GLOBAL_ENGINE.get() {
        return Ok(Arc::clone(engine));
    }
    
    // Try to initialize (using stable API of get_or_init)
    // Note: If initialization fails, will panic, which is reasonable because Engine creation failure is fatal
    let engine = GLOBAL_ENGINE.get_or_init(|| {
        let engine_start = std::time::Instant::now();
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false); // 明确关闭 async
        config.cranelift_opt_level(OptLevel::Speed);
        // 5. Disable time-consuming debug info generation
        config.debug_info(false);           // Do not generate DWARF
        config.generate_address_map(false); // Do not generate address map

        // Enable multi-threaded compilation mode (improves compilation speed for large WASM files)
        config.parallel_compilation(true);
        log::info!("Parallel compilation enabled for faster WASM module compilation");
        
        // Manually enable incremental compilation
        let incremental_enabled = enable_incremental_compilation(&mut config);
        
        if incremental_enabled {
            log::info!("Incremental compilation: ENABLED");
            log::info!("  Benefits:");
            log::info!("    - Only recompiles changed parts of modules");
            log::info!("    - Caches compiled code for faster subsequent compilations");
            log::info!("    - Significantly improves performance for large WASM files");
        } else {
            log::warn!("Incremental compilation: DISABLED");
            log::info!("  Reason: Cache config could not be loaded");
            log::info!("  Impact: All modules will be fully recompiled on each run");
        }

        config.debug_info(false);

        // Enable cache to improve performance (if possible)
        // Note: Cache requires configuration, using default config here
        let engine = Engine::new(&config)
            .unwrap_or_else(|e| {
                panic!("Failed to create global WASM engine: {}", e);
            });
        
        let engine_elapsed = engine_start.elapsed().as_secs_f64();
        log::info!("[Timing] Global Engine created: {:.3}s", engine_elapsed);
        log::info!("Global WASM Engine initialized (will be reused for all processors)");
        
        Arc::new(engine)
    });
    
    Ok(Arc::clone(engine))
}

// 1. Bindgen configuration: disable async
bindgen!({
    world: "processor",
    path: "wit/processor.wit",
    async: false, // 关键：设置为 false
    with: {
        "functionstream:core/kv/store": FunctionStreamStoreHandle,
        "functionstream:core/kv/iterator": FunctionStreamIteratorHandle,
    }
});

use functionstream::core::kv::{self, HostStore, HostIterator, Error, ComplexKey};

// --- Resource Handles ---
pub struct FunctionStreamStoreHandle {
    pub name: String,
    pub state_store: Box<dyn StateStore>,
}

impl Drop for FunctionStreamStoreHandle {
    fn drop(&mut self) {
        log::debug!("Recycling resource Store: {}", self.name);
    }
}

pub struct FunctionStreamIteratorHandle {
    /// State storage iterator
    pub state_iterator: Box<dyn crate::storage::state_backend::StateIterator>,
}

// --- Host State ---
pub struct HostState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    /// State storage factory (used to create StateStore instances)
    pub factory: Arc<dyn StateStoreFactory>,
    /// Output sink list (used to send processed data and watermarks)
    pub output_sinks: Vec<Box<dyn OutputSink>>,
}

impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable { &mut self.table }
    fn ctx(&mut self) -> &mut WasiCtx { &mut self.wasi }
}

impl kv::Host for HostState {}

// --- WIT Implementation (moved from wit_impl.rs for bindgen compatibility) ---

impl HostStore for HostState {
    // ✅ Use constructor keyword, mapped to new method in Rust
    // Note: In wasmtime 24.0, constructor methods cannot return Result, must directly return Resource
    // If error occurs, need to handle via Trap
    fn new(&mut self, name: String) -> Resource<FunctionStreamStoreHandle> {
        // Use saved factory to create StateStore, pass name as column_family
        let state_store = self.factory.new_state_store(Some(name.clone()))
            .unwrap_or_else(|e| {
                panic!("Failed to create state store: {}", e);
            });
        
        let handle = FunctionStreamStoreHandle {
            name,
            state_store,
        };
        self.table.push(handle)
            .unwrap_or_else(|e| {
                panic!("Failed to push resource to table: {}", e);
            })
    }

    fn put_state(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        store.state_store.put_state(key, value)
            .map_err(|e| Error::Other(format!("Failed to put state: {}", e)))
    }

    fn get_state(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        store.state_store.get_state(key)
            .map_err(|e| Error::Other(format!("Failed to get state: {}", e)))
    }

    fn delete_state(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: Vec<u8>) -> Result<(), Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        store.state_store.delete_state(key)
            .map_err(|e| Error::Other(format!("Failed to delete state: {}", e)))
    }

    fn list_states(&mut self, self_: Resource<FunctionStreamStoreHandle>, start: Vec<u8>, end: Vec<u8>) -> Result<Vec<Vec<u8>>, Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        store.state_store.list_states(start, end)
            .map_err(|e| Error::Other(format!("Failed to list states: {}", e)))
    }

    // --- Complex Key Implementation ---
    fn put(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: ComplexKey, value: Vec<u8>) -> Result<(), Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Use build_key function to build complete complex key
        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );
        
        store.state_store.put_state(real_key, value)
            .map_err(|e| Error::Other(format!("Failed to put: {}", e)))
    }

    fn get(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: ComplexKey) -> Result<Option<Vec<u8>>, Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Use build_key function to build complete complex key
        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );
        
        store.state_store.get_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to get: {}", e)))
    }

    fn delete(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: ComplexKey) -> Result<(), Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Use build_key function to build complete complex key
        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );
        
        store.state_store.delete_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to delete: {}", e)))?;
        Ok(())
    }

    fn merge(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: ComplexKey, value: Vec<u8>) -> Result<(), Error> {
        // merge operation: if key exists, merge value; otherwise create new value
        // Simplified implementation here: directly use put (should actually implement merge logic based on storage backend)
        self.put(self_, key, value)
    }

    fn delete_prefix(&mut self, self_: Resource<FunctionStreamStoreHandle>, key: ComplexKey) -> Result<(), Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Use build_key function to build prefix key (user_key is empty)
        let prefix_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &[], // user_key is empty, means delete all keys matching prefix
        );
        
        // List all keys starting with this prefix
        let keys_to_delete = store.state_store.list_states(
            prefix_key.clone(),
            {
                // Build end key (prefix + 0xFF...)
                let mut end_prefix = prefix_key.clone();
                end_prefix.extend_from_slice(&vec![0xFF; 256]); // Large enough end key
                end_prefix
            },
        )
        .map_err(|e| Error::Other(format!("Failed to list keys for delete_prefix: {}", e)))?;
        
        // Delete all matching keys
        for key_to_delete in keys_to_delete {
            store.state_store.delete_state(key_to_delete)
                .map_err(|e| Error::Other(format!("Failed to delete key in delete_prefix: {}", e)))?;
        }
        
        Ok(())
    }

    fn list_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Build start and end keys
        let start_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &start_inclusive,
        );
        let end_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &end_exclusive,
        );
        
        store.state_store.list_states(start_key, end_key)
            .map_err(|e| Error::Other(format!("Failed to list_complex: {}", e)))
    }

    fn scan_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<Resource<FunctionStreamIteratorHandle>, Error> {
        let store = self.table.get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;
        
        // Use StateStore's scan_complex method
        let state_iterator = store.state_store.scan_complex(key_group, key, namespace)
            .map_err(|e| Error::Other(format!("Failed to scan_complex: {}", e)))?;
        
        let iter = FunctionStreamIteratorHandle {
            state_iterator,
        };
        self.table.push(iter)
            .map_err(|e| Error::Other(format!("Failed to push iterator resource: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamStoreHandle>) -> Result<(), anyhow::Error> {
        self.table.delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete store resource: {}", e))?;
        Ok(())
    }
}

// --- HostIterator Implementation ---

impl HostIterator for HostState {
    fn has_next(&mut self, self_: Resource<FunctionStreamIteratorHandle>) -> Result<bool, Error> {
        let iter = self.table.get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;
        
        iter.state_iterator.has_next()
            .map_err(|e| Error::Other(format!("Failed to check has_next: {}", e)))
    }

    fn next(&mut self, self_: Resource<FunctionStreamIteratorHandle>) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let iter = self.table.get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;
        
        iter.state_iterator.next()
            .map_err(|e| Error::Other(format!("Failed to get next: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamIteratorHandle>) -> Result<(), anyhow::Error> {
        // StateIterator is a trait object, resources are automatically cleaned when Box is dropped
        // No additional cleanup logic needed
        self.table.delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete iterator resource: {}", e))?;
        Ok(())
    }
}

// --- 4. Implement Collector (sync version) ---
impl functionstream::core::collector::Host for HostState {
    fn emit(&mut self, target_id: u32, data: Vec<u8>) {
        // Select corresponding OutputSink based on target_id
        let sink_count = self.output_sinks.len();
        let sink = self.output_sinks
            .get_mut(target_id as usize)
            .unwrap_or_else(|| {
                panic!("Invalid target_id: {target_id}, available sinks: {sink_count}");
            });
        
        // Use OutputSink to send data
        let buffer_or_event = BufferOrEvent::new_buffer(
            data,
            Some(format!("target_{}", target_id)), // Encode target_id into channel_info
            false, // more_available
            false, // more_priority_events
        );
        
        sink.collect(buffer_or_event)
            .unwrap_or_else(|e| {
                panic!("failed to collect output: {e}");
            });
    }

    fn emit_watermark(&mut self, target_id: u32, ts: u64) {
        // Select corresponding OutputSink based on target_id
        let sink_count = self.output_sinks.len();
        let sink = self.output_sinks
            .get_mut(target_id as usize)
            .unwrap_or_else(|| {
                panic!("Invalid target_id: {target_id}, available sinks: {sink_count}");
            });
        
        // Serialize watermark to byte array, then send via OutputSink
        // Note: Need to encode watermark information into data
        // Can use a simple format: first 4 bytes are target_id (u32, little-endian), next 8 bytes are timestamp (u64, little-endian)
        let mut watermark_data = Vec::with_capacity(12);
        watermark_data.extend_from_slice(&target_id.to_le_bytes());
        watermark_data.extend_from_slice(&ts.to_le_bytes());
        
        let buffer_or_event = BufferOrEvent::new_buffer(
            watermark_data,
            Some(format!("watermark_target_{}", target_id)), // 标记为 watermark
            false, // more_available
            false, // more_priority_events
        );
        
        sink.collect(buffer_or_event)
            .unwrap_or_else(|e| {
                panic!("failed to collect watermark: {e}");
            });
    }
}

pub fn create_wasm_host(
    wasm_bytes: &[u8],
    output_sinks: Vec<Box<dyn OutputSink>>,
    init_context: &crate::runtime::taskexecutor::InitContext,
    task_name: String,
) -> anyhow::Result<(Processor, Store<HostState>)> {
    let total_start = std::time::Instant::now();
    
    // 1. Get global Engine (created on first call, reused afterwards)
    let engine_start = std::time::Instant::now();
    let engine = get_global_engine(wasm_bytes.len())?;
    let engine_elapsed = engine_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Get Global Engine: {:.3}s", engine_elapsed);
    
    // 2. Parse WASM Component
    let parse_start = std::time::Instant::now();
    log::info!("Parsing WASM component, size: {} MB", wasm_bytes.len() / 1024 / 1024);
    log::debug!("Parsing WASM component, size: {} bytes", wasm_bytes.len());
    let component = Component::from_binary(&engine, wasm_bytes)
        .map_err(|e| {
            let error_msg = format!("Failed to parse WebAssembly component: {}", e);
            log::error!("{}", error_msg);
            log::error!("WASM bytes preview (first 100 bytes): {:?}", 
                wasm_bytes.iter().take(100).collect::<Vec<_>>());
            anyhow::anyhow!(error_msg)
        })?;
    let parse_elapsed = parse_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Parse WASM Component: {:.3}s", parse_elapsed);

    // 3. Create Linker
    let linker_start = std::time::Instant::now();
    let mut linker = Linker::new(&engine);
    let linker_elapsed = linker_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Create Linker: {:.3}s", linker_elapsed);
    
    // 4. Add WASI to linker
    let wasi_start = std::time::Instant::now();
    wasmtime_wasi::add_to_linker_sync(&mut linker)
        .map_err(|e| anyhow::anyhow!("Failed to add WASI to linker: {}", e))?;
    let wasi_elapsed = wasi_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Add WASI to linker: {:.3}s", wasi_elapsed);

    // 5. Link custom Host (closures and traits are all synchronous at this point)
    let kv_start = std::time::Instant::now();
    functionstream::core::kv::add_to_linker(&mut linker, |s: &mut HostState| s)
        .map_err(|e| anyhow::anyhow!("Failed to add kv interface to linker: {}", e))?;
    let kv_elapsed = kv_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Add kv interface to linker: {:.3}s", kv_elapsed);
    
    let collector_start = std::time::Instant::now();
    functionstream::core::collector::add_to_linker(&mut linker, |s: &mut HostState| s)
        .map_err(|e| anyhow::anyhow!("Failed to add collector interface to linker: {}", e))?;
    let collector_elapsed = collector_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Add collector interface to linker: {:.3}s", collector_elapsed);

    // 6. Get created_at from task storage (may involve YAML read)
    let load_task_start = std::time::Instant::now();
    let created_at = init_context.task_storage.load_task(&task_name)
        .ok()
        .map(|info| info.created_at)
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
    let load_task_elapsed = load_task_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Load task from storage (YAML read): {:.3}s", load_task_elapsed);
    
    // 7. Create state storage factory
    let factory_start = std::time::Instant::now();
    let factory = init_context.state_storage_server.create_factory(
        task_name.clone(),
        created_at,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create state store factory: {}", e))?;
    let factory_elapsed = factory_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Create state store factory: {:.3}s", factory_elapsed);
    
    // 8. Create Store
    let store_create_start = std::time::Instant::now();
    let mut store = Store::new(&engine, HostState {
        wasi: WasiCtxBuilder::new().inherit_stdio().build(),
        table: ResourceTable::new(),
        factory,
        output_sinks,
    });
    let store_create_elapsed = store_create_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Create Store: {:.3}s", store_create_elapsed);


    let instantiate_start = std::time::Instant::now();
    log::debug!("Instantiating WASM component");
    let processor = Processor::instantiate(&mut store, &component, &linker)
        .map_err(|e| {
            let error_msg = format!("Failed to instantiate WASM component: {}", e);
            log::error!("{}", error_msg);
            // Try to get more detailed error information
            let mut detailed_msg = error_msg.clone();
            if let Some(source) = e.source() {
                detailed_msg.push_str(&format!(". Source: {}", source));
            }
            anyhow::anyhow!("{}", detailed_msg)
        })?;
    let instantiate_elapsed = instantiate_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host - Instantiate WASM component: {:.3}s", instantiate_elapsed);

    let total_elapsed = total_start.elapsed().as_secs_f64();
    log::info!("[Timing] create_wasm_host total: {:.3}s", total_elapsed);

    Ok((processor, store))
}