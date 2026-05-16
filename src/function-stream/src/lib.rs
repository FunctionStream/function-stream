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

// Library crate for function-stream

#![allow(dead_code)]

use std::sync::Arc;

use anyhow::Context;

pub use function_stream_config as config;
#[path = "../../coordinator/src/coordinator_body.rs"]
pub mod coordinator;
pub use function_stream_logger as logging;

pub use function_stream_runtime_common::{common, memory};

#[path = "../../streaming_runtime/src/streaming/mod.rs"]
pub mod streaming;

#[path = "../../streaming_runtime/src/util/mod.rs"]
pub mod util;

#[path = "../../wasm_runtime/src/wasm/mod.rs"]
pub mod wasm;

pub use wasm::{input, output, processor};

#[path = "../../wasm_runtime/src/state_backend/mod.rs"]
pub mod state_backend;

#[path = "../../catalog_storage/src/stream_catalog/mod.rs"]
pub mod stream_catalog;

#[path = "../../catalog_storage/src/task/mod.rs"]
pub mod task;

/// Install the process-global [`stream_catalog::CatalogManager`] from configuration.
/// In-memory when `config.stream_catalog.persist` is `false`, otherwise a durable
/// [`stream_catalog::RocksDbMetaStore`] (default path: `{data_dir}/catalog.db`).
pub fn initialize_stream_catalog(config: &crate::config::GlobalConfig) -> anyhow::Result<()> {
    use stream_catalog::{CatalogManager, InMemoryMetaStore, MetaStore, RocksDbMetaStore};

    let store: Arc<dyn MetaStore> = if !config.stream_catalog.persist {
        Arc::new(InMemoryMetaStore::new())
    } else {
        let path = config
            .stream_catalog
            .db_path
            .as_ref()
            .map(|p| crate::config::resolve_path(p))
            .unwrap_or_else(|| crate::config::get_data_dir().join("catalog.db"));

        std::fs::create_dir_all(&path).with_context(|| {
            format!(
                "Failed to create stream catalog RocksDB directory {}",
                path.display()
            )
        })?;

        Arc::new(RocksDbMetaStore::open(&path).with_context(|| {
            format!(
                "Failed to open stream catalog RocksDB at {}",
                path.display()
            )
        })?)
    };

    CatalogManager::init_global(store).context("Stream catalog (CatalogManager) global init failed")
}

#[path = "../../servicer/src/servicer_body.rs"]
pub mod server;
pub use function_stream_streaming_planner as sql;
