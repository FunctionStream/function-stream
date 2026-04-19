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

//! Stream table catalog: protobuf persistence, MVCC-style planning snapshots for the coordinator.

mod codec;
mod manager;
mod meta_store;
mod rocksdb_meta_store;

#[allow(unused_imports)]
pub use manager::{
    CatalogManager, StoredStreamingJob, initialize_stream_catalog,
    materialize_kafka_source_checkpoints_from_catalog, restore_global_catalog_from_store,
    restore_streaming_jobs_from_store,
};
pub use meta_store::{InMemoryMetaStore, MetaStore};
pub use rocksdb_meta_store::RocksDbMetaStore;
