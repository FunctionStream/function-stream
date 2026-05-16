//! Catalog domain types and APIs.

pub mod error;
pub mod meta_store;
pub mod stream_catalog;

pub use error::{CatalogError, CatalogResult};
pub use meta_store::MetaStore;
pub use stream_catalog::{GlobalStreamCatalog, StoredStreamingJob, StreamCatalog};
