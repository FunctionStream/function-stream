pub mod incremental_aggregate;
pub mod updating_cache;

pub use incremental_aggregate::{IncrementalAggregatingConstructor, IncrementalAggregatingFunc};
pub use updating_cache::{Key, UpdatingCache};
