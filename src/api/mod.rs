//! REST/RPC API types for the FunctionStream system.
//!
//! Adapted from Arroyo's `arroyo-rpc/src/api_types` and utility modules.

pub mod checkpoints;
pub mod connections;
pub mod metrics;
pub mod pipelines;
pub mod public_ids;
pub mod schema_resolver;
pub mod udfs;
pub mod var_str;

use serde::{Deserialize, Serialize};

pub use checkpoints::*;
pub use connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, Connector, FieldType, SchemaDefinition,
    SourceField,
};
pub use metrics::*;
pub use pipelines::*;
pub use udfs::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedCollection<T> {
    pub data: Vec<T>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NonPaginatedCollection<T> {
    pub data: Vec<T>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PaginationQueryParams {
    pub starting_after: Option<String>,
    pub limit: Option<u32>,
}
