mod data_type;
mod df_field;
pub(crate) mod placeholder_udf;
mod stream_schema;
mod window;

use std::time::Duration;

pub use data_type::convert_data_type;
pub use df_field::{
    DFField, fields_with_qualifiers, schema_from_df_fields, schema_from_df_fields_with_metadata,
};
pub(crate) use placeholder_udf::PlaceholderUdf;
pub use stream_schema::StreamSchema;
pub(crate) use window::WindowBehavior;
pub use window::{WindowType, find_window, get_duration};

pub const TIMESTAMP_FIELD: &str = "_timestamp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessingMode {
    Append,
    Update,
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
        }
    }
}

#[derive(Clone)]
pub struct PlanningOptions {
    pub ttl: Duration,
}

impl Default for PlanningOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(24 * 60 * 60),
        }
    }
}
