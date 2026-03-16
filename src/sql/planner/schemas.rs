// Re-export schema utilities from catalog::utils.
// Kept for backward compatibility with existing planner imports.
pub use crate::sql::catalog::utils::{
    add_timestamp_field, add_timestamp_field_arrow, has_timestamp_field, window_arrow_struct,
};
