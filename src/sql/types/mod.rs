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

mod data_type;
mod df_field;
pub(crate) mod placeholder_udf;
mod stream_schema;
mod window;

use std::time::Duration;

use crate::sql::common::constants::sql_planning_default;

pub use data_type::convert_data_type;
pub use df_field::{
    DFField, fields_with_qualifiers, schema_from_df_fields, schema_from_df_fields_with_metadata,
};
pub(crate) use placeholder_udf::PlaceholderUdf;
pub use stream_schema::StreamSchema;
pub(crate) use window::WindowBehavior;
pub use window::{WindowType, find_window, get_duration};

pub use crate::sql::common::constants::sql_field::TIMESTAMP_FIELD;

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
            default_parallelism: sql_planning_default::DEFAULT_PARALLELISM,
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
            ttl: Duration::from_secs(sql_planning_default::PLANNING_TTL_SECS),
        }
    }
}
