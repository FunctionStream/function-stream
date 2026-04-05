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

use std::time::Duration;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

use super::column_descriptor::ColumnDescriptor;
use crate::sql::common::constants::sql_field;

/// Event-time and watermark configuration for streaming tables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TemporalPipelineConfig {
    pub event_column: Option<String>,
    pub watermark_strategy_column: Option<String>,
    pub liveness_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct TemporalSpec {
    pub time_field: String,
    pub watermark_expr: Option<Expr>,
}

pub fn resolve_temporal_logic(
    columns: &[ColumnDescriptor],
    time_meta: Option<TemporalSpec>,
) -> Result<TemporalPipelineConfig> {
    let mut config = TemporalPipelineConfig::default();

    if let Some(meta) = time_meta {
        let field_exists = columns
            .iter()
            .any(|c| c.arrow_field().name() == meta.time_field.as_str());
        if !field_exists {
            return plan_err!("Temporal field {} does not exist", meta.time_field);
        }
        config.event_column = Some(meta.time_field.clone());

        if meta.watermark_expr.is_some() {
            config.watermark_strategy_column = Some(sql_field::COMPUTED_WATERMARK.to_string());
        } else {
            config.watermark_strategy_column = Some(meta.time_field);
        }
    }

    Ok(config)
}
