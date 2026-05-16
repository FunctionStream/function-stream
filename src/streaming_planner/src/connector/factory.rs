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

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::Result;

use super::config::ConnectorConfig;
use super::registry::REGISTRY;
use super::sink::runtime_config::SinkRuntimeConfig;
use crate::common::connector_options::ConnectorOptions;
use crate::common::formats::{BadData, Format};
use crate::schema::table_role::TableRole;

pub fn build_connector_config(
    connector_name: &str,
    role: TableRole,
    options: &mut ConnectorOptions,
    format: &Option<Format>,
    bad_data: BadData,
) -> Result<ConnectorConfig> {
    let runtime_opts_map = options.snapshot_for_catalog().into_iter().collect();
    let runtime_props =
        SinkRuntimeConfig::from_options_map(&runtime_opts_map)?.to_runtime_properties();
    match role {
        TableRole::Ingestion | TableRole::Reference => REGISTRY
            .get_source(connector_name)?
            .build_source_config(options, format, bad_data),
        TableRole::Egress => {
            REGISTRY
                .get_sink(connector_name)?
                .build_sink_config(options, format, &runtime_props)
        }
    }
}

pub fn build_connector_config_from_options(
    connector_name: &str,
    role: TableRole,
    options: &mut ConnectorOptions,
    format: &Option<Format>,
    bad_data: BadData,
) -> Result<ConnectorConfig> {
    build_connector_config(connector_name, role, options, format, bad_data)
}

pub fn build_connector_config_from_catalog(
    connector_name: &str,
    role: TableRole,
    opts: HashMap<String, String>,
    _physical_schema: &Schema,
) -> Result<ConnectorConfig> {
    let mut options = ConnectorOptions::from_flat_string_map(opts)?;
    let format = Format::from_opts(&mut options)?;
    let bad_data = BadData::from_opts(&mut options)?;
    build_connector_config(connector_name, role, &mut options, &format, bad_data)
}
