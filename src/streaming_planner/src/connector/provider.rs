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

use datafusion::common::{DataFusionError, Result};

use super::config::ConnectorConfig;
use super::sink::runtime_config::SinkRuntimeProperties;
use crate::common::connector_options::ConnectorOptions;
use crate::common::formats::{BadData, Format};

pub trait SourceProvider: Send + Sync {
    fn name(&self) -> &'static str;

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig>;
}

pub trait SinkProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig>;
}

pub fn require_option(
    options: &mut ConnectorOptions,
    key: &str,
    connector_name: &str,
) -> Result<String> {
    options.pull_opt_str(key)?.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Connector '{}' requires option '{}' to be set",
            connector_name, key
        ))
    })
}
