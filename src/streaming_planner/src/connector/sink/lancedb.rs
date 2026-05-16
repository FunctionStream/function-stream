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

use datafusion::common::Result;
use protocol::function_stream_graph::{LanceDbSinkConfig, SinkFormatProto};

use crate::common::Format;
use crate::common::connector_options::ConnectorOptions;
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SinkProvider;
use crate::connector::sink::runtime_config::SinkRuntimeProperties;
use crate::connector::sink::utils::SinkUtils;

pub struct LanceDbSinkConnector;

impl SinkProvider for LanceDbSinkConnector {
    fn name(&self) -> &'static str {
        "lancedb"
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        _format: &Option<Format>,
        runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig> {
        let path = SinkUtils::require_path(options)?;

        let s3_bucket = options.pull_opt_str(opt::S3_BUCKET)?;
        let s3_region = options.pull_opt_str(opt::S3_REGION)?;
        let s3_endpoint = options.pull_opt_str(opt::S3_ENDPOINT)?;
        let s3_access_key_id = options.pull_opt_str(opt::S3_ACCESS_KEY_ID)?;
        let s3_secret_access_key = options.pull_opt_str(opt::S3_SECRET_ACCESS_KEY)?;
        let s3_session_token = options.pull_opt_str(opt::S3_SESSION_TOKEN)?;

        let extra_properties = options.drain_remaining_string_values()?;

        Ok(ConnectorConfig::LanceDbSink(LanceDbSinkConfig {
            path,
            format: SinkFormatProto::SinkFormatLance as i32,
            s3_bucket,
            s3_region,
            s3_endpoint,
            s3_access_key_id,
            s3_secret_access_key,
            s3_session_token,
            extra_properties,
            runtime_properties: runtime_props.clone(),
        }))
    }
}
