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
use protocol::function_stream_graph::{S3SinkConfig, SinkFormatProto};

use crate::common::Format;
use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::connector_type;
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SinkProvider;
use crate::connector::sink::runtime_config::SinkRuntimeProperties;
use crate::connector::sink::utils::SinkUtils;

pub struct S3SinkConnector;

impl SinkProvider for S3SinkConnector {
    fn name(&self) -> &'static str {
        connector_type::S3
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig> {
        let path = SinkUtils::require_path(options)?;

        let format_proto = SinkUtils::resolve_sink_format(
            format,
            self.name(),
            &[
                SinkFormatProto::SinkFormatCsv,
                SinkFormatProto::SinkFormatParquet,
            ],
        )?;

        let bucket = SinkUtils::require_str(options, opt::S3_BUCKET, self.name())?;
        let region = options
            .pull_opt_str(opt::S3_REGION)?
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = options.pull_opt_str(opt::S3_ENDPOINT)?;
        let access_key_id = options.pull_opt_str(opt::S3_ACCESS_KEY_ID)?;
        let secret_access_key = options.pull_opt_str(opt::S3_SECRET_ACCESS_KEY)?;
        let session_token = options.pull_opt_str(opt::S3_SESSION_TOKEN)?;

        let parquet_compression = SinkUtils::extract_parquet_compression(options)?;
        let extra_properties = options.drain_remaining_string_values()?;

        Ok(ConnectorConfig::S3Sink(S3SinkConfig {
            path,
            format: format_proto,
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            parquet_compression,
            extra_properties,
            runtime_properties: runtime_props.clone(),
        }))
    }
}
