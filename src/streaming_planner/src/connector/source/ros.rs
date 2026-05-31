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
use protocol::function_stream_graph::RosVersion;

use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::connector_type;
use crate::common::formats::{BadData, Format};
use crate::connector::provider::SourceProvider;

use super::ros_common::{DEFAULT_ROS_URL, build_ros_source_config};

pub struct RosSourceConnector;

impl SourceProvider for RosSourceConnector {
    fn name(&self) -> &'static str {
        connector_type::ROS
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        bad_data: BadData,
    ) -> Result<crate::connector::config::ConnectorConfig> {
        build_ros_source_config(
            options,
            format,
            bad_data,
            RosVersion::Ros1,
            DEFAULT_ROS_URL,
        )
    }
}
