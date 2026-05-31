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

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use protocol::function_stream_graph::{
    BadDataPolicy, DecimalEncodingProto, FormatConfig, JsonFormatConfig, RawBytesFormatConfig,
    RawStringFormatConfig, RosSourceConfig, RosVersion, TimestampFormatProto, format_config,
};

use crate::common::connector_options::ConnectorOptions;
use crate::common::formats::{
    BadData, DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;

pub(crate) const DEFAULT_ROS_URL: &str = "ws://127.0.0.1:9090";
pub(crate) const DEFAULT_ROS2_URL: &str = "ws://127.0.0.1:9090";
pub(crate) const DEFAULT_ROS_MESSAGE_FIELD: &str = "msg";

pub(crate) fn sql_format_to_proto(fmt: &SqlFormat) -> Result<FormatConfig> {
    match fmt {
        SqlFormat::Json(j) => Ok(FormatConfig {
            format: Some(format_config::Format::Json(JsonFormatConfig {
                timestamp_format: match j.timestamp_format {
                    SqlTimestampFormat::RFC3339 => TimestampFormatProto::TimestampRfc3339 as i32,
                    SqlTimestampFormat::UnixMillis => TimestampFormatProto::TimestampUnixMillis as i32,
                },
                decimal_encoding: match j.decimal_encoding {
                    SqlDecimalEncoding::Number => DecimalEncodingProto::DecimalNumber as i32,
                    SqlDecimalEncoding::String => DecimalEncodingProto::DecimalString as i32,
                    SqlDecimalEncoding::Bytes => DecimalEncodingProto::DecimalBytes as i32,
                },
                include_schema: j.include_schema,
                confluent_schema_registry: j.confluent_schema_registry,
                schema_id: j.schema_id,
                debezium: j.debezium,
                unstructured: j.unstructured,
            })),
        }),
        SqlFormat::RawString(_) => Ok(FormatConfig {
            format: Some(format_config::Format::RawString(RawStringFormatConfig {})),
        }),
        SqlFormat::RawBytes(_) => Ok(FormatConfig {
            format: Some(format_config::Format::RawBytes(RawBytesFormatConfig {})),
        }),
        other => plan_err!(
            "ROS source connector: format '{}' is not supported",
            other.name()
        ),
    }
}

pub(crate) fn bad_data_to_proto(bad: &BadData) -> i32 {
    match bad {
        BadData::Fail {} => BadDataPolicy::BadDataFail as i32,
        BadData::Drop {} => BadDataPolicy::BadDataDrop as i32,
    }
}

pub(crate) fn build_ros_source_config(
    options: &mut ConnectorOptions,
    format: &Option<SqlFormat>,
    bad_data: BadData,
    default_version: RosVersion,
    default_url: &str,
) -> Result<ConnectorConfig> {
    let url = match options.pull_opt_str(opt::ROS_URL)? {
        Some(s) => s,
        None => options
            .pull_opt_str(opt::ROS_URL_LEGACY)?
            .unwrap_or_else(|| default_url.to_string()),
    };

    let topic = match options.pull_opt_str(opt::ROS_TOPIC)? {
        Some(s) => s,
        None => options.pull_opt_str(opt::ROS_TOPIC_LEGACY)?.ok_or_else(|| {
            plan_datafusion_err!("ROS source requires 'topic' (ROS topic name) in the WITH clause")
        })?,
    };

    let message_field = match options.pull_opt_str(opt::ROS_MESSAGE_FIELD)? {
        Some(s) => s,
        None => options
            .pull_opt_str(opt::ROS_MESSAGE_FIELD_LEGACY)?
            .unwrap_or_else(|| DEFAULT_ROS_MESSAGE_FIELD.to_string()),
    };

    let ros_version = match options.pull_opt_str(opt::ROS_VERSION)?.as_deref() {
        Some("ros1" | "1") => RosVersion::Ros1 as i32,
        Some("ros2" | "2") => RosVersion::Ros2 as i32,
        None => default_version as i32,
        Some(other) => {
            return plan_err!("invalid ros.version '{other}'; expected ros1 or ros2");
        }
    };

    let sql_format = format.as_ref().ok_or_else(|| {
        plan_datafusion_err!(
            "ROS source requires 'format' in the WITH clause (e.g. format = 'json')"
        )
    })?;
    let proto_format = sql_format_to_proto(sql_format)?;

    let _ = options.pull_opt_str(opt::TYPE)?;
    let _ = options.pull_opt_str(opt::CONNECTOR)?;

    let mut client_configs = options.drain_remaining_string_values()?;
    client_configs.remove(opt::CHECKPOINT_INTERVAL_MS);
    client_configs.remove(opt::PIPELINE_PARALLELISM);
    client_configs.remove(opt::KEY_BY_PARALLELISM);
    client_configs.remove(opt::FORMAT);

    Ok(ConnectorConfig::RosSource(RosSourceConfig {
        url,
        topic,
        message_field,
        format: Some(proto_format),
        bad_data_policy: bad_data_to_proto(&bad_data),
        client_configs,
        ros_version,
    }))
}
