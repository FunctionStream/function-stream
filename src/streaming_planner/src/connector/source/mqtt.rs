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
    BadDataPolicy, DecimalEncodingProto, FormatConfig, JsonFormatConfig, MqttSourceConfig,
    RawBytesFormatConfig, RawStringFormatConfig, TimestampFormatProto, format_config,
};

use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::connector_type;
use crate::common::formats::{
    BadData, DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SourceProvider;

const DEFAULT_MQTT_PORT: u32 = 1883;
const DEFAULT_MQTT_QOS: u32 = 1;
const DEFAULT_MQTT_KEEP_ALIVE_SECS: u32 = 60;

pub struct MqttSourceConnector;

impl MqttSourceConnector {
    fn sql_format_to_proto(fmt: &SqlFormat) -> Result<FormatConfig> {
        match fmt {
            SqlFormat::Json(j) => Ok(FormatConfig {
                format: Some(format_config::Format::Json(JsonFormatConfig {
                    timestamp_format: match j.timestamp_format {
                        SqlTimestampFormat::RFC3339 => {
                            TimestampFormatProto::TimestampRfc3339 as i32
                        }
                        SqlTimestampFormat::UnixMillis => {
                            TimestampFormatProto::TimestampUnixMillis as i32
                        }
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
                "MQTT source connector: format '{}' is not supported",
                other.name()
            ),
        }
    }

    fn bad_data_to_proto(bad: &BadData) -> i32 {
        match bad {
            BadData::Fail {} => BadDataPolicy::BadDataFail as i32,
            BadData::Drop {} => BadDataPolicy::BadDataDrop as i32,
        }
    }

    fn pull_host(options: &mut ConnectorOptions) -> Result<String> {
        match options.pull_opt_str(opt::MQTT_HOST)? {
            Some(s) => Ok(s),
            None => options.pull_opt_str(opt::MQTT_HOST_LEGACY)?.ok_or_else(|| {
                plan_datafusion_err!(
                    "MQTT connector requires 'mqtt.host' (or 'host') in the WITH clause"
                )
            }),
        }
    }

    fn pull_port(options: &mut ConnectorOptions) -> Result<u32> {
        if let Some(p) = options.pull_opt_u64(opt::MQTT_PORT)? {
            return Ok(p.clamp(1, u16::MAX as u64) as u32);
        }
        if let Some(p) = options.pull_opt_u64(opt::MQTT_PORT_LEGACY)? {
            return Ok(p.clamp(1, u16::MAX as u64) as u32);
        }
        Ok(DEFAULT_MQTT_PORT)
    }

    fn pull_qos(options: &mut ConnectorOptions) -> Result<u32> {
        let qos = match options.pull_opt_u64(opt::MQTT_QOS)? {
            Some(v) => v,
            None => options
                .pull_opt_u64(opt::MQTT_QOS_LEGACY)?
                .unwrap_or(DEFAULT_MQTT_QOS as u64),
        };
        Ok(qos.clamp(0, 2) as u32)
    }

    fn pull_clean_session(options: &mut ConnectorOptions) -> Result<bool> {
        if let Some(v) = options.pull_opt_bool(opt::MQTT_CLEAN_SESSION)? {
            return Ok(v);
        }
        if let Some(v) = options.pull_opt_bool(opt::MQTT_CLEAN_SESSION_LEGACY)? {
            return Ok(v);
        }
        Ok(true)
    }

    fn pull_keep_alive_secs(options: &mut ConnectorOptions) -> Result<u32> {
        if let Some(v) = options.pull_opt_u64(opt::MQTT_KEEP_ALIVE_SECS)? {
            return Ok(v.max(1) as u32);
        }
        if let Some(v) = options.pull_opt_u64(opt::MQTT_KEEP_ALIVE_SECS_LEGACY)? {
            return Ok(v.max(1) as u32);
        }
        Ok(DEFAULT_MQTT_KEEP_ALIVE_SECS)
    }
}

impl SourceProvider for MqttSourceConnector {
    fn name(&self) -> &'static str {
        connector_type::MQTT
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<SqlFormat>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig> {
        let host = Self::pull_host(options)?;
        let port = Self::pull_port(options)?;

        let topic = options.pull_opt_str(opt::MQTT_TOPIC)?.ok_or_else(|| {
            plan_datafusion_err!("MQTT connector requires 'topic' in the WITH clause")
        })?;

        let sql_format = format.as_ref().ok_or_else(|| {
            plan_datafusion_err!(
                "MQTT source requires 'format' in the WITH clause (e.g. format = 'json')"
            )
        })?;
        let proto_format = Self::sql_format_to_proto(sql_format)?;

        let rate_limit = options
            .pull_opt_u64(opt::MQTT_RATE_LIMIT_MESSAGES_PER_SECOND)?
            .map(|v| v.clamp(0, u32::MAX as u64) as u32)
            .unwrap_or(0);

        let client_id = match options.pull_opt_str(opt::MQTT_CLIENT_ID)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::MQTT_CLIENT_ID_LEGACY)?,
        };

        let username = match options.pull_opt_str(opt::MQTT_USERNAME)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::MQTT_USERNAME_LEGACY)?,
        };

        let password = match options.pull_opt_str(opt::MQTT_PASSWORD)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::MQTT_PASSWORD_LEGACY)?,
        };

        let qos = Self::pull_qos(options)?;
        let clean_session = Self::pull_clean_session(options)?;
        let keep_alive_secs = Self::pull_keep_alive_secs(options)?;

        let _ = options.pull_opt_str(opt::TYPE)?;
        let _ = options.pull_opt_str(opt::CONNECTOR)?;

        let mut client_configs = options.drain_remaining_string_values()?;
        client_configs.remove(opt::CHECKPOINT_INTERVAL_MS);
        client_configs.remove(opt::PIPELINE_PARALLELISM);
        client_configs.remove(opt::KEY_BY_PARALLELISM);
        client_configs.remove(opt::FORMAT);

        Ok(ConnectorConfig::MqttSource(MqttSourceConfig {
            topic,
            host,
            port,
            client_id,
            username,
            password,
            qos,
            clean_session,
            keep_alive_secs,
            format: Some(proto_format),
            bad_data_policy: Self::bad_data_to_proto(&bad_data),
            rate_limit_msgs_per_sec: rate_limit,
            client_configs,
        }))
    }
}
