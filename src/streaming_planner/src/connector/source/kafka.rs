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
    BadDataPolicy, DecimalEncodingProto, FormatConfig, JsonFormatConfig, KafkaAuthConfig,
    KafkaAuthNone, KafkaOffsetMode, KafkaReadMode, KafkaSourceConfig, RawBytesFormatConfig,
    RawStringFormatConfig, TimestampFormatProto, format_config, kafka_auth_config,
};

use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::{connector_type, kafka_with_value};
use crate::common::formats::{
    BadData, DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SourceProvider;

pub struct KafkaSourceConnector;

impl KafkaSourceConnector {
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
                "Kafka source connector: format '{}' is not supported",
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
}

impl SourceProvider for KafkaSourceConnector {
    fn name(&self) -> &'static str {
        connector_type::KAFKA
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<SqlFormat>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig> {
        let bootstrap_servers = match options.pull_opt_str(opt::KAFKA_BOOTSTRAP_SERVERS)? {
            Some(s) => s,
            None => options
                .pull_opt_str(opt::KAFKA_BOOTSTRAP_SERVERS_LEGACY)?
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "Kafka connector requires 'bootstrap.servers' in the WITH clause"
                    )
                })?,
        };

        let topic = options.pull_opt_str(opt::KAFKA_TOPIC)?.ok_or_else(|| {
            plan_datafusion_err!("Kafka connector requires 'topic' in the WITH clause")
        })?;

        let sql_format = format.as_ref().ok_or_else(|| {
            plan_datafusion_err!(
                "Kafka source requires 'format' in the WITH clause (e.g. format = 'json')"
            )
        })?;
        let proto_format = Self::sql_format_to_proto(sql_format)?;

        let rate_limit = options
            .pull_opt_u64(opt::KAFKA_RATE_LIMIT_MESSAGES_PER_SECOND)?
            .map(|v| v.clamp(1, u32::MAX as u64) as u32)
            .unwrap_or(0);

        let value_subject = options.pull_opt_str(opt::KAFKA_VALUE_SUBJECT)?;

        let offset_mode = match options
            .pull_opt_str(opt::KAFKA_SCAN_STARTUP_MODE)?
            .as_deref()
        {
            Some(s) if s == kafka_with_value::SCAN_LATEST => {
                KafkaOffsetMode::KafkaOffsetLatest as i32
            }
            Some(s) if s == kafka_with_value::SCAN_EARLIEST => {
                KafkaOffsetMode::KafkaOffsetEarliest as i32
            }
            Some(s)
                if s == kafka_with_value::SCAN_GROUP_OFFSETS
                    || s == kafka_with_value::SCAN_GROUP =>
            {
                KafkaOffsetMode::KafkaOffsetGroup as i32
            }
            None => KafkaOffsetMode::KafkaOffsetGroup as i32,
            Some(other) => {
                return plan_err!(
                    "invalid scan.startup.mode '{other}'; expected latest, earliest, or group-offsets"
                );
            }
        };

        let read_mode = match options.pull_opt_str(opt::KAFKA_ISOLATION_LEVEL)?.as_deref() {
            Some(s) if s == kafka_with_value::ISOLATION_READ_COMMITTED => {
                KafkaReadMode::KafkaReadCommitted as i32
            }
            Some(s) if s == kafka_with_value::ISOLATION_READ_UNCOMMITTED => {
                KafkaReadMode::KafkaReadUncommitted as i32
            }
            None => KafkaReadMode::KafkaReadDefault as i32,
            Some(other) => return plan_err!("invalid isolation.level '{other}'"),
        };

        let group_id = match options.pull_opt_str(opt::KAFKA_GROUP_ID)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::KAFKA_GROUP_ID_LEGACY)?,
        };
        let group_id_prefix = options.pull_opt_str(opt::KAFKA_GROUP_ID_PREFIX)?;

        let _ = options.pull_opt_str(opt::TYPE)?;
        let _ = options.pull_opt_str(opt::CONNECTOR)?;

        let mut client_configs = options.drain_remaining_string_values()?;
        client_configs.remove(opt::CHECKPOINT_INTERVAL_MS);
        client_configs.remove(opt::PIPELINE_PARALLELISM);
        client_configs.remove(opt::KEY_BY_PARALLELISM);
        client_configs.remove(opt::FORMAT);

        Ok(ConnectorConfig::KafkaSource(KafkaSourceConfig {
            topic,
            bootstrap_servers,
            group_id,
            group_id_prefix,
            offset_mode,
            read_mode,
            auth: Some(KafkaAuthConfig {
                auth: Some(kafka_auth_config::Auth::None(KafkaAuthNone {})),
            }),
            client_configs,
            format: Some(proto_format),
            bad_data_policy: Self::bad_data_to_proto(&bad_data),
            rate_limit_msgs_per_sec: rate_limit,
            value_subject,
        }))
    }
}
