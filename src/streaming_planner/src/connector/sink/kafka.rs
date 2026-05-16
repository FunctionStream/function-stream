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
    DecimalEncodingProto, FormatConfig, JsonFormatConfig, KafkaAuthConfig, KafkaAuthNone,
    KafkaSinkCommitMode, KafkaSinkConfig, RawBytesFormatConfig, RawStringFormatConfig,
    TimestampFormatProto, format_config, kafka_auth_config,
};

use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::{connector_type, kafka_with_value};
use crate::common::formats::{
    DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SinkProvider;
use crate::connector::sink::runtime_config::SinkRuntimeProperties;

pub struct KafkaSinkConnector;

impl KafkaSinkConnector {
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
                "Kafka sink connector: format '{}' is not supported",
                other.name()
            ),
        }
    }
}

impl SinkProvider for KafkaSinkConnector {
    fn name(&self) -> &'static str {
        connector_type::KAFKA
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<SqlFormat>,
        _runtime_props: &SinkRuntimeProperties,
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
                "Kafka sink requires 'format' in the WITH clause (e.g. format = 'json')"
            )
        })?;
        let proto_format = Self::sql_format_to_proto(sql_format)?;

        let value_subject = options.pull_opt_str(opt::KAFKA_VALUE_SUBJECT)?;

        let commit_mode = match options
            .pull_opt_str(opt::KAFKA_SINK_COMMIT_MODE)?
            .as_deref()
        {
            Some(s)
                if s == kafka_with_value::SINK_COMMIT_EXACTLY_ONCE_HYPHEN
                    || s == kafka_with_value::SINK_COMMIT_EXACTLY_ONCE_UNDERSCORE =>
            {
                KafkaSinkCommitMode::KafkaSinkExactlyOnce as i32
            }
            Some(s)
                if s == kafka_with_value::SINK_COMMIT_AT_LEAST_ONCE_HYPHEN
                    || s == kafka_with_value::SINK_COMMIT_AT_LEAST_ONCE_UNDERSCORE =>
            {
                KafkaSinkCommitMode::KafkaSinkAtLeastOnce as i32
            }
            None => KafkaSinkCommitMode::KafkaSinkAtLeastOnce as i32,
            Some(other) => return plan_err!("invalid sink.commit.mode '{other}'"),
        };

        let key_field = match options.pull_opt_str(opt::KAFKA_SINK_KEY_FIELD)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::KAFKA_KEY_FIELD_LEGACY)?,
        };
        let timestamp_field = match options.pull_opt_str(opt::KAFKA_SINK_TIMESTAMP_FIELD)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::KAFKA_TIMESTAMP_FIELD_LEGACY)?,
        };

        let _ = options.pull_opt_str(opt::TYPE)?;
        let _ = options.pull_opt_str(opt::CONNECTOR)?;

        let mut client_configs = options.drain_remaining_string_values()?;
        client_configs.remove(opt::CHECKPOINT_INTERVAL_MS);
        client_configs.remove(opt::PIPELINE_PARALLELISM);
        client_configs.remove(opt::KEY_BY_PARALLELISM);
        client_configs.remove(opt::FORMAT);

        Ok(ConnectorConfig::KafkaSink(KafkaSinkConfig {
            topic,
            bootstrap_servers,
            commit_mode,
            key_field,
            timestamp_field,
            auth: Some(KafkaAuthConfig {
                auth: Some(kafka_auth_config::Auth::None(KafkaAuthNone {})),
            }),
            client_configs,
            format: Some(proto_format),
            value_subject,
        }))
    }
}
