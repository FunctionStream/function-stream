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

pub fn default_kafka_consumer_group_id(table_name: &str) -> String {
    let sanitized: String = table_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("fs-{sanitized}-consumer")
}

pub fn ensure_default_consumer_group(
    table_name: &str,
    options: &mut ConnectorOptions,
) -> Result<()> {
    let has_group = options.peek_opt_str(opt::KAFKA_GROUP_ID)?.is_some()
        || options.peek_opt_str(opt::KAFKA_GROUP_ID_LEGACY)?.is_some();
    let has_prefix = options.peek_opt_str(opt::KAFKA_GROUP_ID_PREFIX)?.is_some();
    if has_group || has_prefix {
        return Ok(());
    }
    options.insert_str(
        opt::KAFKA_GROUP_ID,
        default_kafka_consumer_group_id(table_name),
    )?;
    Ok(())
}

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
            rate_limit_msgs_per_sec: 0,
            value_subject,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{Expr, Ident, SqlOption, Value as SqlValue};

    fn options_from_map(pairs: &[(&str, &str)]) -> ConnectorOptions {
        let opts: Vec<SqlOption> = pairs
            .iter()
            .map(|(k, v)| SqlOption::KeyValue {
                key: Ident::new(*k),
                value: Expr::Value(
                    SqlValue::SingleQuotedString((*v).to_string()).with_empty_span(),
                ),
            })
            .collect();
        ConnectorOptions::new(&opts, &None).expect("options")
    }

    #[test]
    fn assigns_default_group_id_when_missing() {
        let mut options = options_from_map(&[
            ("connector", "kafka"),
            ("topic", "events"),
            ("bootstrap.servers", "localhost:9092"),
        ]);
        ensure_default_consumer_group("my_events", &mut options).expect("ensure");
        assert_eq!(
            options.peek_opt_str(opt::KAFKA_GROUP_ID).expect("peek"),
            Some("fs-my_events-consumer".to_string())
        );
    }

    #[test]
    fn keeps_user_group_id() {
        let mut options = options_from_map(&[
            ("connector", "kafka"),
            ("topic", "events"),
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "custom-group"),
        ]);
        ensure_default_consumer_group("my_events", &mut options).expect("ensure");
        assert_eq!(
            options.peek_opt_str(opt::KAFKA_GROUP_ID).expect("peek"),
            Some("custom-group".to_string())
        );
    }

    #[test]
    fn skips_when_group_id_prefix_set() {
        let mut options = options_from_map(&[
            ("connector", "kafka"),
            ("topic", "events"),
            ("bootstrap.servers", "localhost:9092"),
            ("group.id.prefix", "prefix"),
        ]);
        ensure_default_consumer_group("my_events", &mut options).expect("ensure");
        assert!(options.peek_opt_str(opt::KAFKA_GROUP_ID).expect("peek").is_none());
    }
}
