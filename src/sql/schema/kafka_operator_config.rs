// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// Builds strongly-typed proto Kafka configs from SQL DDL WITH options.

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result as DFResult, plan_datafusion_err, plan_err};

use protocol::grpc::api::connector_op::Config as ProtoConfig;
use protocol::grpc::api::{
    BadDataPolicy, DecimalEncodingProto, FormatConfig, JsonFormatConfig, KafkaAuthConfig,
    KafkaAuthNone, KafkaOffsetMode, KafkaReadMode, KafkaSinkCommitMode, KafkaSinkConfig,
    KafkaSourceConfig, RawBytesFormatConfig, RawStringFormatConfig, TimestampFormatProto,
};

use crate::sql::common::constants::{connection_table_role, kafka_with_value};
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::{
    BadData, DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::sql::common::with_option_keys as opt;
use crate::sql::schema::table_role::TableRole;

fn sql_format_to_proto(fmt: &SqlFormat) -> DFResult<FormatConfig> {
    match fmt {
        SqlFormat::Json(j) => Ok(FormatConfig {
            format: Some(protocol::grpc::api::format_config::Format::Json(
                JsonFormatConfig {
                    timestamp_format: match j.timestamp_format {
                        SqlTimestampFormat::RFC3339 => TimestampFormatProto::TimestampRfc3339 as i32,
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
                },
            )),
        }),
        SqlFormat::RawString(_) => Ok(FormatConfig {
            format: Some(protocol::grpc::api::format_config::Format::RawString(
                RawStringFormatConfig {},
            )),
        }),
        SqlFormat::RawBytes(_) => Ok(FormatConfig {
            format: Some(protocol::grpc::api::format_config::Format::RawBytes(
                RawBytesFormatConfig {},
            )),
        }),
        other => plan_err!(
            "Kafka connector: format '{}' is not supported yet",
            other.name()
        ),
    }
}

fn sql_bad_data_to_proto(bad: &BadData) -> i32 {
    match bad {
        BadData::Fail {} => BadDataPolicy::BadDataFail as i32,
        BadData::Drop {} => BadDataPolicy::BadDataDrop as i32,
    }
}

/// Build Kafka proto config from a flat string map (catalog rebuild path).
pub fn build_kafka_proto_config_from_string_map(
    map: HashMap<String, String>,
    _physical_schema: &Schema,
) -> DFResult<ProtoConfig> {
    let mut options = ConnectorOptions::from_flat_string_map(map)?;
    let format = crate::sql::common::formats::Format::from_opts(&mut options)
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("invalid format: {e}")))?;
    let bad_data = BadData::from_opts(&mut options)
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("Invalid bad_data: '{e}'")))?;
    let _framing = crate::sql::common::formats::Framing::from_opts(&mut options)
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("invalid framing: '{e}'")))?;

    let role = match options.pull_opt_str(opt::TYPE)?.as_deref() {
        None | Some(connection_table_role::SOURCE) => TableRole::Ingestion,
        Some(connection_table_role::SINK) => TableRole::Egress,
        Some(connection_table_role::LOOKUP) => TableRole::Reference,
        Some(other) => {
            return plan_err!("invalid connection type '{other}' in WITH options");
        }
    };

    build_kafka_proto_config(&mut options, role, &format, bad_data)
}

/// Core builder shared by SQL DDL and catalog reload paths.
pub fn build_kafka_proto_config(
    options: &mut ConnectorOptions,
    role: TableRole,
    format: &Option<SqlFormat>,
    bad_data: BadData,
) -> DFResult<ProtoConfig> {
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

    let topic = options
        .pull_opt_str(opt::KAFKA_TOPIC)?
        .ok_or_else(|| plan_datafusion_err!("Kafka connector requires 'topic' in the WITH clause"))?;

    let sql_format = format.clone().ok_or_else(|| {
        plan_datafusion_err!(
            "Kafka connector requires 'format' in the WITH clause (e.g. format = 'json')"
        )
    })?;
    let proto_format = sql_format_to_proto(&sql_format)?;

    let rate_limit = options
        .pull_opt_u64(opt::KAFKA_RATE_LIMIT_MESSAGES_PER_SECOND)?
        .map(|v| v.clamp(1, u32::MAX as u64) as u32)
        .unwrap_or(0);

    let value_subject = options.pull_opt_str(opt::KAFKA_VALUE_SUBJECT)?;

    let auth = Some(KafkaAuthConfig {
        auth: Some(protocol::grpc::api::kafka_auth_config::Auth::None(
            KafkaAuthNone {},
        )),
    });

    let _ = options.pull_opt_str(opt::TYPE)?;
    let _ = options.pull_opt_str(opt::CONNECTOR)?;

    match role {
        TableRole::Ingestion => {
            let offset_mode = match options.pull_opt_str(opt::KAFKA_SCAN_STARTUP_MODE)?.as_deref() {
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
                Some(other) => {
                    return plan_err!("invalid isolation.level '{other}'");
                }
            };

            let group_id = match options.pull_opt_str(opt::KAFKA_GROUP_ID)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_GROUP_ID_LEGACY)?,
            };
            let group_id_prefix = options.pull_opt_str(opt::KAFKA_GROUP_ID_PREFIX)?;

            let client_configs = options.drain_remaining_string_values()?;

            Ok(ProtoConfig::KafkaSource(KafkaSourceConfig {
                topic,
                bootstrap_servers,
                group_id,
                group_id_prefix,
                offset_mode,
                read_mode,
                auth,
                client_configs,
                format: Some(proto_format),
                bad_data_policy: sql_bad_data_to_proto(&bad_data),
                rate_limit_msgs_per_sec: rate_limit,
                value_subject,
            }))
        }
        TableRole::Egress => {
            let commit_mode = match options.pull_opt_str(opt::KAFKA_SINK_COMMIT_MODE)?.as_deref() {
                Some(s)
                    if s == kafka_with_value::SINK_COMMIT_EXACTLY_ONCE_HYPHEN
                        || s == kafka_with_value::SINK_COMMIT_EXACTLY_ONCE_UNDERSCORE =>
                {
                    KafkaSinkCommitMode::KafkaSinkExactlyOnce as i32
                }
                None => KafkaSinkCommitMode::KafkaSinkAtLeastOnce as i32,
                Some(s)
                    if s == kafka_with_value::SINK_COMMIT_AT_LEAST_ONCE_HYPHEN
                        || s == kafka_with_value::SINK_COMMIT_AT_LEAST_ONCE_UNDERSCORE =>
                {
                    KafkaSinkCommitMode::KafkaSinkAtLeastOnce as i32
                }
                Some(other) => {
                    return plan_err!("invalid sink.commit.mode '{other}'");
                }
            };
            let key_field = match options.pull_opt_str(opt::KAFKA_SINK_KEY_FIELD)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_KEY_FIELD_LEGACY)?,
            };
            let timestamp_field = match options.pull_opt_str(opt::KAFKA_SINK_TIMESTAMP_FIELD)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_TIMESTAMP_FIELD_LEGACY)?,
            };

            let client_configs = options.drain_remaining_string_values()?;

            Ok(ProtoConfig::KafkaSink(KafkaSinkConfig {
                topic,
                bootstrap_servers,
                commit_mode,
                key_field,
                timestamp_field,
                auth,
                client_configs,
                format: Some(proto_format),
                value_subject,
            }))
        }
        TableRole::Reference => {
            plan_err!("Kafka connector cannot be used as a lookup table in this path")
        }
    }
}
