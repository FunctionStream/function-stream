//! Kafka Source/Sink：从 [`ConnectorOp`] + [`OperatorConfig`] 构造物理算子（鉴权与 client 配置合并）。

use anyhow::{anyhow, bail, Context, Result};
use prost::Message;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use protocol::grpc::api::ConnectorOp;
use tracing::{info, warn};

use super::OperatorConstructor;
use crate::runtime::streaming::api::operator::{ConstructedOperator, Registry};
use crate::runtime::streaming::api::source::SourceOffset;
use crate::runtime::streaming::format::{
    BadDataPolicy, DataSerializer, DecimalEncoding as RtDecimalEncoding, Format as RuntimeFormat,
    JsonFormat as RuntimeJsonFormat, TimestampFormat as RtTimestampFormat,
};
use crate::runtime::streaming::operators::sink::kafka::{ConsistencyMode, KafkaSinkOperator};
use crate::runtime::streaming::operators::source::kafka::{BufferedDeserializer, KafkaSourceOperator};
use crate::sql::common::formats::{
    BadData, DecimalEncoding as SqlDecimalEncoding, Format as SqlFormat, JsonFormat as SqlJsonFormat,
    TimestampFormat as SqlTimestampFormat,
};
use crate::sql::common::kafka_catalog::{
    KafkaConfig, KafkaConfigAuthentication, KafkaTable, ReadMode, SinkCommitMode, TableType,
};
use crate::sql::common::{FsSchema, OperatorConfig};

const DEFAULT_SOURCE_BATCH_SIZE: usize = 1024;

/// 合并连接级鉴权、全局 `connection_properties` 与表级 `client_configs`（表级覆盖同名键）。
pub fn build_client_configs(config: &KafkaConfig, table: &KafkaTable) -> Result<HashMap<String, String>> {
    let mut client_configs = HashMap::new();

    match &config.authentication {
        KafkaConfigAuthentication::None => {}
        KafkaConfigAuthentication::Sasl {
            protocol,
            mechanism,
            username,
            password,
        } => {
            client_configs.insert("security.protocol".to_string(), protocol.clone());
            client_configs.insert("sasl.mechanism".to_string(), mechanism.clone());
            client_configs.insert("sasl.username".to_string(), username.clone());
            client_configs.insert("sasl.password".to_string(), password.clone());
        }
        KafkaConfigAuthentication::AwsMskIam { region } => {
            client_configs.insert("security.protocol".to_string(), "SASL_SSL".to_string());
            client_configs.insert("sasl.mechanism".to_string(), "OAUTHBEARER".to_string());
            client_configs.insert(
                "sasl.oauthbearer.extensions".to_string(),
                format!("logicalCluster=aws_msk;aws_region={region}"),
            );
        }
    }

    for (k, v) in &config.connection_properties {
        client_configs.insert(k.clone(), v.clone());
    }

    for (k, v) in &table.client_configs {
        if client_configs.contains_key(k) {
            warn!(
                "Kafka config key '{}' is defined in both connection and table; using table value",
                k
            );
        }
        client_configs.insert(k.clone(), v.clone());
    }

    Ok(client_configs)
}

fn bad_data_policy(b: Option<BadData>) -> BadDataPolicy {
    match b.unwrap_or_default() {
        BadData::Fail {} => BadDataPolicy::Fail,
        BadData::Drop {} => BadDataPolicy::Drop,
    }
}

fn sql_timestamp_format(t: SqlTimestampFormat) -> RtTimestampFormat {
    match t {
        SqlTimestampFormat::RFC3339 => RtTimestampFormat::RFC3339,
        SqlTimestampFormat::UnixMillis => RtTimestampFormat::UnixMillis,
    }
}

fn sql_decimal_encoding(d: SqlDecimalEncoding) -> RtDecimalEncoding {
    match d {
        SqlDecimalEncoding::Number => RtDecimalEncoding::Number,
        SqlDecimalEncoding::String => RtDecimalEncoding::String,
        SqlDecimalEncoding::Bytes => RtDecimalEncoding::Bytes,
    }
}

fn sql_json_format_to_runtime(j: &SqlJsonFormat) -> RuntimeJsonFormat {
    RuntimeJsonFormat {
        timestamp_format: sql_timestamp_format(j.timestamp_format),
        decimal_encoding: sql_decimal_encoding(j.decimal_encoding),
        include_schema: j.include_schema,
    }
}

fn sql_format_to_runtime(f: SqlFormat) -> Result<RuntimeFormat> {
    match f {
        SqlFormat::Json(j) => Ok(RuntimeFormat::Json(sql_json_format_to_runtime(&j))),
        SqlFormat::RawString(_) => Ok(RuntimeFormat::RawString),
        SqlFormat::RawBytes(_) => Ok(RuntimeFormat::RawBytes),
        other => bail!(
            "Kafka connector: format '{}' is not supported for runtime deserializer/serializer yet",
            other.name()
        ),
    }
}

fn kafka_table_offset_to_runtime(o: crate::sql::common::KafkaTableSourceOffset) -> SourceOffset {
    use crate::sql::common::KafkaTableSourceOffset as KOff;
    match o {
        KOff::Latest => SourceOffset::Latest,
        KOff::Earliest => SourceOffset::Earliest,
        KOff::Group => SourceOffset::Group,
    }
}

fn non_zero_rate_per_second(op: &OperatorConfig) -> NonZeroU32 {
    op.rate_limit
        .as_ref()
        .and_then(|r| NonZeroU32::new(r.messages_per_second.max(1)))
        .unwrap_or_else(|| NonZeroU32::new(1_000_000).expect("nonzero"))
}

fn sink_fs_schema_adjusted(
    fs: FsSchema,
    key_field: &Option<String>,
    timestamp_field: &Option<String>,
) -> Result<FsSchema> {
    if key_field.is_none() && timestamp_field.is_none() {
        return Ok(fs);
    }
    let schema = fs.schema.clone();
    let ts = if let Some(name) = timestamp_field {
        schema
            .column_with_name(name)
            .ok_or_else(|| anyhow!("timestamp column '{name}' not found in schema"))?
            .0
    } else {
        fs.timestamp_index
    };
    let keys = fs.clone_storage_key_indices();
    let routing = if let Some(name) = key_field {
        let k = schema
            .column_with_name(name)
            .ok_or_else(|| anyhow!("key column '{name}' not found in schema"))?
            .0;
        Some(vec![k])
    } else {
        fs.clone_routing_key_indices()
    };
    Ok(FsSchema::new(schema, ts, keys, routing))
}

fn decode_operator_config(op: &ConnectorOp) -> Result<OperatorConfig> {
    serde_json::from_str(&op.config).with_context(|| {
        format!(
            "Invalid OperatorConfig JSON for connector '{}'",
            op.connector
        )
    })
}

/// 由 [`ConnectorOp`] 构造 Kafka Source（`connector` 须为 `kafka`）。
pub struct KafkaSourceDispatcher;

impl OperatorConstructor for KafkaSourceDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload)
            .context("Failed to decode ConnectorOp protobuf for Kafka Source")?;

        if op.connector != "kafka" {
            bail!(
                "KafkaSourceDispatcher: expected connector 'kafka', got '{}'",
                op.connector
            );
        }

        let op_config = decode_operator_config(&op)?;

        let kafka_config: KafkaConfig = serde_json::from_value(op_config.connection.clone())
            .context("Failed to parse Kafka connection configuration")?;

        let kafka_table: KafkaTable = serde_json::from_value(op_config.table.clone())
            .context("Failed to parse Kafka table configuration")?;

        let TableType::Source {
            offset,
            read_mode,
            group_id,
            group_id_prefix,
        } = &kafka_table.kind
        else {
            bail!(
                "Expected Kafka Source, got Sink configuration for topic '{}'",
                kafka_table.topic
            );
        };

        info!("Constructing Kafka Source for topic: {}", kafka_table.topic);

        let mut client_configs = build_client_configs(&kafka_config, &kafka_table)?;
        if let Some(ReadMode::ReadCommitted) = read_mode {
            client_configs.insert("isolation.level".to_string(), "read_committed".to_string());
        }

        let sql_format = op_config
            .format
            .clone()
            .context("Format must be specified for Kafka Source")?;
        let runtime_format = sql_format_to_runtime(sql_format)?;
        let fs = op_config
            .input_schema
            .clone()
            .context("input_schema is required for Kafka Source")?;
        let bad = bad_data_policy(op_config.bad_data.clone());

        let deserializer: std::boxed::Box<
            dyn crate::runtime::streaming::operators::source::kafka::BatchDeserializer,
        > = Box::new(BufferedDeserializer::new(
            runtime_format,
            fs.schema.clone(),
            bad,
            DEFAULT_SOURCE_BATCH_SIZE,
        ));

        let source_op = KafkaSourceOperator::new(
            kafka_table.topic.clone(),
            kafka_config.bootstrap_servers.clone(),
            group_id.clone(),
            group_id_prefix.clone(),
            kafka_table_offset_to_runtime(*offset),
            client_configs,
            non_zero_rate_per_second(&op_config),
            op_config.metadata_fields,
            deserializer,
        );

        Ok(ConstructedOperator::Source(Box::new(source_op)))
    }
}

/// 由 [`ConnectorOp`] 构造 Kafka Sink（`connector` 须为 `kafka`）。
pub struct KafkaSinkDispatcher;

impl OperatorConstructor for KafkaSinkDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload)
            .context("Failed to decode ConnectorOp protobuf for Kafka Sink")?;

        if op.connector != "kafka" {
            bail!(
                "KafkaSinkDispatcher: expected connector 'kafka', got '{}'",
                op.connector
            );
        }

        let op_config = decode_operator_config(&op)?;

        let kafka_config: KafkaConfig = serde_json::from_value(op_config.connection.clone())
            .context("Failed to parse Kafka connection configuration")?;

        let kafka_table: KafkaTable = serde_json::from_value(op_config.table.clone())
            .context("Failed to parse Kafka table configuration")?;

        let TableType::Sink {
            commit_mode,
            key_field,
            timestamp_field,
        } = &kafka_table.kind
        else {
            bail!(
                "Expected Kafka Sink, got Source configuration for topic '{}'",
                kafka_table.topic
            );
        };

        info!("Constructing Kafka Sink for topic: {}", kafka_table.topic);

        let client_configs = build_client_configs(&kafka_config, &kafka_table)?;

        let consistency = match commit_mode {
            SinkCommitMode::ExactlyOnce => ConsistencyMode::ExactlyOnce,
            SinkCommitMode::AtLeastOnce => ConsistencyMode::AtLeastOnce,
        };

        let sql_format = op_config
            .format
            .clone()
            .context("Format must be specified for Kafka Sink")?;
        let runtime_format = sql_format_to_runtime(sql_format)?;

        let fs_in = op_config
            .input_schema
            .clone()
            .context("input_schema is required for Kafka Sink")?;
        let fs = sink_fs_schema_adjusted(fs_in, key_field, timestamp_field)?;

        let serializer = DataSerializer::new(runtime_format, fs.schema.clone());

        let sink_op = KafkaSinkOperator::new(
            kafka_table.topic.clone(),
            kafka_config.bootstrap_servers.clone(),
            consistency,
            client_configs,
            fs,
            serializer,
        );

        Ok(ConstructedOperator::Operator(Box::new(sink_op)))
    }
}

/// 注册 `KafkaSource` / `KafkaSink` 构造器（由 [`super::OperatorFactory::register_builtins`] 调用）。
pub fn register_kafka_plugins(factory: &mut super::OperatorFactory) {
    factory.register("KafkaSource", Box::new(KafkaSourceDispatcher));
    factory.register("KafkaSink", Box::new(KafkaSinkDispatcher));
    info!("Registered Kafka connector plugins (KafkaSource, KafkaSink)");
}
