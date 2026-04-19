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

use anyhow::{Context, Result, bail};
use prost::Message;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use protocol::function_stream_graph::connector_op::Config;
use protocol::function_stream_graph::{
    BadDataPolicy, ConnectorOp, DecimalEncodingProto, FormatConfig, KafkaAuthConfig,
    KafkaOffsetMode, KafkaReadMode, KafkaSinkCommitMode, KafkaSinkConfig, KafkaSourceConfig,
    TimestampFormatProto,
};
use tracing::info;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::api::source::SourceOffset;
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::factory::operator_constructor::OperatorConstructor;
use crate::runtime::streaming::format::{
    BadDataPolicy as RtBadDataPolicy, DataSerializer, DecimalEncoding as RtDecimalEncoding,
    Format as RuntimeFormat, JsonFormat as RuntimeJsonFormat, TimestampFormat as RtTimestampFormat,
};
use crate::runtime::streaming::operators::sink::kafka::{ConsistencyMode, KafkaSinkOperator};
use crate::runtime::streaming::operators::source::kafka::{
    BufferedDeserializer, KafkaSourceOperator,
};
use crate::sql::common::FsSchema;

const DEFAULT_SOURCE_BATCH_SIZE: usize = 1024;

// ─────────────── Proto → Runtime type conversions ───────────────

fn proto_format_to_runtime(fmt: &Option<FormatConfig>) -> Result<RuntimeFormat> {
    let cfg = fmt.as_ref().context("FormatConfig is required")?;
    match &cfg.format {
        Some(protocol::function_stream_graph::format_config::Format::Json(j)) => {
            Ok(RuntimeFormat::Json(RuntimeJsonFormat {
                timestamp_format: match j.timestamp_format() {
                    TimestampFormatProto::TimestampRfc3339 => RtTimestampFormat::RFC3339,
                    TimestampFormatProto::TimestampUnixMillis => RtTimestampFormat::UnixMillis,
                },
                decimal_encoding: match j.decimal_encoding() {
                    DecimalEncodingProto::DecimalNumber => RtDecimalEncoding::Number,
                    DecimalEncodingProto::DecimalString => RtDecimalEncoding::String,
                    DecimalEncodingProto::DecimalBytes => RtDecimalEncoding::Bytes,
                },
                include_schema: j.include_schema,
            }))
        }
        Some(protocol::function_stream_graph::format_config::Format::RawString(_)) => {
            Ok(RuntimeFormat::RawString)
        }
        Some(protocol::function_stream_graph::format_config::Format::RawBytes(_)) => {
            Ok(RuntimeFormat::RawBytes)
        }
        None => bail!("FormatConfig has no format variant set"),
    }
}

fn proto_bad_data_to_runtime(policy: i32) -> RtBadDataPolicy {
    match BadDataPolicy::try_from(policy) {
        Ok(BadDataPolicy::BadDataDrop) => RtBadDataPolicy::Drop,
        _ => RtBadDataPolicy::Fail,
    }
}

fn proto_offset_to_runtime(mode: i32) -> SourceOffset {
    match KafkaOffsetMode::try_from(mode) {
        Ok(KafkaOffsetMode::KafkaOffsetLatest) => SourceOffset::Latest,
        Ok(KafkaOffsetMode::KafkaOffsetEarliest) => SourceOffset::Earliest,
        _ => SourceOffset::Group,
    }
}

fn build_auth_client_configs(auth: &Option<KafkaAuthConfig>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(auth) = auth else { return out };
    match &auth.auth {
        Some(protocol::function_stream_graph::kafka_auth_config::Auth::Sasl(sasl)) => {
            out.insert("security.protocol".to_string(), sasl.protocol.clone());
            out.insert("sasl.mechanism".to_string(), sasl.mechanism.clone());
            out.insert("sasl.username".to_string(), sasl.username.clone());
            out.insert("sasl.password".to_string(), sasl.password.clone());
        }
        Some(protocol::function_stream_graph::kafka_auth_config::Auth::AwsMskIam(iam)) => {
            out.insert("security.protocol".to_string(), "SASL_SSL".to_string());
            out.insert("sasl.mechanism".to_string(), "OAUTHBEARER".to_string());
            out.insert(
                "sasl.oauthbearer.extensions".to_string(),
                format!("logicalCluster=aws_msk;aws_region={}", iam.region),
            );
        }
        _ => {}
    }
    out
}

fn merge_client_configs(
    auth: &Option<KafkaAuthConfig>,
    extra: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut configs = build_auth_client_configs(auth);
    for (k, v) in extra {
        configs.insert(k.clone(), v.clone());
    }
    configs
}

pub struct KafkaConnectorDispatcher;

impl OperatorConstructor for KafkaConnectorDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload).context("Failed to decode ConnectorOp protobuf")?;

        let fs_schema = op
            .fs_schema
            .as_ref()
            .map(|fs| FsSchema::try_from(fs.clone()))
            .transpose()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        match op.config {
            Some(Config::KafkaSource(ref cfg)) => {
                Self::build_kafka_source(&op.name, cfg, fs_schema)
            }
            Some(Config::KafkaSink(ref cfg)) => Self::build_kafka_sink(&op.name, cfg, fs_schema),
            Some(Config::Generic(_)) => bail!(
                "ConnectorOp '{}': GenericConnectorConfig dispatch not yet implemented",
                op.name
            ),
            None => bail!("ConnectorOp '{}' has no configuration payload", op.name),
        }
    }
}

impl KafkaConnectorDispatcher {
    fn build_kafka_source(
        _name: &str,
        cfg: &KafkaSourceConfig,
        fs_schema: Option<FsSchema>,
    ) -> Result<ConstructedOperator> {
        info!(topic = %cfg.topic, "Constructing Kafka Source");

        let fs = fs_schema.context("fs_schema is required for Kafka Source")?;
        let client_configs = merge_client_configs(&cfg.auth, &cfg.client_configs);

        let mut final_configs = client_configs;
        if cfg.read_mode() == KafkaReadMode::KafkaReadCommitted {
            final_configs.insert("isolation.level".to_string(), "read_committed".to_string());
        }

        let runtime_format = proto_format_to_runtime(&cfg.format)?;
        let bad_data = proto_bad_data_to_runtime(cfg.bad_data_policy);

        let deserializer = Box::new(BufferedDeserializer::new(
            runtime_format,
            fs.schema.clone(),
            bad_data,
            DEFAULT_SOURCE_BATCH_SIZE,
        ));

        let rate = NonZeroU32::new(cfg.rate_limit_msgs_per_sec.max(1))
            .unwrap_or_else(|| NonZeroU32::new(1_000_000).expect("nonzero"));

        let source_op = KafkaSourceOperator::new(
            cfg.topic.clone(),
            cfg.bootstrap_servers.clone(),
            cfg.group_id.clone(),
            cfg.group_id_prefix.clone(),
            proto_offset_to_runtime(cfg.offset_mode),
            final_configs,
            rate,
            vec![],
            deserializer,
        );

        Ok(ConstructedOperator::Source(Box::new(source_op)))
    }

    fn build_kafka_sink(
        _name: &str,
        cfg: &KafkaSinkConfig,
        fs_schema: Option<FsSchema>,
    ) -> Result<ConstructedOperator> {
        info!(topic = %cfg.topic, "Constructing Kafka Sink");

        let fs_in = fs_schema.context("fs_schema is required for Kafka Sink")?;
        let client_configs = merge_client_configs(&cfg.auth, &cfg.client_configs);

        let consistency = match cfg.commit_mode() {
            KafkaSinkCommitMode::KafkaSinkExactlyOnce => {
                info!(
                    topic = %cfg.topic,
                    "Kafka sink exactly-once: transactional producer + checkpoint 2PC. Downstream Kafka consumers of this topic should set isolation.level=read_committed."
                );
                ConsistencyMode::ExactlyOnce
            }
            KafkaSinkCommitMode::KafkaSinkAtLeastOnce => ConsistencyMode::AtLeastOnce,
        };

        let runtime_format = proto_format_to_runtime(&cfg.format)?;
        let fs = sink_fs_schema_adjusted(fs_in, &cfg.key_field, &cfg.timestamp_field)?;
        let serializer = DataSerializer::new(runtime_format, fs.schema.clone());

        let sink_op = KafkaSinkOperator::new(
            cfg.topic.clone(),
            cfg.bootstrap_servers.clone(),
            consistency,
            client_configs,
            fs,
            serializer,
        );

        Ok(ConstructedOperator::Operator(Box::new(sink_op)))
    }
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
            .ok_or_else(|| anyhow::anyhow!("timestamp column '{name}' not found in schema"))?
            .0
    } else {
        fs.timestamp_index
    };
    let keys = fs.clone_storage_key_indices();
    let routing = if let Some(name) = key_field {
        let k = schema
            .column_with_name(name)
            .ok_or_else(|| anyhow::anyhow!("key column '{name}' not found in schema"))?
            .0;
        Some(vec![k])
    } else {
        fs.clone_routing_key_indices()
    };
    Ok(FsSchema::new(schema, ts, keys, routing))
}
