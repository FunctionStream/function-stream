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
use std::sync::Arc;
use tracing::info;

use protocol::function_stream_graph::connector_op::Config;
use protocol::function_stream_graph::{
    BadDataPolicy, ConnectorOp, DecimalEncodingProto, FormatConfig, MqttSourceConfig,
    TimestampFormatProto,
};

use crate::core::api::operator::ConstructedOperator;
use crate::factory::global::Registry;
use crate::factory::operator_constructor::OperatorConstructor;
use crate::format::{
    BadDataPolicy as RtBadDataPolicy, DecimalEncoding as RtDecimalEncoding,
    Format as RuntimeFormat, JsonFormat as RuntimeJsonFormat, TimestampFormat as RtTimestampFormat,
};
use crate::operators::source::batch_buffer::DEFAULT_SOURCE_BATCH_SIZE;
use crate::operators::source::kafka::BufferedDeserializer;
use crate::operators::source::mqtt::{MqttSourceOperator, proto_qos_to_rumqtt};
use crate::sql::common::FsSchema;

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

pub struct MqttSourceDispatcher;

impl OperatorConstructor for MqttSourceDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload).context("Failed to decode ConnectorOp protobuf")?;

        let fs_schema = op
            .fs_schema
            .as_ref()
            .map(|fs| FsSchema::try_from(fs.clone()))
            .transpose()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        match op.config {
            Some(Config::MqttSource(ref cfg)) => {
                Self::build_mqtt_source(&op.name, cfg, fs_schema)
            }
            Some(_) => bail!(
                "ConnectorOp '{}': received non-mqtt connector config for MQTT dispatcher",
                op.name
            ),
            None => bail!("ConnectorOp '{}' has no configuration payload", op.name),
        }
    }
}

impl MqttSourceDispatcher {
    fn build_mqtt_source(
        _name: &str,
        cfg: &MqttSourceConfig,
        fs_schema: Option<FsSchema>,
    ) -> Result<ConstructedOperator> {
        info!(
            topic = %cfg.topic,
            host = %cfg.host,
            port = cfg.port,
            "Constructing MQTT Source"
        );

        let fs = fs_schema.context("fs_schema is required for MQTT Source")?;
        let runtime_format = proto_format_to_runtime(&cfg.format)?;
        let bad_data = proto_bad_data_to_runtime(cfg.bad_data_policy);

        let deserializer = Box::new(BufferedDeserializer::new(
            runtime_format,
            fs.schema.clone(),
            bad_data,
            DEFAULT_SOURCE_BATCH_SIZE,
        ));

        let port = cfg.port.clamp(1, u16::MAX as u32) as u16;

        let source_op = MqttSourceOperator::new(
            cfg.topic.clone(),
            cfg.host.clone(),
            port,
            cfg.client_id.clone(),
            cfg.username.clone(),
            cfg.password.clone(),
            proto_qos_to_rumqtt(cfg.qos),
            cfg.clean_session,
            cfg.keep_alive_secs as u64,
            vec![],
            deserializer,
        );

        Ok(ConstructedOperator::Source(Box::new(source_op)))
    }
}
