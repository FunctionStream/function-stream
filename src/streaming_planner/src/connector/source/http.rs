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

use std::collections::HashMap;

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use protocol::function_stream_graph::{
    BadDataPolicy, DecimalEncodingProto, FormatConfig, HttpResponseSplit, HttpSourceConfig,
    HttpSourceMode, JsonFormatConfig, RawBytesFormatConfig, RawStringFormatConfig,
    TimestampFormatProto, format_config,
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

const DEFAULT_SCAN_INTERVAL_MS: u64 = 1000;
const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 30_000;
const DEFAULT_WEBHOOK_HOST: &str = "0.0.0.0";
const DEFAULT_WEBHOOK_PATH: &str = "/";

pub struct HttpSourceConnector;

impl HttpSourceConnector {
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
                "HTTP source connector: format '{}' is not supported",
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

    fn pull_scan_interval_ms(options: &mut ConnectorOptions) -> Result<u64> {
        if let Some(d) = options.pull_opt_duration(opt::HTTP_SCAN_INTERVAL)? {
            return Ok(d.as_millis() as u64);
        }
        if let Some(ms) = options.pull_opt_u64(opt::HTTP_SCAN_INTERVAL_MS)? {
            return Ok(ms.max(1));
        }
        if let Some(ms) = options.pull_opt_u64(opt::HTTP_POLL_INTERVAL_MS)? {
            return Ok(ms.max(1));
        }
        Ok(DEFAULT_SCAN_INTERVAL_MS)
    }

    fn pull_method(options: &mut ConnectorOptions) -> Result<String> {
        if let Some(m) = options.pull_opt_str(opt::HTTP_METHOD)? {
            return Ok(m.to_ascii_uppercase());
        }
        if let Some(m) = options.pull_opt_str(opt::HTTP_LOOKUP_METHOD)? {
            return Ok(m.to_ascii_uppercase());
        }
        Ok("GET".to_string())
    }

    fn pull_response_split(options: &mut ConnectorOptions) -> Result<i32> {
        let split = options
            .pull_opt_str(opt::HTTP_RESPONSE_SPLIT)?
            .map(|s| s.to_ascii_lowercase());
        Ok(match split.as_deref() {
            Some("ndjson") | Some("newline") => HttpResponseSplit::HttpResponseNdjson as i32,
            Some("json-array") | Some("array") => HttpResponseSplit::HttpResponseJsonArray as i32,
            Some("single") | None => HttpResponseSplit::HttpResponseSingle as i32,
            Some(other) => {
                return plan_err!(
                    "invalid http.response.split '{other}'; expected single, ndjson, or json-array"
                );
            }
        })
    }

    fn extract_headers(client_configs: &mut HashMap<String, String>) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        let keys: Vec<String> = client_configs
            .keys()
            .filter(|k| k.starts_with(opt::HTTP_HEADER_PREFIX))
            .cloned()
            .collect();
        for key in keys {
            if let Some(v) = client_configs.remove(&key) {
                let name = key[opt::HTTP_HEADER_PREFIX.len()..].to_string();
                headers.insert(name, v);
            }
        }
        headers
    }

    fn resolve_mode(
        options: &mut ConnectorOptions,
        listen_port: Option<u32>,
        url: Option<String>,
    ) -> Result<HttpSourceMode> {
        if let Some(mode) = options.pull_opt_str(opt::HTTP_MODE)? {
            return match mode.to_ascii_lowercase().as_str() {
                "poll" | "polling" => Ok(HttpSourceMode::HttpSourcePoll),
                "webhook" | "push" => Ok(HttpSourceMode::HttpSourceWebhook),
                other => plan_err!("invalid http mode '{other}'; expected poll or webhook"),
            };
        }
        if listen_port.is_some() {
            return Ok(HttpSourceMode::HttpSourceWebhook);
        }
        if url.is_some() {
            return Ok(HttpSourceMode::HttpSourcePoll);
        }
        Err(plan_datafusion_err!(
            "HTTP source requires 'url' (poll) or 'http.listen.port' (webhook)"
        ))
    }
}

impl SourceProvider for HttpSourceConnector {
    fn name(&self) -> &'static str {
        connector_type::HTTP
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<SqlFormat>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig> {
        let url = options.pull_opt_str(opt::HTTP_URL)?;
        let listen_port = options.pull_opt_u64(opt::HTTP_LISTEN_PORT)?.map(|p| p as u32);
        let mode = Self::resolve_mode(options, listen_port, url.clone())?;

        let sql_format = format.as_ref().ok_or_else(|| {
            plan_datafusion_err!(
                "HTTP source requires 'format' in the WITH clause (e.g. format = 'json')"
            )
        })?;
        let proto_format = Self::sql_format_to_proto(sql_format)?;

        let rate_limit = options
            .pull_opt_u64(opt::MQTT_RATE_LIMIT_MESSAGES_PER_SECOND)?
            .map(|v| v.clamp(0, u32::MAX as u64) as u32)
            .unwrap_or(0);

        let scan_interval_ms = Self::pull_scan_interval_ms(options)?;
        let method = Self::pull_method(options)?;
        let response_split = Self::pull_response_split(options)?;

        let request_body = match options.pull_opt_str(opt::HTTP_REQUEST_BODY)? {
            Some(s) => Some(s),
            None => options.pull_opt_str(opt::HTTP_REQUEST_BODY_LEGACY)?,
        };

        let listen_host = options
            .pull_opt_str(opt::HTTP_LISTEN_HOST)?
            .unwrap_or_else(|| DEFAULT_WEBHOOK_HOST.to_string());

        let webhook_path = options
            .pull_opt_str(opt::HTTP_WEBHOOK_PATH)?
            .unwrap_or_else(|| DEFAULT_WEBHOOK_PATH.to_string());

        let request_timeout_ms = options
            .pull_opt_u64(opt::HTTP_REQUEST_TIMEOUT_MS)?
            .map(|v| v.clamp(1, u32::MAX as u64) as u32)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS);

        let _ = options.pull_opt_str(opt::TYPE)?;
        let _ = options.pull_opt_str(opt::CONNECTOR)?;

        let mut client_configs = options.drain_remaining_string_values()?;
        client_configs.remove(opt::CHECKPOINT_INTERVAL_MS);
        client_configs.remove(opt::PIPELINE_PARALLELISM);
        client_configs.remove(opt::KEY_BY_PARALLELISM);
        client_configs.remove(opt::FORMAT);

        let headers = Self::extract_headers(&mut client_configs);

        let (url, listen_port) = match mode {
            HttpSourceMode::HttpSourcePoll => {
                let url = url.ok_or_else(|| {
                    plan_datafusion_err!("HTTP poll mode requires 'url' in the WITH clause")
                })?;
                (url, listen_port.unwrap_or(0))
            }
            HttpSourceMode::HttpSourceWebhook => {
                let port = listen_port.ok_or_else(|| {
                    plan_datafusion_err!(
                        "HTTP webhook mode requires 'http.listen.port' in the WITH clause"
                    )
                })?;
                (String::new(), port)
            }
        };

        Ok(ConnectorConfig::HttpSource(HttpSourceConfig {
            mode: mode as i32,
            url,
            method,
            scan_interval_ms,
            request_body,
            headers,
            listen_host,
            listen_port,
            webhook_path,
            format: Some(proto_format),
            bad_data_policy: Self::bad_data_to_proto(&bad_data),
            rate_limit_msgs_per_sec: rate_limit,
            client_configs,
            request_timeout_ms,
            response_split,
        }))
    }
}
