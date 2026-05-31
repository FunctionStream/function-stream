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

use std::io::Read;
use std::path::Path;

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use protocol::function_stream_graph::{
    RobotBagFormat, RobotBagSourceConfig,
};

use crate::common::connector_options::ConnectorOptions;
use crate::common::constants::connector_type;
use crate::common::formats::{BadData, Format as SqlFormat};
use crate::common::with_option_keys as opt;
use crate::connector::config::ConnectorConfig;
use crate::connector::provider::SourceProvider;

use super::ros_common::{bad_data_to_proto, sql_format_to_proto};

const DEFAULT_REPLAY_INTERVAL_MS: u64 = 0;

pub struct RobotBagSourceConnector;

impl RobotBagSourceConnector {
    fn directory_has_pcd_files(dir: &Path) -> bool {
        std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("pcd"))
            })
    }

    fn is_ros1_bag_file(path: &Path) -> bool {
        let Ok(mut file) = std::fs::File::open(path) else {
            return false;
        };
        let mut buf = [0u8; 13];
        let Ok(n) = file.read(&mut buf) else {
            return false;
        };
        if n < 13 {
            return false;
        }
        std::str::from_utf8(&buf).ok().is_some_and(|s| {
            s.starts_with("#ROSBAG V2.0") || s.starts_with("#ROSBAG V1.02") || s.starts_with("#ROSBAG V1.0")
        })
    }

    fn detect_format(path: &str) -> i32 {
        let p = Path::new(path);
        if p.is_dir() {
            if Self::directory_has_pcd_files(p) {
                return RobotBagFormat::RobotBagPcd as i32;
            }
            return RobotBagFormat::RobotBagRos2 as i32;
        }
        if Self::is_ros1_bag_file(p) {
            return RobotBagFormat::RobotBagRos1 as i32;
        }
        match p.extension().and_then(|e| e.to_str()).map(|s| s.to_ascii_lowercase()) {
            Some(ext) if ext == "mcap" => RobotBagFormat::RobotBagMcap as i32,
            Some(ext) if ext == "db3" => RobotBagFormat::RobotBagRos2 as i32,
            Some(ext) if ext == "bag" => RobotBagFormat::RobotBagRos1 as i32,
            Some(ext) if ext == "pcd" => RobotBagFormat::RobotBagPcd as i32,
            Some(ext) if ext == "jsonl" || ext == "ndjson" => RobotBagFormat::RobotBagJsonl as i32,
            Some(ext) if ext == "csv" => RobotBagFormat::RobotBagCsv as i32,
            _ => RobotBagFormat::RobotBagAuto as i32,
        }
    }

    fn parse_bag_format(options: &mut ConnectorOptions, path: &str) -> Result<i32> {
        let fmt = options
            .pull_opt_str(opt::ROBOT_BAG_FORMAT)?
            .map(|s| s.to_ascii_lowercase());
        Ok(match fmt.as_deref() {
            None => Self::detect_format(path),
            Some("auto") => Self::detect_format(path),
            Some("mcap") => RobotBagFormat::RobotBagMcap as i32,
            Some("ros2" | "db3" | "rosbag2") => RobotBagFormat::RobotBagRos2 as i32,
            Some("ros1" | "bag" | "rosbag") => RobotBagFormat::RobotBagRos1 as i32,
            Some("pcd" | "pointcloud") => RobotBagFormat::RobotBagPcd as i32,
            Some("jsonl" | "ndjson" | "json") => RobotBagFormat::RobotBagJsonl as i32,
            Some("csv") => RobotBagFormat::RobotBagCsv as i32,
            Some(other) => {
                return plan_err!(
                    "invalid bag.format '{other}'; expected auto, mcap, ros1, ros2, pcd, jsonl, or csv"
                );
            }
        })
    }
}

impl SourceProvider for RobotBagSourceConnector {
    fn name(&self) -> &'static str {
        connector_type::ROBOT_BAG
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<SqlFormat>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig> {
        let path = options.pull_opt_str(opt::ROBOT_BAG_PATH)?;
        let path = if let Some(p) = path {
            p
        } else {
            options.pull_opt_str(opt::PATH)?.ok_or_else(|| {
                plan_datafusion_err!("robot-bag source requires 'path' in the WITH clause")
            })?
        };

        let bag_format = Self::parse_bag_format(options, &path)?;

        let topic = if let Some(t) = options.pull_opt_str(opt::ROBOT_BAG_TOPIC)? {
            Some(t)
        } else {
            options.pull_opt_str(opt::ROS_TOPIC)?
        };

        let replay_interval_ms = options
            .pull_opt_u64(opt::ROBOT_BAG_REPLAY_INTERVAL_MS)?
            .unwrap_or(DEFAULT_REPLAY_INTERVAL_MS);

        let loop_replay = options
            .pull_opt_bool(opt::ROBOT_BAG_LOOP)?
            .unwrap_or(false);

        let pcd_emit_mode = options
            .pull_opt_str(opt::ROBOT_BAG_PCD_EMIT_MODE)?
            .unwrap_or_else(|| "point".to_string());
        match pcd_emit_mode.to_ascii_lowercase().as_str() {
            "point" | "points" | "cloud" | "file" => {}
            other => {
                return plan_err!(
                    "invalid pcd.emit.mode '{other}'; expected 'point' or 'cloud'"
                );
            }
        }

        let sql_format = format.as_ref().ok_or_else(|| {
            plan_datafusion_err!(
                "robot-bag source requires 'format' (json / raw_bytes / raw_string)"
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

        Ok(ConnectorConfig::RobotBagSource(RobotBagSourceConfig {
            path,
            bag_format,
            topic,
            replay_interval_ms,
            loop_replay,
            format: Some(proto_format),
            bad_data_policy: bad_data_to_proto(&bad_data),
            client_configs,
            pcd_emit_mode,
        }))
    }
}
