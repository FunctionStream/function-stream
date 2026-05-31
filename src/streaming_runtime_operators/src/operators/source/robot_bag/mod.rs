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

//! Robot recording file source: MCAP, ROS1 `.bag`, ROS2 `.db3`, PCD, JSONL/NDJSON, CSV.

mod pcd;
mod pcd_body;
mod ros1_bag;

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use pcd::{PcdEmitMode, load_pcd_paths, parse_pcd_file, pcd_to_messages};
use ros1_bag::{is_ros1_bag_file, load_ros1_bag};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::core::api::context::TaskContext;
use crate::core::api::source::{SourceCheckpointReport, SourceEvent, SourceOperator};
use crate::operators::source::batch_buffer::{MAX_BATCH_LINGER_TIME, SOURCE_POLL_TIMEOUT};
use crate::operators::source::kafka::BatchDeserializer;
use crate::sql::common::CheckpointBarrier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobotBagFormat {
    Auto,
    Mcap,
    Ros1,
    Ros2,
    Pcd,
    Jsonl,
    Csv,
}

pub fn proto_bag_format_to_runtime(fmt: i32) -> RobotBagFormat {
    use protocol::function_stream_graph::RobotBagFormat as Proto;
    match Proto::try_from(fmt) {
        Ok(Proto::RobotBagMcap) => RobotBagFormat::Mcap,
        Ok(Proto::RobotBagRos1) => RobotBagFormat::Ros1,
        Ok(Proto::RobotBagRos2) => RobotBagFormat::Ros2,
        Ok(Proto::RobotBagPcd) => RobotBagFormat::Pcd,
        Ok(Proto::RobotBagJsonl) => RobotBagFormat::Jsonl,
        Ok(Proto::RobotBagCsv) => RobotBagFormat::Csv,
        _ => RobotBagFormat::Auto,
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
struct BagMessage {
    payload: Vec<u8>,
    timestamp_ms: u64,
}

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

fn detect_format(path: &Path) -> RobotBagFormat {
    if path.is_dir() {
        if directory_has_pcd_files(path) {
            return RobotBagFormat::Pcd;
        }
        return RobotBagFormat::Ros2;
    }
    if is_ros1_bag_file(path) {
        return RobotBagFormat::Ros1;
    }
    match path.extension().and_then(|e| e.to_str()).map(|s| s.to_ascii_lowercase()) {
        Some(ext) if ext == "mcap" => RobotBagFormat::Mcap,
        Some(ext) if ext == "bag" => RobotBagFormat::Ros1,
        Some(ext) if ext == "db3" => RobotBagFormat::Ros2,
        Some(ext) if ext == "pcd" => RobotBagFormat::Pcd,
        Some(ext) if ext == "jsonl" || ext == "ndjson" => RobotBagFormat::Jsonl,
        Some(ext) if ext == "csv" => RobotBagFormat::Csv,
        _ => RobotBagFormat::Jsonl,
    }
}

fn resolve_format(path: &Path, fmt: RobotBagFormat) -> RobotBagFormat {
    if fmt == RobotBagFormat::Auto {
        detect_format(path)
    } else {
        fmt
    }
}

fn find_ros2_db3(path: &Path) -> Result<PathBuf> {
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    if path.is_dir() {
        for entry in std::fs::read_dir(path).with_context(|| format!("read dir {}", path.display()))?
        {
            let p = entry?.path();
            if p.extension().is_some_and(|e| e == "db3") {
                return Ok(p);
            }
        }
    }
    Err(anyhow!(
        "no ROS2 bag .db3 file found under '{}'",
        path.display()
    ))
}

fn load_mcap(path: &Path, topic_filter: Option<&str>) -> Result<Vec<BagMessage>> {
    let data = std::fs::read(path).with_context(|| format!("read mcap {}", path.display()))?;

    let mut out = Vec::new();
    for msg in mcap::MessageStream::new(&data).context("mcap message stream")? {
        let msg = msg.context("mcap message")?;
        let topic = msg.channel.topic.as_str();
        if let Some(filter) = topic_filter
            && topic != filter
        {
            continue;
        }
        out.push(BagMessage {
            payload: msg.data.to_vec(),
            timestamp_ms: msg.log_time / 1_000_000,
        });
    }
    out.sort_by_key(|m| m.timestamp_ms);
    info!(
        path = %path.display(),
        messages = out.len(),
        topic_filter = ?topic_filter,
        "loaded MCAP robot bag"
    );
    Ok(out)
}

fn load_ros1(path: &Path, topic_filter: Option<&str>) -> Result<Vec<BagMessage>> {
    let raw = load_ros1_bag(path, topic_filter)?;
    Ok(raw
        .into_iter()
        .map(|(payload, timestamp_ms)| BagMessage {
            payload,
            timestamp_ms,
        })
        .collect())
}

fn load_ros2_db3(path: &Path, topic_filter: Option<&str>) -> Result<Vec<BagMessage>> {
    let db3 = find_ros2_db3(path)?;
    let conn = rusqlite::Connection::open(&db3)
        .with_context(|| format!("open ros2 bag sqlite {}", db3.display()))?;

    let mut out = Vec::new();
    if let Some(topic) = topic_filter {
        let mut stmt = conn.prepare(
            "SELECT m.timestamp, m.data FROM messages m \
             JOIN topics t ON m.topic_id = t.id \
             WHERE t.name = ?1 ORDER BY m.timestamp",
        )?;
        let rows = stmt.query_map([topic], |row| {
            let ts: i64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((ts, data))
        })?;
        for row in rows {
            let (ts, data) = row?;
            out.push(BagMessage {
                payload: data,
                timestamp_ms: (ts / 1_000_000).max(0) as u64,
            });
        }
    } else {
        let mut stmt =
            conn.prepare("SELECT timestamp, data FROM messages ORDER BY timestamp")?;
        let rows = stmt.query_map([], |row| {
            let ts: i64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((ts, data))
        })?;
        for row in rows {
            let (ts, data) = row?;
            out.push(BagMessage {
                payload: data,
                timestamp_ms: (ts / 1_000_000).max(0) as u64,
            });
        }
    }

    info!(
        db3 = %db3.display(),
        messages = out.len(),
        topic_filter = ?topic_filter,
        "loaded ROS2 bag"
    );
    Ok(out)
}

fn load_pcd(path: &Path, emit_mode: PcdEmitMode) -> Result<Vec<BagMessage>> {
    let files = load_pcd_paths(path)?;
    let mut out = Vec::new();
    let base_ts = wall_clock_ms();

    for (file_idx, file_path) in files.iter().enumerate() {
        let cloud = parse_pcd_file(file_path)?;
        let file_ts = base_ts.saturating_add(file_idx as u64);
        let msgs = pcd_to_messages(&cloud, emit_mode, file_ts)?;
        info!(
            path = %file_path.display(),
            points = cloud.header.points,
            data_mode = ?cloud.header.data_mode,
            emit_mode = ?emit_mode,
            messages = msgs.len(),
            "loaded PCD point cloud"
        );
        for (payload, ts) in msgs {
            out.push(BagMessage {
                payload,
                timestamp_ms: ts,
            });
        }
    }
    Ok(out)
}

fn load_jsonl(path: &Path) -> Result<Vec<BagMessage>> {
    let file = File::open(path).with_context(|| format!("open jsonl {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(BagMessage {
            payload: trimmed.as_bytes().to_vec(),
            timestamp_ms: wall_clock_ms(),
        });
    }
    info!(path = %path.display(), messages = out.len(), "loaded JSONL robot log");
    Ok(out)
}

fn load_csv(path: &Path) -> Result<Vec<BagMessage>> {
    let file = File::open(path).with_context(|| format!("open csv {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut first = true;
    for line in reader.lines() {
        let line = line?;
        if first {
            first = false;
            continue;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(BagMessage {
            payload: trimmed.as_bytes().to_vec(),
            timestamp_ms: wall_clock_ms(),
        });
    }
    info!(path = %path.display(), messages = out.len(), "loaded CSV robot log");
    Ok(out)
}

fn load_bag_messages(
    path: &Path,
    format: RobotBagFormat,
    topic_filter: Option<&str>,
    pcd_emit_mode: PcdEmitMode,
) -> Result<Vec<BagMessage>> {
    let fmt = resolve_format(path, format);
    match fmt {
        RobotBagFormat::Mcap => load_mcap(path, topic_filter),
        RobotBagFormat::Ros1 => load_ros1(path, topic_filter),
        RobotBagFormat::Ros2 => load_ros2_db3(path, topic_filter),
        RobotBagFormat::Pcd => load_pcd(path, pcd_emit_mode),
        RobotBagFormat::Jsonl => load_jsonl(path),
        RobotBagFormat::Csv => load_csv(path),
        RobotBagFormat::Auto => unreachable!("resolved above"),
    }
}

pub struct RobotBagSourceOperator {
    path: PathBuf,
    bag_format: RobotBagFormat,
    topic_filter: Option<String>,
    pcd_emit_mode: PcdEmitMode,
    replay_interval: Duration,
    loop_replay: bool,
    deserializer: Box<dyn BatchDeserializer>,
    messages: Vec<BagMessage>,
    cursor: usize,
    last_flush_time: Instant,
    last_emit_time: Instant,
}

impl RobotBagSourceOperator {
    pub fn new(
        path: String,
        bag_format: RobotBagFormat,
        topic_filter: Option<String>,
        pcd_emit_mode: PcdEmitMode,
        replay_interval: Duration,
        loop_replay: bool,
        deserializer: Box<dyn BatchDeserializer>,
    ) -> Self {
        Self {
            path: PathBuf::from(path),
            bag_format,
            topic_filter,
            pcd_emit_mode,
            replay_interval,
            loop_replay,
            deserializer,
            messages: Vec::new(),
            cursor: 0,
            last_flush_time: Instant::now(),
            last_emit_time: Instant::now(),
        }
    }

    fn try_emit_buffered_batch(&mut self, reason: &str) -> Result<Option<SourceEvent>> {
        let should_flush_by_size = self.deserializer.should_flush();
        let should_flush_by_time = self.last_flush_time.elapsed() > MAX_BATCH_LINGER_TIME;

        if !self.deserializer.is_empty()
            && (should_flush_by_size || should_flush_by_time)
            && let Some(batch) = self.deserializer.flush_buffer()?
        {
            self.last_flush_time = Instant::now();
            debug!(num_rows = batch.num_rows(), reason, "robot-bag emitting batch");
            return Ok(Some(SourceEvent::Data(batch)));
        }
        Ok(None)
    }

    fn reload_messages(&mut self) -> Result<()> {
        self.messages = load_bag_messages(
            &self.path,
            self.bag_format,
            self.topic_filter.as_deref(),
            self.pcd_emit_mode,
        )?;
        self.cursor = 0;
        Ok(())
    }
}

#[async_trait]
impl SourceOperator for RobotBagSourceOperator {
    fn name(&self) -> &str {
        self.path.to_str().unwrap_or("robot-bag")
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        self.reload_messages()?;
        if self.messages.is_empty() {
            warn!(path = %self.path.display(), "robot bag loaded zero messages");
        }
        Ok(())
    }

    async fn fetch_next(&mut self, _ctx: &mut TaskContext) -> Result<SourceEvent> {
        loop {
            if let Some(event) = self.try_emit_buffered_batch("linger")? {
                return Ok(event);
            }

            if self.cursor >= self.messages.len() {
                if !self.deserializer.is_empty()
                    && let Some(batch) = self.deserializer.flush_buffer()?
                {
                    self.last_flush_time = Instant::now();
                    return Ok(SourceEvent::Data(batch));
                }
                if self.loop_replay {
                    info!(path = %self.path.display(), "robot bag loop replay");
                    self.reload_messages()?;
                    if self.messages.is_empty() {
                        return Ok(SourceEvent::Idle);
                    }
                } else {
                    return Ok(SourceEvent::EndOfStream);
                }
            }

            if !self.replay_interval.is_zero()
                && self.last_emit_time.elapsed() < self.replay_interval
            {
                tokio::time::sleep(SOURCE_POLL_TIMEOUT).await;
                continue;
            }

            let msg = self.messages[self.cursor].clone();
            self.cursor += 1;
            self.deserializer
                .deserialize_slice(&msg.payload, msg.timestamp_ms, None)?;
            self.last_emit_time = Instant::now();

            if let Some(event) = self.try_emit_buffered_batch("batch full")? {
                return Ok(event);
            }
        }
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<SourceCheckpointReport> {
        debug!(
            subtask = ctx.subtask_index,
            epoch = barrier.epoch,
            cursor = self.cursor,
            "robot-bag checkpoint (file offset v1: cursor only in memory)"
        );
        Ok(SourceCheckpointReport::default())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!(path = %self.path.display(), "robot-bag source closed");
        Ok(())
    }
}

pub fn parse_pcd_emit_mode(value: Option<&str>) -> Result<PcdEmitMode> {
    match value.map(|s| s.to_ascii_lowercase()).as_deref() {
        None | Some("point") | Some("points") => Ok(PcdEmitMode::Point),
        Some("cloud") | Some("file") => Ok(PcdEmitMode::Cloud),
        Some(other) => Err(anyhow!(
            "invalid pcd.emit.mode '{other}'; expected 'point' or 'cloud'"
        )),
    }
}
