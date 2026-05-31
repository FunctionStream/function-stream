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

//! ROS1 rosbag (v1/v2) loader using the `rosbag` crate.

use anyhow::{Context as _, Result, anyhow, bail};
use rosbag::{ChunkRecord, IndexRecord, MessageRecord, RosBag, record_types::MessageData};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use tracing::info;

pub fn is_ros1_bag_file(path: &Path) -> bool {
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

pub fn load_ros1_bag(path: &Path, topic_filter: Option<&str>) -> Result<Vec<(Vec<u8>, u64)>> {
    if !path.is_file() {
        bail!("ROS1 bag path '{}' must be a .bag file", path.display());
    }
    if !is_ros1_bag_file(path) {
        bail!(
            "file '{}' is not a ROS1 rosbag (expected header '#ROSBAG V2.0' or '#ROSBAG V1.02')",
            path.display()
        );
    }

    let bag = RosBag::new(path).with_context(|| format!("open ros1 bag {}", path.display()))?;

    let mut conn_topics: HashMap<u32, (String, String)> = HashMap::new();

    for record in bag.index_records() {
        match record.context("ros1 bag index record")? {
            IndexRecord::Connection(conn) => {
                conn_topics.insert(
                    conn.id,
                    (conn.topic.to_string(), conn.tp.to_string()),
                );
            }
            IndexRecord::IndexData(_) | IndexRecord::ChunkInfo(_) => {}
        }
    }

    let mut messages = Vec::new();

    for record in bag.chunk_records() {
        match record.context("ros1 bag chunk record")? {
            ChunkRecord::Chunk(chunk) => {
                let mut chunk_messages = Vec::new();
                for msg in chunk.messages() {
                    match msg.context("ros1 bag message record")? {
                        MessageRecord::Connection(conn) => {
                            conn_topics.insert(
                                conn.id,
                                (conn.topic.to_string(), conn.tp.to_string()),
                            );
                        }
                        MessageRecord::MessageData(msg_data) => {
                            chunk_messages.push(msg_data);
                        }
                    }
                }
                for msg_data in chunk_messages {
                    push_ros1_message(&mut messages, &conn_topics, msg_data, topic_filter);
                }
            }
            ChunkRecord::IndexData(_) => {}
        }
    }

    messages.sort_by_key(|m| m.1);

    info!(
        path = %path.display(),
        messages = messages.len(),
        topic_filter = ?topic_filter,
        connections = conn_topics.len(),
        "loaded ROS1 rosbag"
    );

    if messages.is_empty() && topic_filter.is_some() {
        return Err(anyhow!(
            "ROS1 bag '{}' has no messages for topic '{}'",
            path.display(),
            topic_filter.unwrap_or("")
        ));
    }

    Ok(messages)
}

fn push_ros1_message(
    out: &mut Vec<(Vec<u8>, u64)>,
    conn_topics: &HashMap<u32, (String, String)>,
    msg_data: MessageData<'_>,
    topic_filter: Option<&str>,
) {
    let Some((topic, _)) = conn_topics.get(&msg_data.conn_id) else {
        return;
    };
    if let Some(filter) = topic_filter
        && topic != filter
    {
        return;
    }
    out.push((msg_data.data.to_vec(), msg_data.time / 1_000_000));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_non_bag_file() {
        let dir = std::env::temp_dir().join("fs_ros1_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("not_bag.txt");
        std::fs::write(&path, b"hello").expect("write");
        assert!(!is_ros1_bag_file(&path));
        let err = load_ros1_bag(&path, None).unwrap_err();
        assert!(err.to_string().contains("not a ROS1 rosbag"));
        let _ = std::fs::remove_file(path);
    }
}
