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

//! Protobuf wire format for RocksDB task rows, with legacy bincode read support.

use anyhow::{Context, Result, anyhow};
use prost::Message;
use protocol::storage::{
    ComponentStateKind, ComponentStateProto, TaskMetadataProto, TaskModulePayloadProto,
    TaskModulePython, TaskModuleWasm, task_module_payload_proto,
};
use serde::{Deserialize, Serialize};

use crate::runtime::common::ComponentState;

use super::storage::TaskModuleBytes;

/// Magic prefix for protobuf-encoded task values (meta + payload). Legacy rows have no prefix.
pub const TASK_STORAGE_PROTO_MAGIC: &[u8; 4] = b"FSP1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyTaskMetadata {
    task_type: String,
    state: ComponentState,
    created_at: u64,
    checkpoint_id: Option<u64>,
}

fn component_state_to_proto(state: &ComponentState) -> ComponentStateProto {
    let (kind, error_message) = match state {
        ComponentState::Uninitialized => (ComponentStateKind::Uninitialized, String::new()),
        ComponentState::Initialized => (ComponentStateKind::Initialized, String::new()),
        ComponentState::Starting => (ComponentStateKind::Starting, String::new()),
        ComponentState::Running => (ComponentStateKind::Running, String::new()),
        ComponentState::Checkpointing => (ComponentStateKind::Checkpointing, String::new()),
        ComponentState::Stopping => (ComponentStateKind::Stopping, String::new()),
        ComponentState::Stopped => (ComponentStateKind::Stopped, String::new()),
        ComponentState::Closing => (ComponentStateKind::Closing, String::new()),
        ComponentState::Closed => (ComponentStateKind::Closed, String::new()),
        ComponentState::Error { error } => (ComponentStateKind::Error, error.clone()),
    };
    ComponentStateProto {
        kind: kind as i32,
        error_message,
    }
}

fn component_state_from_proto(p: &ComponentStateProto) -> ComponentState {
    let kind = ComponentStateKind::try_from(p.kind).unwrap_or(ComponentStateKind::Unspecified);
    match kind {
        ComponentStateKind::Unspecified | ComponentStateKind::Uninitialized => {
            ComponentState::Uninitialized
        }
        ComponentStateKind::Initialized => ComponentState::Initialized,
        ComponentStateKind::Starting => ComponentState::Starting,
        ComponentStateKind::Running => ComponentState::Running,
        ComponentStateKind::Checkpointing => ComponentState::Checkpointing,
        ComponentStateKind::Stopping => ComponentState::Stopping,
        ComponentStateKind::Stopped => ComponentState::Stopped,
        ComponentStateKind::Closing => ComponentState::Closing,
        ComponentStateKind::Closed => ComponentState::Closed,
        ComponentStateKind::Error => ComponentState::Error {
            error: if p.error_message.is_empty() {
                "unknown error".to_string()
            } else {
                p.error_message.clone()
            },
        },
    }
}

/// Encode task metadata for `task_meta` column family (always protobuf + magic).
pub fn encode_task_metadata_bytes(
    task_type: &str,
    state: &ComponentState,
    created_at: u64,
    checkpoint_id: Option<u64>,
) -> Result<Vec<u8>> {
    let proto = TaskMetadataProto {
        task_type: task_type.to_string(),
        state: Some(component_state_to_proto(state)),
        created_at,
        checkpoint_id,
    };
    let mut out = TASK_STORAGE_PROTO_MAGIC.to_vec();
    proto.encode(&mut out).context("encode TaskMetadataProto")?;
    Ok(out)
}

pub struct DecodedTaskMetadata {
    pub task_type: String,
    pub state: ComponentState,
    pub created_at: u64,
    pub checkpoint_id: Option<u64>,
}

/// Decode metadata written by this version (protobuf) or legacy bincode+serde.
pub fn decode_task_metadata_bytes(raw: &[u8]) -> Result<DecodedTaskMetadata> {
    if raw.len() >= TASK_STORAGE_PROTO_MAGIC.len()
        && &raw[..TASK_STORAGE_PROTO_MAGIC.len()] == TASK_STORAGE_PROTO_MAGIC.as_slice()
    {
        let proto = TaskMetadataProto::decode(&raw[TASK_STORAGE_PROTO_MAGIC.len()..])
            .context("decode TaskMetadataProto")?;
        let state = proto
            .state
            .as_ref()
            .map(component_state_from_proto)
            .unwrap_or_default();
        return Ok(DecodedTaskMetadata {
            task_type: proto.task_type,
            state,
            created_at: proto.created_at,
            checkpoint_id: proto.checkpoint_id,
        });
    }

    let (legacy, _): (LegacyTaskMetadata, _) =
        bincode::serde::decode_from_slice(raw, bincode::config::standard())
            .map_err(|e| anyhow!("legacy task metadata bincode decode failed: {e}"))?;
    Ok(DecodedTaskMetadata {
        task_type: legacy.task_type,
        state: legacy.state,
        created_at: legacy.created_at,
        checkpoint_id: legacy.checkpoint_id,
    })
}

fn module_to_proto(module: &TaskModuleBytes) -> TaskModulePayloadProto {
    match module {
        TaskModuleBytes::Wasm(bytes) => TaskModulePayloadProto {
            payload: Some(task_module_payload_proto::Payload::Wasm(TaskModuleWasm {
                wasm_binary: bytes.clone(),
            })),
        },
        TaskModuleBytes::Python {
            class_name,
            module,
            bytes,
        } => TaskModulePayloadProto {
            payload: Some(task_module_payload_proto::Payload::Python(
                TaskModulePython {
                    class_name: class_name.clone(),
                    module_path: module.clone(),
                    embedded_code: bytes.clone(),
                },
            )),
        },
    }
}

/// Encode module payload for `task_payload` column family (always protobuf + magic).
pub fn encode_task_module_bytes(module: &TaskModuleBytes) -> Result<Vec<u8>> {
    let proto = module_to_proto(module);
    let mut out = TASK_STORAGE_PROTO_MAGIC.to_vec();
    proto
        .encode(&mut out)
        .context("encode TaskModulePayloadProto")?;
    Ok(out)
}

/// Decode module payload: protobuf+magic or legacy bincode+serde [`TaskModuleBytes`].
pub fn decode_task_module_bytes(raw: &[u8]) -> Result<TaskModuleBytes> {
    if raw.len() >= TASK_STORAGE_PROTO_MAGIC.len()
        && &raw[..TASK_STORAGE_PROTO_MAGIC.len()] == TASK_STORAGE_PROTO_MAGIC.as_slice()
    {
        let proto = TaskModulePayloadProto::decode(&raw[TASK_STORAGE_PROTO_MAGIC.len()..])
            .context("decode TaskModulePayloadProto")?;
        return proto.try_into_task_module();
    }

    let (legacy, _): (TaskModuleBytes, _) =
        bincode::serde::decode_from_slice(raw, bincode::config::standard())
            .map_err(|e| anyhow!("legacy task module bincode decode failed: {e}"))?;
    Ok(legacy)
}

trait TryIntoTaskModule {
    fn try_into_task_module(self) -> Result<TaskModuleBytes>;
}

impl TryIntoTaskModule for TaskModulePayloadProto {
    fn try_into_task_module(self) -> Result<TaskModuleBytes> {
        match self.payload {
            Some(task_module_payload_proto::Payload::Wasm(w)) => {
                Ok(TaskModuleBytes::Wasm(w.wasm_binary))
            }
            Some(task_module_payload_proto::Payload::Python(p)) => Ok(TaskModuleBytes::Python {
                class_name: p.class_name,
                module: p.module_path,
                bytes: p.embedded_code,
            }),
            None => Err(anyhow!("TaskModulePayloadProto missing payload")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_roundtrip_proto() {
        let enc =
            encode_task_metadata_bytes("wasm", &ComponentState::Running, 42, Some(7)).unwrap();
        let dec = decode_task_metadata_bytes(&enc).unwrap();
        assert_eq!(dec.task_type, "wasm");
        assert_eq!(dec.state, ComponentState::Running);
        assert_eq!(dec.created_at, 42);
        assert_eq!(dec.checkpoint_id, Some(7));
    }

    #[test]
    fn module_roundtrip_wasm_proto() {
        let m = TaskModuleBytes::Wasm(vec![1, 2, 3]);
        let enc = encode_task_module_bytes(&m).unwrap();
        let dec = decode_task_module_bytes(&enc).unwrap();
        assert_eq!(dec, m);
    }

    #[test]
    fn module_roundtrip_python_proto() {
        let m = TaskModuleBytes::Python {
            class_name: "C".into(),
            module: "m".into(),
            bytes: Some(vec![9]),
        };
        let enc = encode_task_module_bytes(&m).unwrap();
        let dec = decode_task_module_bytes(&enc).unwrap();
        assert_eq!(dec, m);
    }

    #[test]
    fn legacy_bincode_metadata_still_decodes() {
        let legacy = LegacyTaskMetadata {
            task_type: "legacy".into(),
            state: ComponentState::Stopped,
            created_at: 99,
            checkpoint_id: None,
        };
        let raw = bincode::serde::encode_to_vec(&legacy, bincode::config::standard()).unwrap();
        let dec = decode_task_metadata_bytes(&raw).unwrap();
        assert_eq!(dec.task_type, "legacy");
        assert_eq!(dec.state, ComponentState::Stopped);
        assert_eq!(dec.created_at, 99);
    }

    #[test]
    fn legacy_bincode_module_still_decodes() {
        let m = TaskModuleBytes::Wasm(vec![8, 9]);
        let raw = bincode::serde::encode_to_vec(&m, bincode::config::standard()).unwrap();
        assert_eq!(decode_task_module_bytes(&raw).unwrap(), m);
    }
}
