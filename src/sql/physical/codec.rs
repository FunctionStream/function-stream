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

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result, UnnestOptions, not_impl_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use protocol::grpc::api::{
    DebeziumDecodeNode, DebeziumEncodeNode, FsExecNode, MemExecNode, UnnestExecNode,
    fs_exec_node::Node,
};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::sql::analysis::UNNESTED_COL;
use crate::sql::common::constants::{mem_exec_join_side, window_function_udf};
use crate::sql::physical::cdc::{CdcDebeziumPackExec, CdcDebeziumUnrollExec};
use crate::sql::physical::source_exec::{
    BufferedBatchesExec, InjectableSingleBatchExec, MpscReceiverStreamExec, PlanningPlaceholderExec,
};
use crate::sql::physical::udfs::window;

// ============================================================================
// StreamingExtensionCodec & StreamingDecodingContext
// ============================================================================

/// Worker-side context used when deserializing a physical plan from the coordinator.
///
/// Planning uses [`PlanningPlaceholderExec`]; at runtime this selects the real source
/// implementation (locked batch, MPSC stream, join sides, etc.).
#[derive(Debug)]
pub enum StreamingDecodingContext {
    None,
    Planning,
    SingleLockedBatch(Arc<std::sync::RwLock<Option<RecordBatch>>>),
    UnboundedBatchStream(Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>),
    LockedBatchVec(Arc<std::sync::RwLock<Vec<RecordBatch>>>),
    LockedJoinPair {
        left: Arc<std::sync::RwLock<Option<RecordBatch>>>,
        right: Arc<std::sync::RwLock<Option<RecordBatch>>>,
    },
    LockedJoinStream {
        left: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
        right: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    },
}

/// Codec for custom streaming physical extension nodes (`FsExecNode` protobuf).
#[derive(Debug)]
pub struct StreamingExtensionCodec {
    pub context: StreamingDecodingContext,
}

impl Default for StreamingExtensionCodec {
    fn default() -> Self {
        Self {
            context: StreamingDecodingContext::None,
        }
    }
}

impl PhysicalExtensionCodec for StreamingExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec: FsExecNode = Message::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("Failed to deserialize FsExecNode protobuf: {err}"))
        })?;

        let node = exec.node.ok_or_else(|| {
            DataFusionError::Internal("Decoded FsExecNode contains no inner node data".to_string())
        })?;

        match node {
            Node::MemExec(mem) => self.decode_placeholder_exec(mem),
            Node::UnnestExec(unnest) => decode_unnest_exec(unnest, inputs),
            Node::DebeziumDecode(debezium) => decode_debezium_unroll(debezium, inputs),
            Node::DebeziumEncode(debezium) => decode_debezium_pack(debezium, inputs),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let mut proto = None;

        if let Some(table) = node.as_any().downcast_ref::<PlanningPlaceholderExec>() {
            let schema_json = serde_json::to_string(&table.schema).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize schema to JSON: {e}"))
            })?;

            proto = Some(FsExecNode {
                node: Some(Node::MemExec(MemExecNode {
                    table_name: table.table_name.clone(),
                    schema: schema_json,
                })),
            });
        } else if let Some(unnest) = node.as_any().downcast_ref::<UnnestExec>() {
            let schema_json = serde_json::to_string(&unnest.schema()).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize unnest schema to JSON: {e}"))
            })?;

            proto = Some(FsExecNode {
                node: Some(Node::UnnestExec(UnnestExecNode {
                    schema: schema_json,
                })),
            });
        } else if let Some(decode) = node.as_any().downcast_ref::<CdcDebeziumUnrollExec>() {
            let schema_json = serde_json::to_string(decode.schema().as_ref()).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize CDC unroll schema: {e}"))
            })?;

            proto = Some(FsExecNode {
                node: Some(Node::DebeziumDecode(DebeziumDecodeNode {
                    schema: schema_json,
                    primary_keys: decode
                        .primary_key_indices()
                        .iter()
                        .map(|&c| c as u64)
                        .collect(),
                })),
            });
        } else if let Some(encode) = node.as_any().downcast_ref::<CdcDebeziumPackExec>() {
            let schema_json = serde_json::to_string(encode.schema().as_ref()).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize CDC pack schema: {e}"))
            })?;

            proto = Some(FsExecNode {
                node: Some(Node::DebeziumEncode(DebeziumEncodeNode {
                    schema: schema_json,
                })),
            });
        }

        if let Some(proto_node) = proto {
            proto_node.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("Failed to encode protobuf node: {err}"))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "Cannot serialize unknown physical plan node: {node:?}"
            )))
        }
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == window_function_udf::NAME {
            return Ok(window());
        }
        not_impl_err!("PhysicalExtensionCodec does not support scalar function '{name}'")
    }
}

impl StreamingExtensionCodec {
    fn decode_placeholder_exec(&self, mem_exec: MemExecNode) -> Result<Arc<dyn ExecutionPlan>> {
        let schema: Schema = serde_json::from_str(&mem_exec.schema).map_err(|e| {
            DataFusionError::Internal(format!("Invalid schema JSON in exec codec: {e:?}"))
        })?;
        let schema = Arc::new(schema);

        match &self.context {
            StreamingDecodingContext::SingleLockedBatch(single_batch) => Ok(Arc::new(
                InjectableSingleBatchExec::new(schema, single_batch.clone()),
            )),
            StreamingDecodingContext::UnboundedBatchStream(unbounded_stream) => Ok(Arc::new(
                MpscReceiverStreamExec::new(schema, unbounded_stream.clone()),
            )),
            StreamingDecodingContext::LockedBatchVec(locked_batches) => Ok(Arc::new(
                BufferedBatchesExec::new(schema, locked_batches.clone()),
            )),
            StreamingDecodingContext::Planning => Ok(Arc::new(PlanningPlaceholderExec::new(
                mem_exec.table_name,
                schema,
            ))),
            StreamingDecodingContext::None => Err(DataFusionError::Internal(
                "A valid StreamingDecodingContext is required to decode placeholders into execution streams.".into(),
            )),
            StreamingDecodingContext::LockedJoinPair { left, right } => {
                match mem_exec.table_name.as_str() {
                    mem_exec_join_side::LEFT => Ok(Arc::new(InjectableSingleBatchExec::new(
                        schema,
                        left.clone(),
                    ))),
                    mem_exec_join_side::RIGHT => Ok(Arc::new(InjectableSingleBatchExec::new(
                        schema,
                        right.clone(),
                    ))),
                    _ => Err(DataFusionError::Internal(format!(
                        "Unknown join side table name: {}",
                        mem_exec.table_name
                    ))),
                }
            }
            StreamingDecodingContext::LockedJoinStream { left, right } => {
                match mem_exec.table_name.as_str() {
                    mem_exec_join_side::LEFT => Ok(Arc::new(MpscReceiverStreamExec::new(
                        schema,
                        left.clone(),
                    ))),
                    mem_exec_join_side::RIGHT => Ok(Arc::new(MpscReceiverStreamExec::new(
                        schema,
                        right.clone(),
                    ))),
                    _ => Err(DataFusionError::Internal(format!(
                        "Unknown join side table name: {}",
                        mem_exec.table_name
                    ))),
                }
            }
        }
    }
}

fn decode_unnest_exec(
    unnest: UnnestExecNode,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema: Schema = serde_json::from_str(&unnest.schema)
        .map_err(|e| DataFusionError::Internal(format!("Invalid unnest schema JSON: {e:?}")))?;

    let column = schema.index_of(UNNESTED_COL).map_err(|_| {
        DataFusionError::Internal(format!(
            "Unnest schema missing required column: {UNNESTED_COL}"
        ))
    })?;

    let input = inputs.first().ok_or_else(|| {
        DataFusionError::Internal("UnnestExec requires exactly one input plan".to_string())
    })?;

    Ok(Arc::new(UnnestExec::new(
        input.clone(),
        vec![ListUnnest {
            index_in_input_schema: column,
            depth: 1,
        }],
        vec![],
        Arc::new(schema),
        UnnestOptions::default(),
    )))
}

fn decode_debezium_unroll(
    debezium: DebeziumDecodeNode,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(
        serde_json::from_str::<Schema>(&debezium.schema).map_err(|e| {
            DataFusionError::Internal(format!("Invalid DebeziumDecode schema JSON: {e:?}"))
        })?,
    );

    let input = inputs.first().ok_or_else(|| {
        DataFusionError::Internal(
            "CdcDebeziumUnrollExec requires exactly one input plan".to_string(),
        )
    })?;

    let primary_keys = debezium
        .primary_keys
        .into_iter()
        .map(|c| c as usize)
        .collect();

    Ok(Arc::new(CdcDebeziumUnrollExec::from_decoded_parts(
        input.clone(),
        schema,
        primary_keys,
    )))
}

fn decode_debezium_pack(
    debezium: DebeziumEncodeNode,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(
        serde_json::from_str::<Schema>(&debezium.schema).map_err(|e| {
            DataFusionError::Internal(format!("Invalid DebeziumEncode schema JSON: {e:?}"))
        })?,
    );

    let input = inputs.first().ok_or_else(|| {
        DataFusionError::Internal("CdcDebeziumPackExec requires exactly one input plan".to_string())
    })?;

    Ok(Arc::new(CdcDebeziumPackExec::from_decoded_parts(
        input.clone(),
        schema,
    )))
}

// Historical names (same types) — keep existing `use crate::sql::physical::FsPhysicalExtensionCodec` working.
pub type FsPhysicalExtensionCodec = StreamingExtensionCodec;
pub type DecodingContext = StreamingDecodingContext;
