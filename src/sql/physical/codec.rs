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
use crate::sql::physical::cdc::{DebeziumUnrollingExec, ToDebeziumExec};
use crate::sql::physical::readers::{
    FsMemExec, RecordBatchVecReader, RwLockRecordBatchReader, UnboundedRecordBatchReader,
};
use crate::sql::physical::udfs::window;

#[derive(Debug)]
pub struct FsPhysicalExtensionCodec {
    pub context: DecodingContext,
}

impl Default for FsPhysicalExtensionCodec {
    fn default() -> Self {
        Self {
            context: DecodingContext::None,
        }
    }
}

#[derive(Debug)]
pub enum DecodingContext {
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

impl PhysicalExtensionCodec for FsPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec: FsExecNode = Message::decode(buf)
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {err}")))?;

        let node = exec
            .node
            .ok_or_else(|| DataFusionError::Internal("exec node is empty".to_string()))?;

        match node {
            Node::MemExec(mem) => self.decode_mem_exec(mem),
            Node::UnnestExec(unnest) => decode_unnest_exec(unnest, inputs),
            Node::DebeziumDecode(debezium) => decode_debezium_decode(debezium, inputs),
            Node::DebeziumEncode(debezium) => decode_debezium_encode(debezium, inputs),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let mut proto = None;

        if let Some(table) = node.as_any().downcast_ref::<FsMemExec>() {
            proto = Some(FsExecNode {
                node: Some(Node::MemExec(MemExecNode {
                    table_name: table.table_name.clone(),
                    schema: serde_json::to_string(&table.schema).unwrap(),
                })),
            });
        }

        if let Some(unnest) = node.as_any().downcast_ref::<UnnestExec>() {
            proto = Some(FsExecNode {
                node: Some(Node::UnnestExec(UnnestExecNode {
                    schema: serde_json::to_string(&unnest.schema()).unwrap(),
                })),
            });
        }

        if let Some(decode) = node.as_any().downcast_ref::<DebeziumUnrollingExec>() {
            proto = Some(FsExecNode {
                node: Some(Node::DebeziumDecode(DebeziumDecodeNode {
                    schema: serde_json::to_string(decode.schema().as_ref()).unwrap(),
                    primary_keys: decode
                        .primary_key_indices()
                        .iter()
                        .map(|c| *c as u64)
                        .collect(),
                })),
            });
        }

        if let Some(encode) = node.as_any().downcast_ref::<ToDebeziumExec>() {
            proto = Some(FsExecNode {
                node: Some(Node::DebeziumEncode(DebeziumEncodeNode {
                    schema: serde_json::to_string(encode.schema().as_ref()).unwrap(),
                })),
            });
        }

        if let Some(node) = proto {
            node.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("couldn't serialize exec node {err}"))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "cannot serialize {node:?}"
            )))
        }
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == window_function_udf::NAME {
            return Ok(window());
        }
        not_impl_err!("PhysicalExtensionCodec is not provided for scalar function {name}")
    }
}

impl FsPhysicalExtensionCodec {
    fn decode_mem_exec(&self, mem_exec: MemExecNode) -> Result<Arc<dyn ExecutionPlan>> {
        let schema: Schema = serde_json::from_str(&mem_exec.schema).map_err(|e| {
            DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
        })?;
        let schema = Arc::new(schema);
        match &self.context {
            DecodingContext::SingleLockedBatch(single_batch) => Ok(Arc::new(
                RwLockRecordBatchReader::new(schema, single_batch.clone()),
            )),
            DecodingContext::UnboundedBatchStream(unbounded_stream) => Ok(Arc::new(
                UnboundedRecordBatchReader::new(schema, unbounded_stream.clone()),
            )),
            DecodingContext::LockedBatchVec(locked_batches) => Ok(Arc::new(
                RecordBatchVecReader::new(schema, locked_batches.clone()),
            )),
            DecodingContext::Planning => Ok(Arc::new(FsMemExec::new(mem_exec.table_name, schema))),
            DecodingContext::None => Err(DataFusionError::Internal(
                "Need an internal context to decode".into(),
            )),
            DecodingContext::LockedJoinPair { left, right } => match mem_exec.table_name.as_str() {
                mem_exec_join_side::LEFT => {
                    Ok(Arc::new(RwLockRecordBatchReader::new(schema, left.clone())))
                }
                mem_exec_join_side::RIGHT => Ok(Arc::new(RwLockRecordBatchReader::new(
                    schema,
                    right.clone(),
                ))),
                _ => Err(DataFusionError::Internal(format!(
                    "unknown table name {}",
                    mem_exec.table_name
                ))),
            },
            DecodingContext::LockedJoinStream { left, right } => {
                match mem_exec.table_name.as_str() {
                    mem_exec_join_side::LEFT => Ok(Arc::new(UnboundedRecordBatchReader::new(
                        schema,
                        left.clone(),
                    ))),
                    mem_exec_join_side::RIGHT => Ok(Arc::new(UnboundedRecordBatchReader::new(
                        schema,
                        right.clone(),
                    ))),
                    _ => Err(DataFusionError::Internal(format!(
                        "unknown table name {}",
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
        .map_err(|e| DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}")))?;

    let column = schema.index_of(UNNESTED_COL).map_err(|_| {
        DataFusionError::Internal(format!(
            "unnest node schema does not contain {UNNESTED_COL} col"
        ))
    })?;

    Ok(Arc::new(UnnestExec::new(
        inputs
            .first()
            .ok_or_else(|| DataFusionError::Internal("no input for unnest node".to_string()))?
            .clone(),
        vec![ListUnnest {
            index_in_input_schema: column,
            depth: 1,
        }],
        vec![],
        Arc::new(schema),
        UnnestOptions::default(),
    )))
}

fn decode_debezium_decode(
    debezium: DebeziumDecodeNode,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(
        serde_json::from_str::<Schema>(&debezium.schema).map_err(|e| {
            DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
        })?,
    );
    let input = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("no input for debezium node".to_string()))?
        .clone();
    let primary_keys = debezium
        .primary_keys
        .into_iter()
        .map(|c| c as usize)
        .collect();
    Ok(Arc::new(DebeziumUnrollingExec::from_decoded_parts(
        input,
        schema.clone(),
        primary_keys,
    )))
}

fn decode_debezium_encode(
    debezium: DebeziumEncodeNode,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(
        serde_json::from_str::<Schema>(&debezium.schema).map_err(|e| {
            DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
        })?,
    );
    let input = inputs
        .first()
        .ok_or_else(|| DataFusionError::Internal("no input for debezium node".to_string()))?
        .clone();
    Ok(Arc::new(ToDebeziumExec::from_decoded_parts(input, schema)))
}
