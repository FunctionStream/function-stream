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

use std::any::Any;
use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::common::{DataFusionError, Result, Statistics, not_impl_err, plan_err};
use datafusion::datasource::memory::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::sql::common::constants::physical_plan_node_name;

/// Standard [`PlanProperties`] for a continuous, unbounded stream: incremental emission,
/// unknown partitioning, and unbounded boundedness (without requiring infinite memory).
pub(crate) fn create_unbounded_stream_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
    )
}

/// Alias for call sites that still use the older name.
pub(crate) fn make_stream_properties(schema: SchemaRef) -> PlanProperties {
    create_unbounded_stream_properties(schema)
}

// ============================================================================
// InjectableSingleBatchExec (formerly RwLockRecordBatchReader)
// ============================================================================

/// Yields exactly one [`RecordBatch`], injected via a lock before `execute` runs.
///
/// For event-driven loops that receive a single batch from the network and run a DataFusion
/// plan over it, the batch is stored in the lock until execution starts.
#[derive(Debug)]
pub(crate) struct InjectableSingleBatchExec {
    schema: SchemaRef,
    injected_batch: Arc<std::sync::RwLock<Option<RecordBatch>>>,
    properties: PlanProperties,
}

impl InjectableSingleBatchExec {
    pub(crate) fn new(
        schema: SchemaRef,
        injected_batch: Arc<std::sync::RwLock<Option<RecordBatch>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            injected_batch,
            properties: create_unbounded_stream_properties(schema),
        }
    }
}

impl DisplayAs for InjectableSingleBatchExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "InjectableSingleBatchExec")
    }
}

impl ExecutionPlan for InjectableSingleBatchExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn name(&self) -> &str {
        physical_plan_node_name::RW_LOCK_READER
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "InjectableSingleBatchExec does not support children".into(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = self.injected_batch.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire write lock: {e}"))
        })?;

        let batch = guard.take().ok_or_else(|| {
            DataFusionError::Execution(
                "Execution triggered, but no RecordBatch was injected into the node.".into(),
            )
        })?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// MpscReceiverStreamExec (formerly UnboundedRecordBatchReader)
// ============================================================================

/// Unbounded streaming source backed by a Tokio `mpsc` receiver.
///
/// Bridges async producers (e.g. network threads) into a DataFusion pipeline.
#[derive(Debug)]
pub(crate) struct MpscReceiverStreamExec {
    schema: SchemaRef,
    channel_receiver: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    properties: PlanProperties,
}

impl MpscReceiverStreamExec {
    pub(crate) fn new(
        schema: SchemaRef,
        channel_receiver: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            channel_receiver,
            properties: create_unbounded_stream_properties(schema),
        }
    }
}

impl DisplayAs for MpscReceiverStreamExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MpscReceiverStreamExec")
    }
}

impl ExecutionPlan for MpscReceiverStreamExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn name(&self) -> &str {
        physical_plan_node_name::UNBOUNDED_READER
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "MpscReceiverStreamExec does not support children".into(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = self.channel_receiver.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire lock for MPSC receiver: {e}"))
        })?;

        let receiver = guard.take().ok_or_else(|| {
            DataFusionError::Execution(
                "The MPSC receiver was already consumed by a previous execution.".into(),
            )
        })?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            UnboundedReceiverStream::new(receiver).map(Ok),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// BufferedBatchesExec (formerly RecordBatchVecReader)
// ============================================================================

/// Drains a growable, locked `Vec<RecordBatch>` when `execute` runs (micro-batching).
#[derive(Debug)]
pub(crate) struct BufferedBatchesExec {
    schema: SchemaRef,
    buffered_batches: Arc<std::sync::RwLock<Vec<RecordBatch>>>,
    properties: PlanProperties,
}

impl BufferedBatchesExec {
    pub(crate) fn new(
        schema: SchemaRef,
        buffered_batches: Arc<std::sync::RwLock<Vec<RecordBatch>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            buffered_batches,
            properties: create_unbounded_stream_properties(schema),
        }
    }
}

impl DisplayAs for BufferedBatchesExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "BufferedBatchesExec")
    }
}

impl ExecutionPlan for BufferedBatchesExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn name(&self) -> &str {
        physical_plan_node_name::VEC_READER
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "BufferedBatchesExec does not support children".into(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = self.buffered_batches.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire lock for buffered batches: {e}"))
        })?;

        let accumulated_batches = mem::take(&mut *guard);

        let memory_config =
            MemorySourceConfig::try_new(&[accumulated_batches], self.schema.clone(), None)?;

        DataSourceExec::new(Arc::new(memory_config)).execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
#[derive(Debug, Clone)]
pub struct PlanningPlaceholderExec {
    pub table_name: String,
    pub schema: SchemaRef,
    properties: PlanProperties,
}

impl PlanningPlaceholderExec {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self {
            schema: schema.clone(),
            table_name,
            properties: create_unbounded_stream_properties(schema),
        }
    }
}

impl DisplayAs for PlanningPlaceholderExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PlanningPlaceholderExec: schema={}", self.schema)
    }
}

impl ExecutionPlan for PlanningPlaceholderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn name(&self) -> &str {
        physical_plan_node_name::MEM_EXEC
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("PlanningPlaceholderExec does not accept children.")
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        plan_err!("PlanningPlaceholderExec cannot be executed; swap for a real source before run.")
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

// Backward-compatible aliases
pub type FsMemExec = PlanningPlaceholderExec;
pub type RwLockRecordBatchReader = InjectableSingleBatchExec;
pub type UnboundedRecordBatchReader = MpscReceiverStreamExec;
pub type RecordBatchVecReader = BufferedBatchesExec;
