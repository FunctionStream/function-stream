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

//! 无界/锁控 `RecordBatch` 数据源与规划期占位 `FsMemExec`。

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
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::sql::common::constants::physical_plan_node_name;

pub(crate) fn make_stream_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
    )
}

#[derive(Debug)]
pub(crate) struct RwLockRecordBatchReader {
    schema: SchemaRef,
    locked_batch: Arc<std::sync::RwLock<Option<RecordBatch>>>,
    properties: PlanProperties,
}

impl RwLockRecordBatchReader {
    pub(crate) fn new(
        schema: SchemaRef,
        locked_batch: Arc<std::sync::RwLock<Option<RecordBatch>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            locked_batch,
            properties: make_stream_properties(schema),
        }
    }
}

impl DisplayAs for RwLockRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RW Lock RecordBatchReader")
    }
}

impl ExecutionPlan for RwLockRecordBatchReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let result = self
            .locked_batch
            .write()
            .unwrap()
            .take()
            .expect("should have set a record batch before calling execute()");
        Ok(Box::pin(MemoryStream::try_new(
            vec![result],
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn name(&self) -> &str {
        physical_plan_node_name::RW_LOCK_READER
    }
}

#[derive(Debug)]
pub(crate) struct UnboundedRecordBatchReader {
    schema: SchemaRef,
    receiver: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    properties: PlanProperties,
}

impl UnboundedRecordBatchReader {
    pub(crate) fn new(
        schema: SchemaRef,
        receiver: Arc<std::sync::RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_stream_properties(schema),
        }
    }
}

impl DisplayAs for UnboundedRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "unbounded record batch reader")
    }
}

impl ExecutionPlan for UnboundedRecordBatchReader {
    fn name(&self) -> &str {
        physical_plan_node_name::UNBOUNDED_READER
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            UnboundedReceiverStream::new(
                self.receiver
                    .write()
                    .unwrap()
                    .take()
                    .expect("unbounded receiver should be present before calling exec"),
            )
            .map(Ok),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct RecordBatchVecReader {
    schema: SchemaRef,
    receiver: Arc<std::sync::RwLock<Vec<RecordBatch>>>,
    properties: PlanProperties,
}

impl RecordBatchVecReader {
    pub(crate) fn new(
        schema: SchemaRef,
        receiver: Arc<std::sync::RwLock<Vec<RecordBatch>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_stream_properties(schema),
        }
    }
}

impl DisplayAs for RecordBatchVecReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "record batch vec reader")
    }
}

impl ExecutionPlan for RecordBatchVecReader {
    fn name(&self) -> &str {
        physical_plan_node_name::VEC_READER
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let memory = MemorySourceConfig::try_new(
            &[mem::take(self.receiver.write().unwrap().as_mut())],
            self.schema.clone(),
            None,
        )?;

        DataSourceExec::new(Arc::new(memory)).execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FsMemExec {
    pub table_name: String,
    pub schema: SchemaRef,
    properties: PlanProperties,
}

impl DisplayAs for FsMemExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EmptyPartitionStream: schema={}", self.schema)
    }
}

impl FsMemExec {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self {
            schema: schema.clone(),
            table_name,
            properties: make_stream_properties(schema),
        }
    }
}

impl ExecutionPlan for FsMemExec {
    fn name(&self) -> &str {
        physical_plan_node_name::MEM_EXEC
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("with_new_children is not implemented for mem_exec; should not be called")
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        plan_err!(
            "EmptyPartitionStream cannot be executed, this is only used for physical planning before serialization"
        )
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}
