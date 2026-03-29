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


use anyhow::{anyhow, Result};
use arrow::compute::{max, min};
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::warn;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::factory::Registry;
use async_trait::async_trait;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{from_nanos, CheckpointBarrier, FsSchema, FsSchemaRef, Watermark};
use crate::sql::common::time_utils::print_time;
use crate::sql::physical::{DecodingContext, FsPhysicalExtensionCodec};

// ============================================================================
// ============================================================================

struct ActiveWindowExec {
    sender: Option<UnboundedSender<RecordBatch>>,
    result_stream: Option<SendableRecordBatchStream>,
}

impl ActiveWindowExec {
    fn new(
        plan: Arc<dyn ExecutionPlan>,
        hook: &Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        *hook.write().unwrap() = Some(rx);
        plan.reset()?;
        let result_stream = plan.execute(0, SessionContext::new().task_ctx())?;
        Ok(Self {
            sender: Some(tx),
            result_stream: Some(result_stream),
        })
    }

    async fn close_and_drain(&mut self) -> Result<Vec<RecordBatch>> {
        self.sender.take();
        let mut results = Vec::new();
        if let Some(mut stream) = self.result_stream.take() {
            while let Some(batch) = stream.next().await {
                results.push(batch?);
            }
        }
        Ok(results)
    }
}

// ============================================================================
// ============================================================================

pub struct WindowFunctionOperator {
    input_schema: FsSchemaRef,
    input_schema_unkeyed: FsSchemaRef,
    window_exec_plan: Arc<dyn ExecutionPlan>,
    receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    active_execs: BTreeMap<SystemTime, ActiveWindowExec>,
}

impl WindowFunctionOperator {
    fn filter_and_split_batches(
        &self,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<Vec<(RecordBatch, SystemTime)>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let timestamp_column = self.input_schema.timestamp_column(&batch);
        let min_timestamp = from_nanos(min(timestamp_column).unwrap() as u128);
        let max_timestamp = from_nanos(max(timestamp_column).unwrap() as u128);

        if let Some(wm) = watermark {
            if max_timestamp < wm {
                warn!(
                    "dropped late batch: max_ts {} < watermark {}",
                    print_time(max_timestamp),
                    print_time(wm)
                );
                return Ok(vec![]);
            }
        }

        if min_timestamp == max_timestamp {
            return Ok(vec![(batch, max_timestamp)]);
        }

        let sorted_batch = self
            .input_schema_unkeyed
            .sort(batch, true)
            .map_err(|e| anyhow!("sort for window fn: {e}"))?;
        let filtered_batch = self
            .input_schema_unkeyed
            .filter_by_time(sorted_batch, watermark)
            .map_err(|e| anyhow!("filter_by_time: {e}"))?;
        if filtered_batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let filtered_timestamps = self.input_schema.timestamp_column(&filtered_batch);
        let ranges = self
            .input_schema_unkeyed
            .partition(&filtered_batch, true)
            .map_err(|e| anyhow!("partition by time: {e}"))?;

        let mut batches = Vec::with_capacity(ranges.len());
        for range in ranges {
            let slice = filtered_batch.slice(range.start, range.end - range.start);
            let ts = from_nanos(filtered_timestamps.value(range.start) as u128);
            batches.push((slice, ts));
        }
        Ok(batches)
    }

    fn get_or_create_exec(&mut self, timestamp: SystemTime) -> Result<&mut ActiveWindowExec> {
        use std::collections::btree_map::Entry;
        match self.active_execs.entry(timestamp) {
            Entry::Vacant(v) => {
                let new_exec =
                    ActiveWindowExec::new(self.window_exec_plan.clone(), &self.receiver_hook)?;
                Ok(v.insert(new_exec))
            }
            Entry::Occupied(o) => Ok(o.into_mut()),
        }
    }
}

#[async_trait]
impl MessageOperator for WindowFunctionOperator {
    fn name(&self) -> &str {
        "WindowFunction"
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let current_watermark = ctx.last_present_watermark();
        let split_batches = self.filter_and_split_batches(batch, current_watermark)?;

        for (sub_batch, timestamp) in split_batches {
            let exec = self.get_or_create_exec(timestamp)?;
            exec.sender
                .as_ref()
                .ok_or_else(|| anyhow!("window exec sender missing"))?
                .send(sub_batch)
                .map_err(|e| anyhow!("route batch to plan: {e}"))?;
        }

        Ok(vec![])
    }

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let Watermark::EventTime(current_time) = watermark else {
            return Ok(vec![]);
        };

        let mut final_outputs = Vec::new();

        let mut expired_timestamps = Vec::new();
        for &k in self.active_execs.keys() {
            if k < current_time {
                expired_timestamps.push(k);
            } else {
                break;
            }
        }

        for ts in expired_timestamps {
            let mut exec = self
                .active_execs
                .remove(&ts)
                .ok_or_else(|| anyhow!("missing window exec"))?;
            let result_batches = exec.close_and_drain().await?;
            for batch in result_batches {
                final_outputs.push(StreamOutput::Forward(batch));
            }
        }

        Ok(final_outputs)
    }

    async fn snapshot_state(&mut self, _barrier: CheckpointBarrier, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// ============================================================================

pub struct WindowFunctionConstructor;

impl WindowFunctionConstructor {
    pub fn with_config(
        &self,
        config: protocol::grpc::api::WindowFunctionOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<WindowFunctionOperator> {
        let input_schema = Arc::new(
            FsSchema::try_from(
                config
                    .input_schema
                    .ok_or_else(|| anyhow!("missing input schema"))?,
            )
            .map_err(|e| anyhow!("input schema: {e}"))?,
        );

        let input_schema_unkeyed = Arc::new(
            FsSchema::from_schema_unkeyed(input_schema.schema.clone())
                .map_err(|e| anyhow!("unkeyed schema: {e}"))?,
        );

        let receiver_hook = Arc::new(RwLock::new(None));
        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver_hook.clone()),
        };

        let window_exec_node =
            PhysicalPlanNode::decode(&mut config.window_function_plan.as_slice())
                .map_err(|e| anyhow!("decode window_function_plan: {e}"))?;
        let window_exec_plan = window_exec_node
            .try_into_physical_plan(
                registry.as_ref(),
                &RuntimeEnvBuilder::new().build()?,
                &codec,
            )
            .map_err(|e| anyhow!("window physical plan: {e}"))?;

        Ok(WindowFunctionOperator {
            input_schema,
            input_schema_unkeyed,
            window_exec_plan,
            receiver_hook,
            active_execs: BTreeMap::new(),
        })
    }
}

