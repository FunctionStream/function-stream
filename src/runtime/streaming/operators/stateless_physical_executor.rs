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


use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;

use crate::runtime::streaming::factory::Registry;
use crate::sql::physical::{DecodingContext, FsPhysicalExtensionCodec};

pub struct StatelessPhysicalExecutor {
    batch: Arc<RwLock<Option<RecordBatch>>>,
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
}

impl StatelessPhysicalExecutor {
    pub fn new(mut proto: &[u8], registry: &Registry) -> Result<Self> {
        let batch = Arc::new(RwLock::default());

        let plan_node = PhysicalPlanNode::decode(&mut proto)
            .map_err(|e| anyhow!("decode PhysicalPlanNode: {e}"))?;
        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(batch.clone()),
        };

        let plan = plan_node.try_into_physical_plan(
            registry,
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        Ok(Self {
            batch,
            plan,
            task_context: SessionContext::new().task_ctx(),
        })
    }

    pub async fn process_batch(&mut self, batch: RecordBatch) -> Result<SendableRecordBatchStream> {
        {
            let mut writer = self
                .batch
                .write()
                .map_err(|e| anyhow!("SingleLockedBatch lock: {e}"))?;
            *writer = Some(batch);
        }
        self.plan
            .reset()
            .map_err(|e| anyhow!("reset execution plan: {e}"))?;
        self.plan
            .execute(0, self.task_context.clone())
            .map_err(|e| anyhow!("failed to compute plan: {e}"))
    }

    pub async fn process_single(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut stream = self.process_batch(batch).await?;
        let result = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("empty output stream"))??;
        anyhow::ensure!(
            stream.next().await.is_none(),
            "expected exactly one output batch"
        );
        Ok(result)
    }
}
