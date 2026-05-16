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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_array_lance::RecordBatchIterator as LanceBatchIterator;
use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use tracing::{info, warn};

use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::{CheckpointBarrier, Watermark};
use crate::streaming::StreamOutput;
use crate::streaming::api::context::TaskContext;
use crate::streaming::api::operator::{Collector, Operator};

pub struct LanceDbSinkOperator {
    table_name: String,
    dataset_uri: String,
    storage_options: HashMap<String, String>,
    pending: Vec<RecordBatch>,
    initialized: bool,
}

impl LanceDbSinkOperator {
    pub fn new(
        table_name: String,
        dataset_uri: String,
        storage_options: HashMap<String, String>,
    ) -> Self {
        Self {
            table_name,
            dataset_uri,
            storage_options,
            pending: Vec::new(),
            initialized: false,
        }
    }

    fn to_lance_batches(
        &self,
        batches: &[RecordBatch],
    ) -> Result<Vec<arrow_array_lance::RecordBatch>> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .context("lanceDB sink requires at least one record batch")?;

        let mut ipc_payload = Vec::<u8>::new();
        {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut ipc_payload, &schema)
                .context("failed to build ipc writer for lanceDB conversion")?;
            for batch in batches {
                writer
                    .write(batch)
                    .context("failed writing ipc payload for lanceDB conversion")?;
            }
            writer
                .finish()
                .context("failed finishing ipc payload for lanceDB conversion")?;
        }

        let reader = arrow_ipc_lance::reader::FileReader::try_new(Cursor::new(ipc_payload), None)
            .context("failed reading lance-compatible ipc payload")?;
        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed converting batches into lance-compatible batches")
    }

    async fn flush_epoch(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let lance_batches = self.to_lance_batches(&self.pending)?;
        let schema = lance_batches
            .first()
            .map(|b| b.schema())
            .context("lanceDB sink produced no converted batches")?;
        let reader = LanceBatchIterator::new(lance_batches.into_iter().map(Ok), schema);

        let store_params = if self.storage_options.is_empty() {
            None
        } else {
            Some(ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(self.storage_options.clone()),
                )),
                ..Default::default()
            })
        };

        let params = WriteParams {
            mode: if self.initialized {
                WriteMode::Append
            } else {
                WriteMode::Create
            },
            store_params,
            ..Default::default()
        };
        Dataset::write(reader, &self.dataset_uri, Some(params))
            .await
            .with_context(|| format!("failed writing lance dataset '{}'", self.dataset_uri))?;

        self.initialized = true;
        self.pending.clear();
        Ok(())
    }
}

#[async_trait]
impl Operator for LanceDbSinkOperator {
    fn name(&self) -> &str {
        factory_operator_name::CONNECTOR_SINK
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!(
            table = %self.table_name,
            dataset = %self.dataset_uri,
            "Starting lanceDB sink operator"
        );
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        self.pending.push(batch);
        Ok(())
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        Ok(())
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        self.flush_epoch().await
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        if !self.pending.is_empty() {
            warn!(
                table = %self.table_name,
                "flushing remaining lanceDB sink batches on close"
            );
            self.flush_epoch().await?;
        }
        Ok(vec![])
    }
}
