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

//! Arrow Schema IPC and DataFusion logical plan serialization.

use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;

pub struct CatalogCodec;

impl CatalogCodec {
    pub fn encode_schema(schema: &Arc<Schema>) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let empty_batch = RecordBatch::new_empty(Arc::clone(schema));
        let mut writer = StreamWriter::try_new(&mut buffer, schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        writer
            .write(&empty_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        writer
            .finish()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(buffer)
    }

    pub fn decode_schema(bytes: &[u8]) -> Result<Arc<Schema>> {
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(reader.schema())
    }

    pub fn encode_logical_plan(plan: &LogicalPlan) -> Result<Vec<u8>> {
        datafusion_proto::bytes::logical_plan_to_bytes(plan).map(|b| b.to_vec())
    }

    pub fn decode_logical_plan(bytes: &[u8], ctx: &SessionContext) -> Result<LogicalPlan> {
        datafusion_proto::bytes::logical_plan_from_bytes(bytes, ctx)
    }
}
