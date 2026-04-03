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

use crate::sql::extensions::sink::{STREAM_EGRESS_NODE_NAME, StreamEgressNode};
use crate::sql::extensions::table_source::{STREAM_INGESTION_NODE_NAME, StreamIngestionNode};
use crate::sql::schema::StreamSchemaProvider;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::logical_expr::{Extension, LogicalPlan};
use std::collections::HashSet;

/// Collects connection IDs from source and sink nodes in the logical plan.
pub struct SourceMetadataVisitor<'a> {
    schema_provider: &'a StreamSchemaProvider,
    pub connection_ids: HashSet<i64>,
}

impl<'a> SourceMetadataVisitor<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self {
            schema_provider,
            connection_ids: HashSet::new(),
        }
    }

    fn get_connection_id(&self, node: &LogicalPlan) -> Option<i64> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return None;
        };

        let table_name = match node.name() {
            name if name == STREAM_INGESTION_NODE_NAME => {
                let ext = node.as_any().downcast_ref::<StreamIngestionNode>()?;
                ext.source_identifier.to_string()
            }
            name if name == STREAM_EGRESS_NODE_NAME => {
                let ext = node.as_any().downcast_ref::<StreamEgressNode>()?;
                ext.target_identifier.to_string()
            }
            _ => return None,
        };

        let table = self.schema_provider.get_catalog_table(&table_name)?;
        match table {
            crate::sql::schema::table::Table::ConnectorTable(t) => t.registry_id,
            _ => None,
        }
    }
}

impl TreeNodeVisitor<'_> for SourceMetadataVisitor<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Some(id) = self.get_connection_id(node) {
            self.connection_ids.insert(id);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
