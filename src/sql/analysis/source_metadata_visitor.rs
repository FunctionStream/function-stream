use crate::sql::extensions::sink::SinkExtension;
use crate::sql::extensions::table_source::TableSourceExtension;
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
            "TableSourceExtension" => {
                let ext = node.as_any().downcast_ref::<TableSourceExtension>()?;
                ext.name.to_string()
            }
            "SinkExtension" => {
                let ext = node.as_any().downcast_ref::<SinkExtension>()?;
                ext.name.to_string()
            }
            _ => return None,
        };

        let table = self.schema_provider.get_catalog_table(&table_name)?;
        match table {
            crate::sql::schema::table::Table::ConnectorTable(t) => t.id,
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
