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

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use prost::Message;

use crate::multifield_partial_ord;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::debezium::DebeziumSchemaCodec;
use crate::sql::logical_node::logical::{LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::schema::SourceTable;
use crate::sql::schema::utils::add_timestamp_field;
use crate::sql::types::schema_from_df_fields;

use super::{CompiledTopologyNode, StreamingOperatorBlueprint};

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const STREAM_INGESTION_NODE_NAME: &str = "StreamIngestionNode";

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Foundational ingestion point: connects to external systems and injects raw or CDC data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamIngestionNode {
    pub(crate) source_identifier: TableReference,
    pub(crate) source_definition: SourceTable,
    pub(crate) resolved_schema: DFSchemaRef,
}

multifield_partial_ord!(StreamIngestionNode, source_identifier, source_definition);

impl StreamIngestionNode {
    pub fn try_new(
        source_identifier: TableReference,
        source_definition: SourceTable,
    ) -> Result<Self> {
        let resolved_schema =
            Self::build_ingestion_schema(&source_identifier, &source_definition)?;

        Ok(Self {
            source_identifier,
            source_definition,
            resolved_schema,
        })
    }

    fn build_ingestion_schema(
        identifier: &TableReference,
        definition: &SourceTable,
    ) -> Result<DFSchemaRef> {
        let physical_fields: Vec<_> = definition
            .schema_specs
            .iter()
            .filter(|col| !col.is_computed())
            .map(|col| (Some(identifier.clone()), Arc::new(col.arrow_field().clone())).into())
            .collect();

        let base_schema = Arc::new(schema_from_df_fields(&physical_fields)?);

        let enveloped_schema = if definition.is_updating() {
            DebeziumSchemaCodec::wrap_into_envelope(&base_schema, Some(identifier.clone()))?
        } else {
            base_schema
        };

        add_timestamp_field(enveloped_schema, Some(identifier.clone()))
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for StreamIngestionNode {
    fn name(&self) -> &str {
        STREAM_INGESTION_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamIngestionNode({}): Schema={}",
            self.source_identifier, self.resolved_schema
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return plan_err!(
                "StreamIngestionNode acts as a leaf boundary and cannot accept upstream inputs."
            );
        }

        Ok(Self {
            source_identifier: self.source_identifier.clone(),
            source_definition: self.source_definition.clone(),
            resolved_schema: self.resolved_schema.clone(),
        })
    }
}

// -----------------------------------------------------------------------------
// Core Execution Blueprint Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for StreamIngestionNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        Some(NamedNode::Source(self.source_identifier.clone()))
    }

    fn compile_to_graph_node(
        &self,
        _compiler_context: &Planner,
        node_id_sequence: usize,
        upstream_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if !upstream_schemas.is_empty() {
            return plan_err!(
                "Topology Violation: StreamIngestionNode is a source origin and cannot process upstream routing edges."
            );
        }

        let sql_source = self.source_definition.as_sql_source()?;
        let connector_payload = sql_source.source.config.encode_to_vec();
        let operator_description = sql_source.source.config.description.clone();

        let execution_unit = LogicalNode::single(
            node_id_sequence as u32,
            format!("source_{}_{node_id_sequence}", self.source_identifier),
            OperatorName::ConnectorSource,
            connector_payload,
            operator_description,
            1,
        );

        Ok(CompiledTopologyNode::new(execution_unit, vec![]))
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_keys(Arc::new(self.resolved_schema.as_ref().into()), vec![]).expect(
            "Fatal: Failed to generate output schema for stream ingestion",
        )
    }
}
