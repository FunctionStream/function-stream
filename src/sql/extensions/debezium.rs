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

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::{
    internal_err, plan_err, DFSchema, DFSchemaRef, DataFusionError, Result, TableReference,
};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::DisplayAs;

use crate::multifield_partial_ord;
use crate::sql::common::constants::{cdc, extension_node};
use crate::sql::common::{FsSchema, FsSchemaRef, UPDATING_META_FIELD};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::logical_planner::updating_meta_field;
use crate::sql::types::TIMESTAMP_FIELD;

use super::{CompiledTopologyNode, StreamingOperatorBlueprint};

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const UNROLL_NODE_NAME: &str = extension_node::UNROLL_DEBEZIUM_PAYLOAD;
pub(crate) const PACK_NODE_NAME: &str = extension_node::PACK_DEBEZIUM_ENVELOPE;

// -----------------------------------------------------------------------------
// Core Schema Codec
// -----------------------------------------------------------------------------

/// Transforms between flat schemas and Debezium CDC envelopes.
pub(crate) struct DebeziumSchemaCodec;

impl DebeziumSchemaCodec {
    /// Wraps a flat physical schema into a Debezium CDC envelope structure.
    pub(crate) fn wrap_into_envelope(
        flat_schema: &DFSchemaRef,
        qualifier_override: Option<TableReference>,
    ) -> Result<DFSchemaRef> {
        let ts_field = if flat_schema.has_column_with_unqualified_name(TIMESTAMP_FIELD) {
            Some(flat_schema.field_with_unqualified_name(TIMESTAMP_FIELD)?.clone())
        } else {
            None
        };

        let payload_fields: Vec<_> = flat_schema
            .fields()
            .iter()
            .filter(|f| f.name() != TIMESTAMP_FIELD && f.name() != UPDATING_META_FIELD)
            .cloned()
            .collect();

        let payload_struct_type = DataType::Struct(payload_fields.into());

        let mut envelope_fields = vec![
            Arc::new(Field::new(
                cdc::BEFORE,
                payload_struct_type.clone(),
                true,
            )),
            Arc::new(Field::new(cdc::AFTER, payload_struct_type, true)),
            Arc::new(Field::new(cdc::OP, DataType::Utf8, true)),
        ];

        if let Some(ts) = ts_field {
            envelope_fields.push(Arc::new(ts));
        }

        let arrow_schema = Schema::new(envelope_fields);
        let final_schema = match qualifier_override {
            Some(qualifier) => DFSchema::try_from_qualified_schema(qualifier, &arrow_schema)?,
            None => DFSchema::try_from(arrow_schema)?,
        };

        Ok(Arc::new(final_schema))
    }
}

// -----------------------------------------------------------------------------
// Logical Node: Unroll Debezium Payload
// -----------------------------------------------------------------------------

/// Decodes an incoming Debezium envelope into a flat, updating stream representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnrollDebeziumPayloadNode {
    upstream_plan: LogicalPlan,
    resolved_schema: DFSchemaRef,
    pub pk_indices: Vec<usize>,
    pk_names: Arc<Vec<String>>,
}

multifield_partial_ord!(
    UnrollDebeziumPayloadNode,
    upstream_plan,
    pk_indices,
    pk_names
);

impl UnrollDebeziumPayloadNode {
    pub fn try_new(upstream_plan: LogicalPlan, pk_names: Arc<Vec<String>>) -> Result<Self> {
        let input_schema = upstream_plan.schema();

        let (before_idx, after_idx) = Self::validate_envelope_structure(input_schema)?;

        let payload_fields = Self::extract_payload_fields(input_schema, before_idx)?;

        let pk_indices = Self::map_primary_keys(payload_fields, &pk_names)?;

        let qualifier = Self::resolve_schema_qualifier(input_schema, before_idx, after_idx)?;

        let resolved_schema =
            Self::compile_unrolled_schema(input_schema, payload_fields, qualifier)?;

        Ok(Self {
            upstream_plan,
            resolved_schema,
            pk_indices,
            pk_names,
        })
    }

    fn validate_envelope_structure(schema: &DFSchemaRef) -> Result<(usize, usize)> {
        let before_idx = schema.index_of_column_by_name(None, cdc::BEFORE).ok_or_else(
            || DataFusionError::Plan("Missing 'before' state column in CDC stream".into()),
        )?;

        let after_idx = schema.index_of_column_by_name(None, cdc::AFTER).ok_or_else(
            || DataFusionError::Plan("Missing 'after' state column in CDC stream".into()),
        )?;

        let op_idx = schema.index_of_column_by_name(None, cdc::OP).ok_or_else(|| {
            DataFusionError::Plan("Missing 'op' operation column in CDC stream".into())
        })?;

        let before_type = schema.field(before_idx).data_type();
        let after_type = schema.field(after_idx).data_type();

        if before_type != after_type {
            return plan_err!(
                "State column type mismatch: 'before' is {before_type}, but 'after' is {after_type}"
            );
        }

        if *schema.field(op_idx).data_type() != DataType::Utf8 {
            return plan_err!(
                "The '{}' column must be of type Utf8",
                cdc::OP
            );
        }

        Ok((before_idx, after_idx))
    }

    fn extract_payload_fields<'a>(
        schema: &'a DFSchemaRef,
        state_idx: usize,
    ) -> Result<&'a arrow_schema::Fields> {
        match schema.field(state_idx).data_type() {
            DataType::Struct(fields) => Ok(fields),
            other => plan_err!("State columns must be of type Struct, found {other}"),
        }
    }

    fn map_primary_keys(
        fields: &arrow_schema::Fields,
        pk_names: &[String],
    ) -> Result<Vec<usize>> {
        pk_names
            .iter()
            .map(|pk| fields.find(pk).map(|(idx, _)| idx))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Plan("Specified primary key not found in payload schema".into())
            })
    }

    fn resolve_schema_qualifier(
        schema: &DFSchemaRef,
        before_idx: usize,
        after_idx: usize,
    ) -> Result<Option<TableReference>> {
        let before_qualifier = schema.qualified_field(before_idx).0;
        let after_qualifier = schema.qualified_field(after_idx).0;

        match (before_qualifier, after_qualifier) {
            (Some(bq), Some(aq)) if bq == aq => Ok(Some(bq.clone())),
            (None, None) => Ok(None),
            _ => plan_err!(
                "'before' and 'after' columns must share the same namespace/qualifier"
            ),
        }
    }

    fn compile_unrolled_schema(
        original_schema: &DFSchemaRef,
        payload_fields: &arrow_schema::Fields,
        qualifier: Option<TableReference>,
    ) -> Result<DFSchemaRef> {
        let mut flat_fields = payload_fields.to_vec();

        flat_fields.push(updating_meta_field());

        let ts_idx = original_schema
            .index_of_column_by_name(None, TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Required event time field '{TIMESTAMP_FIELD}' is missing"
                ))
            })?;

        flat_fields.push(Arc::new(original_schema.field(ts_idx).clone()));

        let arrow_schema = Schema::new(flat_fields);
        let compiled_schema = match qualifier {
            Some(q) => DFSchema::try_from_qualified_schema(q, &arrow_schema)?,
            None => DFSchema::try_from(arrow_schema)?,
        };

        Ok(Arc::new(compiled_schema))
    }
}

impl UserDefinedLogicalNodeCore for UnrollDebeziumPayloadNode {
    fn name(&self) -> &str {
        UNROLL_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UnrollDebeziumPayload")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "UnrollDebeziumPayloadNode expects exactly 1 input, got {}",
                inputs.len()
            );
        }
        Self::try_new(inputs.remove(0), self.pk_names.clone())
    }
}

impl StreamingOperatorBlueprint for UnrollDebeziumPayloadNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn is_passthrough_boundary(&self) -> bool {
        true
    }

    fn compile_to_graph_node(
        &self,
        _: &Planner,
        _: usize,
        _: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        plan_err!("UnrollDebeziumPayloadNode is a logical boundary and should not be physically planned")
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(Arc::new(self.resolved_schema.as_ref().into())).unwrap_or_else(
            |_| panic!("Failed to extract physical schema for {}", UNROLL_NODE_NAME),
        )
    }
}

// -----------------------------------------------------------------------------
// Logical Node: Pack Debezium Envelope
// -----------------------------------------------------------------------------

/// Encodes a flat updating stream back into a Debezium CDC envelope representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PackDebeziumEnvelopeNode {
    upstream_plan: Arc<LogicalPlan>,
    envelope_schema: DFSchemaRef,
}

multifield_partial_ord!(PackDebeziumEnvelopeNode, upstream_plan);

impl PackDebeziumEnvelopeNode {
    pub(crate) fn try_new(upstream_plan: LogicalPlan) -> Result<Self> {
        let envelope_schema = DebeziumSchemaCodec::wrap_into_envelope(upstream_plan.schema(), None)
            .map_err(|e| {
                DataFusionError::Plan(format!("Failed to compile Debezium envelope schema: {e}"))
            })?;

        Ok(Self {
            upstream_plan: Arc::new(upstream_plan),
            envelope_schema,
        })
    }
}

impl DisplayAs for PackDebeziumEnvelopeNode {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PackDebeziumEnvelope")
    }
}

impl UserDefinedLogicalNodeCore for PackDebeziumEnvelopeNode {
    fn name(&self) -> &str {
        PACK_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.envelope_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PackDebeziumEnvelope")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "PackDebeziumEnvelopeNode expects exactly 1 input, got {}",
                inputs.len()
            );
        }
        Self::try_new(inputs.remove(0))
    }
}

impl StreamingOperatorBlueprint for PackDebeziumEnvelopeNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn is_passthrough_boundary(&self) -> bool {
        true
    }

    fn compile_to_graph_node(
        &self,
        _: &Planner,
        _: usize,
        _: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        internal_err!("PackDebeziumEnvelopeNode is a logical boundary and should not be physically planned")
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(Arc::new(self.envelope_schema.as_ref().into()))
            .unwrap_or_else(|_| {
                panic!("Failed to extract physical schema for {}", PACK_NODE_NAME)
            })
    }
}
