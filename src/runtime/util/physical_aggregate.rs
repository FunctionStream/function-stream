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

use arrow::datatypes::SchemaRef;
use datafusion::common::internal_err;
use datafusion::common::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion_proto::physical_plan::from_proto::{parse_physical_expr, parse_physical_sort_expr};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_proto::protobuf::physical_aggregate_expr_node::AggregateFunction;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{PhysicalExprNode, proto_error};

pub fn decode_aggregate(
    schema: &SchemaRef,
    name: &str,
    expr: &PhysicalExprNode,
    registry: &dyn FunctionRegistry,
) -> DFResult<Arc<AggregateFunctionExpr>> {
    let codec = &DefaultPhysicalExtensionCodec {};
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty aggregate physical expression"))?;

    match expr_type {
        ExprType::AggregateExpr(agg_node) => {
            let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node
                .expr
                .iter()
                .map(|e| parse_physical_expr(e, registry, schema, codec))
                .collect::<DFResult<Vec<_>>>()?;
            let ordering_req: LexOrdering = agg_node
                .ordering_req
                .iter()
                .map(|e| parse_physical_sort_expr(e, registry, schema, codec))
                .collect::<DFResult<LexOrdering>>()?;
            agg_node
                .aggregate_function
                .as_ref()
                .map(|func| match func {
                    AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
                        let agg_udf = match &agg_node.fun_definition {
                            Some(buf) => codec.try_decode_udaf(udaf_name, buf)?,
                            None => registry.udaf(udaf_name)?,
                        };

                        AggregateExprBuilder::new(agg_udf, input_phy_expr)
                            .schema(Arc::clone(schema))
                            .alias(name)
                            .with_ignore_nulls(agg_node.ignore_nulls)
                            .with_distinct(agg_node.distinct)
                            .order_by(ordering_req)
                            .build()
                            .map(Arc::new)
                    }
                })
                .transpose()?
                .ok_or_else(|| proto_error("Invalid AggregateExpr, missing aggregate_function"))
        }
        _ => internal_err!("Invalid aggregate expression for AggregateExec"),
    }
}
