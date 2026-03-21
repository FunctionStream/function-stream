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

use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::UserDefinedLogicalNode;

use crate::sql::extensions::aggregate::StreamWindowAggregateNode;
use crate::sql::extensions::async_udf::AsyncFunctionExecutionNode;
use crate::sql::extensions::debezium::{PackDebeziumEnvelopeNode, UnrollDebeziumPayloadNode};
use crate::sql::extensions::join::StreamingJoinNode;
use crate::sql::extensions::key_calculation::KeyExtractionNode;
use crate::sql::extensions::lookup::StreamReferenceJoinNode;
use crate::sql::extensions::projection::StreamProjectionNode;
use crate::sql::extensions::remote_table::RemoteTableBoundaryNode;
use crate::sql::extensions::sink::StreamEgressNode;
use crate::sql::extensions::streaming_operator_blueprint::StreamingOperatorBlueprint;
use crate::sql::extensions::table_source::StreamIngestionNode;
use crate::sql::extensions::updating_aggregate::ContinuousAggregateNode;
use crate::sql::extensions::watermark_node::EventTimeWatermarkNode;
use crate::sql::extensions::windows_function::StreamingWindowFunctionNode;

fn try_from_t<T: StreamingOperatorBlueprint + 'static>(
    node: &dyn UserDefinedLogicalNode,
) -> std::result::Result<&dyn StreamingOperatorBlueprint, ()> {
    node.as_any()
        .downcast_ref::<T>()
        .map(|t| t as &dyn StreamingOperatorBlueprint)
        .ok_or(())
}

impl<'a> TryFrom<&'a dyn UserDefinedLogicalNode> for &'a dyn StreamingOperatorBlueprint {
    type Error = DataFusionError;

    fn try_from(node: &'a dyn UserDefinedLogicalNode) -> Result<Self, Self::Error> {
        try_from_t::<StreamIngestionNode>(node)
            .or_else(|_| try_from_t::<EventTimeWatermarkNode>(node))
            .or_else(|_| try_from_t::<StreamEgressNode>(node))
            .or_else(|_| try_from_t::<KeyExtractionNode>(node))
            .or_else(|_| try_from_t::<StreamWindowAggregateNode>(node))
            .or_else(|_| try_from_t::<RemoteTableBoundaryNode>(node))
            .or_else(|_| try_from_t::<StreamingJoinNode>(node))
            .or_else(|_| try_from_t::<StreamingWindowFunctionNode>(node))
            .or_else(|_| try_from_t::<AsyncFunctionExecutionNode>(node))
            .or_else(|_| try_from_t::<PackDebeziumEnvelopeNode>(node))
            .or_else(|_| try_from_t::<UnrollDebeziumPayloadNode>(node))
            .or_else(|_| try_from_t::<ContinuousAggregateNode>(node))
            .or_else(|_| try_from_t::<StreamReferenceJoinNode>(node))
            .or_else(|_| try_from_t::<StreamProjectionNode>(node))
            .map_err(|_| DataFusionError::Plan(format!("unexpected node: {}", node.name())))
    }
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn StreamingOperatorBlueprint {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> Result<Self, Self::Error> {
        TryFrom::try_from(node.as_ref())
    }
}
