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

use crate::sql::extensions::aggregate::AggregateExtension;
use crate::sql::extensions::async_udf::AsyncUDFExtension;
use crate::sql::extensions::debezium::{DebeziumUnrollingExtension, ToDebeziumExtension};
use crate::sql::extensions::join::JoinExtension;
use crate::sql::extensions::key_calculation::KeyCalculationExtension;
use crate::sql::extensions::lookup::LookupJoin;
use crate::sql::extensions::projection::ProjectionExtension;
use crate::sql::extensions::remote_table::RemoteTableExtension;
use crate::sql::extensions::sink::SinkExtension;
use crate::sql::extensions::stream_extension::StreamExtension;
use crate::sql::extensions::table_source::TableSourceExtension;
use crate::sql::extensions::updating_aggregate::UpdatingAggregateExtension;
use crate::sql::extensions::watermark_node::WatermarkNode;
use crate::sql::extensions::window_fn::WindowFunctionExtension;

fn try_from_t<T: StreamExtension + 'static>(
    node: &dyn UserDefinedLogicalNode,
) -> std::result::Result<&dyn StreamExtension, ()> {
    node.as_any()
        .downcast_ref::<T>()
        .map(|t| t as &dyn StreamExtension)
        .ok_or(())
}

impl<'a> TryFrom<&'a dyn UserDefinedLogicalNode> for &'a dyn StreamExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a dyn UserDefinedLogicalNode) -> Result<Self, Self::Error> {
        try_from_t::<TableSourceExtension>(node)
            .or_else(|_| try_from_t::<WatermarkNode>(node))
            .or_else(|_| try_from_t::<SinkExtension>(node))
            .or_else(|_| try_from_t::<KeyCalculationExtension>(node))
            .or_else(|_| try_from_t::<AggregateExtension>(node))
            .or_else(|_| try_from_t::<RemoteTableExtension>(node))
            .or_else(|_| try_from_t::<JoinExtension>(node))
            .or_else(|_| try_from_t::<WindowFunctionExtension>(node))
            .or_else(|_| try_from_t::<AsyncUDFExtension>(node))
            .or_else(|_| try_from_t::<ToDebeziumExtension>(node))
            .or_else(|_| try_from_t::<DebeziumUnrollingExtension>(node))
            .or_else(|_| try_from_t::<UpdatingAggregateExtension>(node))
            .or_else(|_| try_from_t::<LookupJoin>(node))
            .or_else(|_| try_from_t::<ProjectionExtension>(node))
            .map_err(|_| DataFusionError::Plan(format!("unexpected node: {}", node.name())))
    }
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn StreamExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> Result<Self, Self::Error> {
        TryFrom::try_from(node.as_ref())
    }
}
