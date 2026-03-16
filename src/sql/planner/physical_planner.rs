use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use datafusion::arrow::datatypes::IntervalMonthDayNanoType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, Spans, plan_err,
};
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions::datetime::date_bin;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{AggregateMode, physical_plan_node::PhysicalPlanType},
};
use petgraph::graph::{DiGraph, NodeIndex};
use prost::Message;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

use async_trait::async_trait;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;

use crate::datastream::logical::{LogicalEdge, LogicalGraph, LogicalNode};
use crate::sql::physical::{
    DebeziumUnrollingExec, DecodingContext, FsMemExec, FsPhysicalExtensionCodec, ToDebeziumExec,
};
use crate::sql::planner::StreamSchemaProvider;
use crate::sql::planner::extension::debezium::{
    DEBEZIUM_UNROLLING_EXTENSION_NAME, DebeziumUnrollingExtension, TO_DEBEZIUM_EXTENSION_NAME,
};
use crate::sql::planner::extension::key_calculation::KeyCalculationExtension;
use crate::sql::planner::extension::{NamedNode, NodeWithIncomingEdges, StreamExtension};
use crate::sql::planner::schemas::add_timestamp_field_arrow;
use crate::types::{FsSchema, FsSchemaRef};

pub(crate) struct PlanToGraphVisitor<'a> {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    output_schemas: HashMap<NodeIndex, FsSchemaRef>,
    named_nodes: HashMap<NamedNode, NodeIndex>,
    traversal: Vec<Vec<NodeIndex>>,
    planner: Planner<'a>,
}

impl<'a> PlanToGraphVisitor<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider, session_state: &'a SessionState) -> Self {
        Self {
            graph: Default::default(),
            output_schemas: Default::default(),
            named_nodes: Default::default(),
            traversal: vec![],
            planner: Planner::new(schema_provider, session_state),
        }
    }
}

pub(crate) struct Planner<'a> {
    schema_provider: &'a StreamSchemaProvider,
    planner: DefaultPhysicalPlanner,
    session_state: &'a SessionState,
}

impl<'a> Planner<'a> {
    pub(crate) fn new(
        schema_provider: &'a StreamSchemaProvider,
        session_state: &'a SessionState,
    ) -> Self {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(FsExtensionPlanner {})]);
        Self {
            schema_provider,
            planner,
            session_state,
        }
    }

    pub(crate) fn sync_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let fut = self.planner.create_physical_plan(plan, self.session_state);
        let (tx, mut rx) = oneshot::channel();
        thread::scope(|s| {
            let _handle = tokio::runtime::Handle::current();
            let builder = thread::Builder::new();
            let builder = if cfg!(debug_assertions) {
                builder.stack_size(10_000_000)
            } else {
                builder
            };
            builder
                .spawn_scoped(s, move || {
                    let rt = Builder::new_current_thread().enable_all().build().unwrap();
                    rt.block_on(async {
                        let plan = fut.await;
                        tx.send(plan).unwrap();
                    });
                })
                .unwrap();
        });

        rx.try_recv().unwrap()
    }

    pub(crate) fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.planner
            .create_physical_expr(expr, input_dfschema, self.session_state)
    }

    pub(crate) fn serialize_as_physical_expr(
        &self,
        expr: &Expr,
        schema: &DFSchema,
    ) -> Result<Vec<u8>> {
        let physical = self.create_physical_expr(expr, schema)?;
        let proto = serialize_physical_expr(&physical, &DefaultPhysicalExtensionCodec {})?;
        Ok(proto.encode_to_vec())
    }

    pub(crate) fn split_physical_plan(
        &self,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
        add_timestamp_field: bool,
    ) -> Result<SplitPlanOutput> {
        let physical_plan = self.sync_plan(aggregate)?;
        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let mut physical_plan_node =
            PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;
        let PhysicalPlanType::Aggregate(mut final_aggregate_proto) = physical_plan_node
            .physical_plan_type
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing physical plan type".to_string()))?
        else {
            return plan_err!("unexpected physical plan type");
        };
        let AggregateMode::Final = final_aggregate_proto.mode() else {
            return plan_err!("unexpected physical plan type");
        };

        let partial_aggregation_plan = *final_aggregate_proto
            .input
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing input".to_string()))?;

        let partial_aggregation_exec_plan = partial_aggregation_plan.try_into_physical_plan(
            self.schema_provider,
            &RuntimeEnvBuilder::new().build().unwrap(),
            &codec,
        )?;

        let partial_schema = partial_aggregation_exec_plan.schema();
        let final_input_table_provider = FsMemExec::new("partial".into(), partial_schema.clone());

        final_aggregate_proto.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(final_input_table_provider),
            &codec,
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(final_aggregate_proto)),
        };

        let (partial_schema, timestamp_index) = if add_timestamp_field {
            (
                add_timestamp_field_arrow((*partial_schema).clone()),
                partial_schema.fields().len(),
            )
        } else {
            (partial_schema.clone(), partial_schema.fields().len() - 1)
        };

        let partial_schema = FsSchema::new_keyed(partial_schema, timestamp_index, key_indices);

        Ok(SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        })
    }

    pub fn binning_function_proto(
        &self,
        width: Duration,
        input_schema: DFSchemaRef,
    ) -> Result<PhysicalExprNode> {
        let date_bin = date_bin().call(vec![
            Expr::Literal(
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNanoType::make_value(
                    0,
                    0,
                    width.as_nanos() as i64,
                ))),
                None,
            ),
            Expr::Column(datafusion::common::Column {
                relation: None,
                name: "_timestamp".into(),
                spans: Spans::new(),
            }),
        ]);

        let binning_function = self.create_physical_expr(&date_bin, &input_schema)?;
        serialize_physical_expr(&binning_function, &DefaultPhysicalExtensionCodec {})
    }
}

struct FsExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for FsExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let schema = node.schema().as_ref().into();
        if let Ok::<&dyn StreamExtension, _>(stream_extension) = node.try_into() {
            if stream_extension.transparent() {
                match node.name() {
                    DEBEZIUM_UNROLLING_EXTENSION_NAME => {
                        let node = node
                            .as_any()
                            .downcast_ref::<DebeziumUnrollingExtension>()
                            .unwrap();
                        let input = physical_inputs[0].clone();
                        return Ok(Some(Arc::new(DebeziumUnrollingExec::try_new(
                            input,
                            node.primary_keys.clone(),
                        )?)));
                    }
                    TO_DEBEZIUM_EXTENSION_NAME => {
                        let input = physical_inputs[0].clone();
                        return Ok(Some(Arc::new(ToDebeziumExec::try_new(input)?)));
                    }
                    _ => return Ok(None),
                }
            }
        };
        let name =
            if let Some(key_extension) = node.as_any().downcast_ref::<KeyCalculationExtension>() {
                key_extension.name.clone()
            } else {
                None
            };
        Ok(Some(Arc::new(FsMemExec::new(
            name.unwrap_or("memory".to_string()),
            Arc::new(schema),
        ))))
    }
}

impl PlanToGraphVisitor<'_> {
    fn add_index_to_traversal(&mut self, index: NodeIndex) {
        if let Some(last) = self.traversal.last_mut() {
            last.push(index);
        }
    }

    pub(crate) fn add_plan(&mut self, plan: LogicalPlan) -> Result<()> {
        self.traversal.clear();
        plan.visit(self)?;
        Ok(())
    }

    pub fn into_graph(self) -> LogicalGraph {
        self.graph
    }

    pub fn build_extension(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        extension: &dyn StreamExtension,
    ) -> Result<()> {
        if let Some(node_name) = extension.node_name() {
            if self.named_nodes.contains_key(&node_name) {
                return plan_err!(
                    "extension {:?} has already been planned, shouldn't try again.",
                    node_name
                );
            }
        }

        let input_schemas = input_nodes
            .iter()
            .map(|index| {
                Ok(self
                    .output_schemas
                    .get(index)
                    .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?
                    .clone())
            })
            .collect::<Result<Vec<_>>>()?;

        let NodeWithIncomingEdges { node, edges } = extension
            .plan_node(&self.planner, self.graph.node_count(), input_schemas)
            .map_err(|e| e.context(format!("planning operator {extension:?}")))?;

        let node_index = self.graph.add_node(node);
        self.add_index_to_traversal(node_index);

        for (source, edge) in input_nodes.into_iter().zip(edges.into_iter()) {
            self.graph.add_edge(source, node_index, edge);
        }

        self.output_schemas
            .insert(node_index, extension.output_schema().into());

        if let Some(node_name) = extension.node_name() {
            self.named_nodes.insert(node_name, node_index);
        }
        Ok(())
    }
}

impl TreeNodeVisitor<'_> for PlanToGraphVisitor<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let stream_extension: &dyn StreamExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        if stream_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }

        if let Some(name) = stream_extension.node_name() {
            if let Some(node_index) = self.named_nodes.get(&name) {
                self.add_index_to_traversal(*node_index);
                return Ok(TreeNodeRecursion::Jump);
            }
        }

        if !node.inputs().is_empty() {
            self.traversal.push(vec![]);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let stream_extension: &dyn StreamExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("planning extension"))?;

        if stream_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }

        if let Some(name) = stream_extension.node_name() {
            if self.named_nodes.contains_key(&name) {
                return Ok(TreeNodeRecursion::Continue);
            }
        }

        let input_nodes = if !node.inputs().is_empty() {
            self.traversal.pop().unwrap_or_default()
        } else {
            vec![]
        };
        let stream_extension: &dyn StreamExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        self.build_extension(input_nodes, stream_extension)?;

        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) struct SplitPlanOutput {
    pub(crate) partial_aggregation_plan: PhysicalPlanNode,
    pub(crate) partial_schema: FsSchema,
    pub(crate) finish_plan: PhysicalPlanNode,
}
