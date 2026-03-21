use itertools::Itertools;

use datafusion::arrow::datatypes::DataType;
use petgraph::Direction;
use petgraph::dot::Dot;
use petgraph::graph::DiGraph;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use datafusion_proto::protobuf::ArrowType;
use prost::Message;
use strum::{Display, EnumString};
use protocol::grpc::api;
use crate::types::FsSchema;

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumString, Display)]
pub enum OperatorName {
    ExpressionWatermark,
    ArrowValue,
    ArrowKey,
    Projection,
    AsyncUdf,
    Join,
    InstantJoin,
    LookupJoin,
    WindowFunction,
    TumblingWindowAggregate,
    SlidingWindowAggregate,
    SessionWindowAggregate,
    UpdatingAggregate,
    ConnectorSource,
    ConnectorSink,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum LogicalEdgeType {
    Forward,
    Shuffle,
    LeftJoin,
    RightJoin,
}

impl Display for LogicalEdgeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalEdgeType::Forward => write!(f, "→"),
            LogicalEdgeType::Shuffle => write!(f, "⤨"),
            LogicalEdgeType::LeftJoin => write!(f, "-[left]⤨"),
            LogicalEdgeType::RightJoin => write!(f, "-[right]⤨"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEdge {
    pub edge_type: LogicalEdgeType,
    pub schema: Arc<FsSchema>,
}

impl LogicalEdge {
    pub fn new(edge_type: LogicalEdgeType, schema: FsSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }

    pub fn project_all(edge_type: LogicalEdgeType, schema: FsSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChainedLogicalOperator {
    pub operator_id: String,
    pub operator_name: OperatorName,
    pub operator_config: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct OperatorChain {
    pub(crate) operators: Vec<ChainedLogicalOperator>,
    pub(crate) edges: Vec<Arc<FsSchema>>,
}

impl OperatorChain {
    pub fn new(operator: ChainedLogicalOperator) -> Self {
        Self {
            operators: vec![operator],
            edges: vec![],
        }
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&mut ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter_mut()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn first(&self) -> &ChainedLogicalOperator {
        &self.operators[0]
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    pub fn is_source(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSource
    }

    pub fn is_sink(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSink
    }
}

#[derive(Clone)]
pub struct LogicalNode {
    pub node_id: u32,
    pub description: String,
    pub operator_chain: OperatorChain,
    pub parallelism: usize,
}

impl LogicalNode {
    pub fn single(
        id: u32,
        operator_id: String,
        name: OperatorName,
        config: Vec<u8>,
        description: String,
        parallelism: usize,
    ) -> Self {
        Self {
            node_id: id,
            description,
            operator_chain: OperatorChain {
                operators: vec![ChainedLogicalOperator {
                    operator_id,
                    operator_name: name,
                    operator_config: config,
                }],
                edges: vec![],
            },
            parallelism,
        }
    }
}

impl Display for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl Debug for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.operator_chain
                .operators
                .iter()
                .map(|op| op.operator_id.clone())
                .collect::<Vec<_>>()
                .join(" -> "),
            self.parallelism
        )
    }
}

pub type LogicalGraph = DiGraph<LogicalNode, LogicalEdge>;

pub trait Optimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool;

    fn optimize(&self, plan: &mut LogicalGraph) {
        loop {
            if !self.optimize_once(plan) {
                break;
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct DylibUdfConfig {
    pub dylib_path: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub aggregate: bool,
    pub is_async: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PythonUdfConfig {
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub name: Arc<String>,
    pub definition: Arc<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ProgramConfig {
    pub udf_dylibs: HashMap<String, DylibUdfConfig>,
    pub python_udfs: HashMap<String, PythonUdfConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct LogicalProgram {
    pub graph: LogicalGraph,
    pub program_config: ProgramConfig,
}

impl LogicalProgram {
    pub fn new(graph: LogicalGraph, program_config: ProgramConfig) -> Self {
        Self {
            graph,
            program_config,
        }
    }

    pub fn optimize(&mut self, optimizer: &dyn Optimizer) {
        optimizer.optimize(&mut self.graph);
    }

    pub fn update_parallelism(&mut self, overrides: &HashMap<u32, usize>) {
        for node in self.graph.node_weights_mut() {
            if let Some(p) = overrides.get(&node.node_id) {
                node.parallelism = *p;
            }
        }
    }

    pub fn dot(&self) -> String {
        format!("{:?}", Dot::with_config(&self.graph, &[]))
    }

    pub fn task_count(&self) -> usize {
        self.graph.node_weights().map(|nw| nw.parallelism).sum()
    }

    pub fn sources(&self) -> HashSet<u32> {
        self.graph
            .externals(Direction::Incoming)
            .map(|t| self.graph.node_weight(t).unwrap().node_id)
            .collect()
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                tasks_per_operator.insert(op.operator_id.clone(), node.parallelism);
            }
        }
        tasks_per_operator
    }

    pub fn operator_names_by_id(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                m.insert(op.operator_id.clone(), op.operator_name.to_string());
            }
        }
        m
    }

    pub fn tasks_per_node(&self) -> HashMap<u32, usize> {
        let mut tasks_per_node = HashMap::new();
        for node in self.graph.node_weights() {
            tasks_per_node.insert(node.node_id, node.parallelism);
        }
        tasks_per_node
    }

    pub fn features(&self) -> HashSet<String> {
        let mut s = HashSet::new();
        for n in self.graph.node_weights() {
            for t in &n.operator_chain.operators {
                let feature = match &t.operator_name {
                    OperatorName::AsyncUdf => "async-udf".to_string(),
                    OperatorName::ExpressionWatermark
                    | OperatorName::ArrowValue
                    | OperatorName::ArrowKey
                    | OperatorName::Projection => continue,
                    OperatorName::Join => "join-with-expiration".to_string(),
                    OperatorName::InstantJoin => "windowed-join".to_string(),
                    OperatorName::WindowFunction => "sql-window-function".to_string(),
                    OperatorName::LookupJoin => "lookup-join".to_string(),
                    OperatorName::TumblingWindowAggregate => {
                        "sql-tumbling-window-aggregate".to_string()
                    }
                    OperatorName::SlidingWindowAggregate => {
                        "sql-sliding-window-aggregate".to_string()
                    }
                    OperatorName::SessionWindowAggregate => {
                        "sql-session-window-aggregate".to_string()
                    }
                    OperatorName::UpdatingAggregate => "sql-updating-aggregate".to_string(),
                    OperatorName::ConnectorSource => "connector-source".to_string(),
                    OperatorName::ConnectorSink => "connector-sink".to_string(),
                };
                s.insert(feature);
            }
        }
        s
    }
}


impl From<DylibUdfConfig> for api::DylibUdfConfig {
    fn from(from: DylibUdfConfig) -> Self {
        api::DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    ArrowType::try_from(t)
                        .expect("unsupported data type")
                        .encode_to_vec()
                })
                .collect(),
            return_type: ArrowType::try_from(&from.return_type)
                .expect("unsupported data type")
                .encode_to_vec(),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}

impl From<api::DylibUdfConfig> for DylibUdfConfig {
    fn from(from: api::DylibUdfConfig) -> Self {
        DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    DataType::try_from(
                        &ArrowType::decode(&mut t.as_slice()).expect("invalid arrow type"),
                    )
                        .expect("invalid arrow type")
                })
                .collect(),
            return_type: DataType::try_from(
                &ArrowType::decode(&mut from.return_type.as_slice()).unwrap(),
            )
                .expect("invalid arrow type"),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}