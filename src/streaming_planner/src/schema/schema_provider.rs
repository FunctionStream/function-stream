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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{self as datatypes, DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::{DefaultTableSource, TableProvider, TableType};
use datafusion::execution::{FunctionRegistry, SessionStateDefaults};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion::optimizer::Analyzer;
use datafusion::sql::TableReference;
use datafusion::sql::planner::ContextProvider;
use thiserror::Error;
use tracing::{debug, error, info};
use unicase::UniCase;

use crate::common::constants::{planning_placeholder_udf, window_fn};
use crate::logical_node::logical::{DylibUdfConfig, LogicalProgram};
use crate::schema::table::CatalogEntity;
use crate::schema::utils::window_arrow_struct;
use crate::types::{PlanningOptions, PlanningPlaceholderUdf, SqlConfig};

pub type ObjectName = UniCase<String>;

#[inline]
fn object_name(s: impl Into<String>) -> ObjectName {
    UniCase::new(s.into())
}

#[derive(Error, Debug)]
pub enum PlanningError {
    #[error("Catalog table not found: {0}")]
    TableNotFound(String),
    #[error("Planning init failed: {0}")]
    InitError(String),
    #[error("Engine error: {0}")]
    Engine(#[from] DataFusionError),
}

impl From<PlanningError> for DataFusionError {
    fn from(err: PlanningError) -> Self {
        match err {
            PlanningError::Engine(inner) => inner,
            other => DataFusionError::Plan(other.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StreamTable {
    Source {
        name: String,
        connector: String,
        schema: Arc<Schema>,
        event_time_field: Option<String>,
        watermark_field: Option<String>,
        with_options: BTreeMap<String, String>,
    },
    Sink {
        name: String,
        program: LogicalProgram,
    },
}

impl StreamTable {
    pub fn name(&self) -> &str {
        match self {
            Self::Source { name, .. } | Self::Sink { name, .. } => name,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        match self {
            Self::Source { schema, .. } => Arc::clone(schema),
            Self::Sink { program, .. } => program
                .egress_arrow_schema()
                .unwrap_or_else(|| Arc::new(Schema::empty())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogicalBatchInput {
    pub table_name: String,
    pub schema: Arc<Schema>,
}

#[async_trait::async_trait]
impl TableProvider for LogicalBatchInput {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(Arc::new(crate::physical::FsMemExec::new(
            self.table_name.clone(),
            Arc::clone(&self.schema),
        )))
    }
}

#[derive(Clone, Default)]
pub struct FunctionCatalog {
    pub scalars: HashMap<String, Arc<ScalarUDF>>,
    pub aggregates: HashMap<String, Arc<AggregateUDF>>,
    pub windows: HashMap<String, Arc<WindowUDF>>,
    pub planners: Vec<Arc<dyn ExprPlanner>>,
}

#[derive(Clone, Default)]
pub struct TableCatalog {
    pub streams: HashMap<ObjectName, Arc<StreamTable>>,
    pub catalogs: HashMap<ObjectName, Arc<CatalogEntity>>,
    pub source_defs: HashMap<String, String>,
}

#[derive(Clone, Default)]
pub struct StreamPlanningContext {
    pub tables: TableCatalog,
    pub functions: FunctionCatalog,
    pub dylib_udfs: HashMap<String, DylibUdfConfig>,
    pub config_options: datafusion::config::ConfigOptions,
    pub planning_options: PlanningOptions,
    pub analyzer: Analyzer,
    pub sql_config: SqlConfig,
}

pub type StreamSchemaProvider = StreamPlanningContext;

impl StreamPlanningContext {
    pub fn builder() -> StreamPlanningContextBuilder {
        StreamPlanningContextBuilder::default()
    }

    #[inline]
    pub fn default_parallelism(&self) -> usize {
        self.sql_config.default_parallelism
    }

    #[inline]
    pub fn key_by_parallelism(&self) -> usize {
        self.sql_config.key_by_parallelism
    }

    pub fn try_new(config: SqlConfig) -> Result<Self, PlanningError> {
        info!("Initializing StreamPlanningContext");
        let mut builder = StreamPlanningContextBuilder::default();
        builder
            .with_streaming_extensions()?
            .with_default_functions()?;
        let mut ctx = builder.build();
        ctx.sql_config = config;
        Ok(ctx)
    }

    pub fn new() -> Self {
        let config = crate::planning_runtime::sql_planning_snapshot();
        Self::try_new(config).expect("StreamPlanningContext bootstrap")
    }

    pub fn register_stream_table(&mut self, table: StreamTable) {
        let key = object_name(table.name().to_string());
        debug!(table = %key, "register stream table");
        self.tables.streams.insert(key, Arc::new(table));
    }

    pub fn get_stream_table(&self, name: &str) -> Option<Arc<StreamTable>> {
        self.tables
            .streams
            .get(&object_name(name.to_string()))
            .cloned()
    }

    pub fn register_catalog_table(&mut self, table: CatalogEntity) {
        let key = object_name(table.name().to_string());
        debug!(table = %key, "register catalog table");
        self.tables.catalogs.insert(key, Arc::new(table));
    }

    pub fn get_catalog_table(&self, table_name: impl AsRef<str>) -> Option<&CatalogEntity> {
        self.tables
            .catalogs
            .get(&object_name(table_name.as_ref().to_string()))
            .map(|t| t.as_ref())
    }

    pub fn get_catalog_table_mut(
        &mut self,
        table_name: impl AsRef<str>,
    ) -> Option<&mut CatalogEntity> {
        self.tables
            .catalogs
            .get_mut(&object_name(table_name.as_ref().to_string()))
            .map(Arc::make_mut)
    }

    pub fn add_source_table(
        &mut self,
        name: String,
        schema: Arc<Schema>,
        event_time_field: Option<String>,
        watermark_field: Option<String>,
    ) {
        self.register_stream_table(StreamTable::Source {
            name,
            connector: "stream_catalog".to_string(),
            schema,
            event_time_field,
            watermark_field,
            with_options: BTreeMap::new(),
        });
    }

    pub fn add_sink_table(&mut self, name: String, program: LogicalProgram) {
        self.register_stream_table(StreamTable::Sink { name, program });
    }

    pub fn insert_table(&mut self, table: StreamTable) {
        self.register_stream_table(table);
    }

    pub fn insert_catalog_table(&mut self, table: CatalogEntity) {
        self.register_catalog_table(table);
    }

    pub fn get_table(&self, table_name: impl AsRef<str>) -> Option<&StreamTable> {
        self.tables
            .streams
            .get(&object_name(table_name.as_ref().to_string()))
            .map(|a| a.as_ref())
    }

    pub fn get_table_mut(&mut self, table_name: impl AsRef<str>) -> Option<&mut StreamTable> {
        self.tables
            .streams
            .get_mut(&object_name(table_name.as_ref().to_string()))
            .map(Arc::make_mut)
    }

    pub fn get_async_udf_options(&self, _name: &str) -> Option<crate::analysis::AsyncOptions> {
        None
    }

    fn create_table_source(name: String, schema: Arc<Schema>) -> Arc<dyn TableSource> {
        let provider = LogicalBatchInput {
            table_name: name,
            schema,
        };
        Arc::new(DefaultTableSource::new(Arc::new(provider)))
    }
}

impl ContextProvider for StreamPlanningContext {
    fn get_table_source(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        let name_str = name.table();
        match self.get_stream_table(name_str) {
            Some(table) => Ok(Self::create_table_source(name.to_string(), table.schema())),
            None => {
                error!(table = %name_str, "stream table lookup failed");
                Err(DataFusionError::Plan(format!("Table {} not found", name)))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.scalars.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.functions.aggregates.get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.functions.windows.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.config_options
    }

    fn udf_names(&self) -> Vec<String> {
        self.functions.scalars.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.functions.aggregates.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.functions.windows.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.functions.planners
    }
}

impl FunctionRegistry for StreamPlanningContext {
    fn udfs(&self) -> HashSet<String> {
        self.functions.scalars.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> DataFusionResult<Arc<ScalarUDF>> {
        self.functions
            .scalars
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("No UDF with name {name}")))
    }

    fn udaf(&self, name: &str) -> DataFusionResult<Arc<AggregateUDF>> {
        self.functions
            .aggregates
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("No UDAF with name {name}")))
    }

    fn udwf(&self, name: &str) -> DataFusionResult<Arc<WindowUDF>> {
        self.functions
            .windows
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("No UDWF with name {name}")))
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> DataFusionResult<()> {
        self.analyzer.add_function_rewrite(rewrite);
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> DataFusionResult<Option<Arc<ScalarUDF>>> {
        Ok(self.functions.scalars.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> DataFusionResult<Option<Arc<AggregateUDF>>> {
        Ok(self
            .functions
            .aggregates
            .insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> DataFusionResult<Option<Arc<WindowUDF>>> {
        Ok(self.functions.windows.insert(udwf.name().to_string(), udwf))
    }

    fn register_expr_planner(
        &mut self,
        expr_planner: Arc<dyn ExprPlanner>,
    ) -> DataFusionResult<()> {
        self.functions.planners.push(expr_planner);
        Ok(())
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.functions.planners.clone()
    }
}

#[derive(Default)]
pub struct StreamPlanningContextBuilder {
    context: StreamPlanningContext,
}

impl StreamPlanningContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default_functions(&mut self) -> Result<&mut Self, PlanningError> {
        for p in SessionStateDefaults::default_scalar_functions() {
            self.context.register_udf(p)?;
        }
        for p in SessionStateDefaults::default_aggregate_functions() {
            self.context.register_udaf(p)?;
        }
        for p in SessionStateDefaults::default_window_functions() {
            self.context.register_udwf(p)?;
        }
        for p in SessionStateDefaults::default_expr_planners() {
            self.context.register_expr_planner(p)?;
        }
        Ok(self)
    }

    pub fn with_streaming_extensions(&mut self) -> Result<&mut Self, PlanningError> {
        let extensions = vec![
            PlanningPlaceholderUdf::new_with_return(
                window_fn::HOP,
                vec![
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                ],
                window_arrow_struct(),
            ),
            PlanningPlaceholderUdf::new_with_return(
                window_fn::TUMBLE,
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_arrow_struct(),
            ),
            PlanningPlaceholderUdf::new_with_return(
                window_fn::SESSION,
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_arrow_struct(),
            ),
            PlanningPlaceholderUdf::new_with_return(
                planning_placeholder_udf::UNNEST,
                vec![DataType::List(Arc::new(Field::new(
                    planning_placeholder_udf::LIST_ELEMENT_FIELD,
                    DataType::Utf8,
                    true,
                )))],
                DataType::Utf8,
            ),
            PlanningPlaceholderUdf::new_with_return(
                planning_placeholder_udf::ROW_TIME,
                vec![],
                DataType::Timestamp(datatypes::TimeUnit::Nanosecond, None),
            ),
        ];

        for ext in extensions {
            self.context.register_udf(ext)?;
        }

        Ok(self)
    }

    pub fn build(self) -> StreamPlanningContext {
        self.context
    }
}
