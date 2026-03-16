use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{self as datatypes, DataType, Field, Schema};
use datafusion::common::{Result, plan_err};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SessionStateDefaults};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{
    AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion::optimizer::Analyzer;
use datafusion::sql::TableReference;
use datafusion::sql::planner::ContextProvider;
use unicase::UniCase;

use crate::sql::catalog::table::Table as CatalogTable;
use crate::sql::planner::schemas::window_arrow_struct;
use crate::sql::types::{PlaceholderUdf, PlanningOptions};

#[derive(Clone, Default)]
pub struct StreamSchemaProvider {
    pub source_defs: HashMap<String, String>,
    tables: HashMap<UniCase<String>, StreamTable>,
    catalog_tables: HashMap<UniCase<String>, CatalogTable>,
    pub functions: HashMap<String, Arc<ScalarUDF>>,
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    pub window_functions: HashMap<String, Arc<WindowUDF>>,
    config_options: datafusion::config::ConfigOptions,
    pub expr_planners: Vec<Arc<dyn ExprPlanner>>,
    pub planning_options: PlanningOptions,
    pub analyzer: Analyzer,
}

#[derive(Clone, Debug)]
pub enum StreamTable {
    Source {
        name: String,
        schema: Arc<Schema>,
        event_time_field: Option<String>,
        watermark_field: Option<String>,
    },
    Sink {
        name: String,
        schema: Arc<Schema>,
    },
    Memory {
        name: String,
        logical_plan: Option<LogicalPlan>,
    },
}

impl StreamTable {
    pub fn name(&self) -> &str {
        match self {
            StreamTable::Source { name, .. } => name,
            StreamTable::Sink { name, .. } => name,
            StreamTable::Memory { name, .. } => name,
        }
    }

    pub fn get_fields(&self) -> Vec<Arc<Field>> {
        match self {
            StreamTable::Source { schema, .. } => schema.fields().to_vec(),
            StreamTable::Sink { schema, .. } => schema.fields().to_vec(),
            StreamTable::Memory { .. } => vec![],
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogicalBatchInput {
    pub table_name: String,
    pub schema: Arc<Schema>,
}

#[async_trait::async_trait]
impl datafusion::datasource::TableProvider for LogicalBatchInput {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(Arc::new(crate::sql::physical::FsMemExec::new(
            self.table_name.clone(),
            self.schema.clone(),
        )))
    }
}

fn create_table(table_name: String, schema: Arc<Schema>) -> Arc<dyn TableSource> {
    let table_provider = LogicalBatchInput { table_name, schema };
    let wrapped = Arc::new(table_provider);
    let provider = DefaultTableSource::new(wrapped);
    Arc::new(provider)
}

impl StreamSchemaProvider {
    pub fn new() -> Self {
        let mut registry = Self {
            ..Default::default()
        };

        registry
            .register_udf(PlaceholderUdf::with_return(
                "hop",
                vec![
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                ],
                window_arrow_struct(),
            ))
            .unwrap();

        registry
            .register_udf(PlaceholderUdf::with_return(
                "tumble",
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_arrow_struct(),
            ))
            .unwrap();

        registry
            .register_udf(PlaceholderUdf::with_return(
                "session",
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_arrow_struct(),
            ))
            .unwrap();

        registry
            .register_udf(PlaceholderUdf::with_return(
                "unnest",
                vec![DataType::List(Arc::new(Field::new(
                    "field",
                    DataType::Utf8,
                    true,
                )))],
                DataType::Utf8,
            ))
            .unwrap();

        registry
            .register_udf(PlaceholderUdf::with_return(
                "row_time",
                vec![],
                DataType::Timestamp(datatypes::TimeUnit::Nanosecond, None),
            ))
            .unwrap();

        for p in SessionStateDefaults::default_scalar_functions() {
            registry.register_udf(p).unwrap();
        }
        for p in SessionStateDefaults::default_aggregate_functions() {
            registry.register_udaf(p).unwrap();
        }
        for p in SessionStateDefaults::default_window_functions() {
            registry.register_udwf(p).unwrap();
        }
        for p in SessionStateDefaults::default_expr_planners() {
            registry.register_expr_planner(p).unwrap();
        }

        registry
    }

    pub fn add_source_table(
        &mut self,
        name: String,
        schema: Arc<Schema>,
        event_time_field: Option<String>,
        watermark_field: Option<String>,
    ) {
        self.tables.insert(
            UniCase::new(name.clone()),
            StreamTable::Source {
                name,
                schema,
                event_time_field,
                watermark_field,
            },
        );
    }

    pub fn add_sink_table(&mut self, name: String, schema: Arc<Schema>) {
        self.tables.insert(
            UniCase::new(name.clone()),
            StreamTable::Sink { name, schema },
        );
    }

    pub fn insert_table(&mut self, table: StreamTable) {
        self.tables
            .insert(UniCase::new(table.name().to_string()), table);
    }

    pub fn get_table(&self, table_name: impl Into<String>) -> Option<&StreamTable> {
        self.tables.get(&UniCase::new(table_name.into()))
    }

    pub fn get_table_mut(&mut self, table_name: impl Into<String>) -> Option<&mut StreamTable> {
        self.tables.get_mut(&UniCase::new(table_name.into()))
    }

    pub fn insert_catalog_table(&mut self, table: CatalogTable) {
        self.catalog_tables
            .insert(UniCase::new(table.name().to_string()), table);
    }

    pub fn get_catalog_table(&self, table_name: impl Into<String>) -> Option<&CatalogTable> {
        self.catalog_tables.get(&UniCase::new(table_name.into()))
    }

    pub fn get_catalog_table_mut(
        &mut self,
        table_name: impl Into<String>,
    ) -> Option<&mut CatalogTable> {
        self.catalog_tables
            .get_mut(&UniCase::new(table_name.into()))
    }

    pub fn get_async_udf_options(
        &self,
        _name: &str,
    ) -> Option<crate::sql::planner::rewrite::AsyncOptions> {
        // TODO: implement async UDF lookup
        None
    }
}

impl ContextProvider for StreamSchemaProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table = self
            .get_table(name.to_string())
            .ok_or_else(|| DataFusionError::Plan(format!("Table {name} not found")))?;

        let fields = table.get_fields();
        let schema = Arc::new(Schema::new_with_metadata(
            fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<Field>>(),
            HashMap::new(),
        ));
        Ok(create_table(name.to_string(), schema))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.config_options
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.window_functions.get(name).cloned()
    }

    fn udf_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.window_functions.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

impl FunctionRegistry for StreamSchemaProvider {
    fn udfs(&self) -> HashSet<String> {
        self.functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        if let Some(f) = self.functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDF with name {name}")
        }
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        if let Some(f) = self.aggregate_functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDAF with name {name}")
        }
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        if let Some(f) = self.window_functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDWF with name {name}")
        }
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> Result<()> {
        self.analyzer.add_function_rewrite(rewrite);
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        Ok(self.functions.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> Result<Option<Arc<AggregateUDF>>> {
        Ok(self
            .aggregate_functions
            .insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        Ok(self.window_functions.insert(udwf.name().to_string(), udwf))
    }

    fn register_expr_planner(&mut self, expr_planner: Arc<dyn ExprPlanner>) -> Result<()> {
        self.expr_planners.push(expr_planner);
        Ok(())
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.expr_planners.clone()
    }
}
