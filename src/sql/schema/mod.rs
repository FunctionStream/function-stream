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

pub mod catalog_ddl;
pub mod column_descriptor;
pub mod connection_type;
pub mod connector_config;
pub mod kafka_operator_config;
pub mod source_table;
pub mod data_encoding_format;
pub mod schema_context;
pub mod schema_provider;
pub mod table;
pub mod table_execution_unit;
pub mod table_role;
pub mod temporal_pipeline_config;
pub mod utils;

pub use catalog_ddl::{
    catalog_table_row_detail, schema_columns_one_line, show_create_catalog_table,
    show_create_stream_table, stream_table_row_detail,
};
pub use column_descriptor::ColumnDescriptor;
pub use connection_type::ConnectionType;
pub use connector_config::ConnectorConfig;
pub use source_table::{SourceOperator, SourceTable};

/// Back-compat alias for [`SourceTable`].
pub type ConnectorTable = SourceTable;
pub use data_encoding_format::DataEncodingFormat;
pub use schema_context::{DfSchemaContext, SchemaContext};
pub use schema_provider::{
    FunctionCatalog, LogicalBatchInput, ObjectName, StreamPlanningContext,
    StreamPlanningContextBuilder, StreamSchemaProvider, StreamTable, TableCatalog,
};
pub use table::Table;
pub use table_execution_unit::{EngineDescriptor, SyncMode, TableExecutionUnit};
pub use table_role::{
    apply_adapter_specific_rules, deduce_role, serialize_backend_params, validate_adapter_availability,
    TableRole,
};
pub use temporal_pipeline_config::{resolve_temporal_logic, TemporalPipelineConfig, TemporalSpec};
