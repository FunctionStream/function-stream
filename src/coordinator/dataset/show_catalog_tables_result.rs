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

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::datatypes::Schema as DfSchema;

use super::DataSet;
use crate::sql::schema::table::Table as CatalogTable;
use crate::sql::schema::{catalog_table_row_detail, schema_columns_one_line};

#[derive(Clone, Debug)]
pub struct ShowCatalogTablesResult {
    names: Vec<String>,
    kinds: Vec<String>,
    column_counts: Vec<i32>,
    schema_lines: Vec<String>,
    details: Vec<String>,
}

impl ShowCatalogTablesResult {
    pub fn from_tables(tables: &[Arc<CatalogTable>]) -> Self {
        let mut names = Vec::with_capacity(tables.len());
        let mut kinds = Vec::with_capacity(tables.len());
        let mut column_counts = Vec::with_capacity(tables.len());
        let mut schema_lines = Vec::with_capacity(tables.len());
        let mut details = Vec::with_capacity(tables.len());

        for t in tables {
            let schema = match t.as_ref() {
                CatalogTable::ConnectorTable(source) | CatalogTable::LookupTable(source) => {
                    source.produce_physical_schema()
                }
                CatalogTable::TableFromQuery { .. } => DfSchema::new(t.get_fields()),
            };
            let ncols = schema.fields().len() as i32;
            names.push(t.name().to_string());
            kinds.push(
                match t.as_ref() {
                    CatalogTable::ConnectorTable(_) => "SOURCE",
                    CatalogTable::LookupTable(_) => "LOOKUP",
                    CatalogTable::TableFromQuery { .. } => "QUERY",
                }
                .to_string(),
            );
            column_counts.push(ncols);
            schema_lines.push(schema_columns_one_line(&schema));
            details.push(catalog_table_row_detail(t.as_ref()));
        }

        Self {
            names,
            kinds,
            column_counts,
            schema_lines,
            details,
        }
    }
}

impl DataSet for ShowCatalogTablesResult {
    fn to_record_batch(&self) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("column_count", DataType::Int32, false),
            Field::new("schema_columns", DataType::Utf8, false),
            Field::new("details", DataType::Utf8, false),
        ]));

        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    self.names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    self.kinds.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(self.column_counts.clone())),
                Arc::new(StringArray::from(
                    self.schema_lines
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    self.details.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap_or_else(|_| arrow_array::RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}
