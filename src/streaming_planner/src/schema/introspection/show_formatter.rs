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

use crate::common::constants::connection_table_role;
use crate::schema::catalog::ExternalTable;
use crate::schema::table::CatalogEntity;

use super::ddl_formatter::DdlBuilder;

impl ExternalTable {
    pub fn to_ddl_string(&self) -> String {
        match self {
            ExternalTable::Source(source) => {
                let schema = source.produce_physical_schema();
                DdlBuilder::new(&source.table_identifier, &schema)
                    .with_watermark(source.temporal_config.watermark_strategy_column.as_deref())
                    .with_primary_keys(&source.key_constraints)
                    .with_options(
                        &source.catalog_with_options,
                        connection_table_role::SOURCE,
                        &source.adapter_type,
                    )
                    .to_string()
            }
            ExternalTable::Sink(sink) => {
                let schema = sink.produce_physical_schema();
                DdlBuilder::new(&sink.table_identifier, &schema)
                    .with_primary_keys(&sink.key_constraints)
                    .with_options(
                        &sink.catalog_with_options,
                        connection_table_role::SINK,
                        &sink.adapter_type,
                    )
                    .to_string()
            }
            ExternalTable::Lookup(lookup) => {
                let schema = lookup.produce_physical_schema();
                DdlBuilder::new(&lookup.table_identifier, &schema)
                    .with_primary_keys(&lookup.key_constraints)
                    .with_options(
                        &lookup.catalog_with_options,
                        connection_table_role::LOOKUP,
                        &lookup.adapter_type,
                    )
                    .to_string()
            }
        }
    }

    pub fn to_row_detail(&self) -> String {
        match self {
            ExternalTable::Source(s) => format!(
                "{{ kind: 'source', connector: '{}', watermark: '{}', options_count: {} }}",
                s.adapter_type,
                s.temporal_config
                    .watermark_strategy_column
                    .as_deref()
                    .unwrap_or("none"),
                s.catalog_with_options.len()
            ),
            ExternalTable::Sink(s) => format!(
                "{{ kind: 'sink', connector: '{}', partitioned: {}, options_count: {} }}",
                s.adapter_type,
                s.partition_exprs.as_ref().is_some(),
                s.catalog_with_options.len()
            ),
            ExternalTable::Lookup(s) => format!(
                "{{ kind: 'lookup', connector: '{}', cache_ttl_secs: {}, options_count: {} }}",
                s.adapter_type,
                s.lookup_cache_ttl.map(|d| d.as_secs()).unwrap_or(0),
                s.catalog_with_options.len()
            ),
        }
    }
}

pub fn show_create_catalog_table(table: &CatalogEntity) -> String {
    match table {
        CatalogEntity::ExternalConnector(ext) => ext.to_ddl_string(),
        CatalogEntity::ComputedTable { name, .. } => {
            format!("-- Logical query view\nCREATE VIEW {name} AS SELECT ...;")
        }
    }
}

pub fn catalog_table_row_detail(table: &CatalogEntity) -> String {
    match table {
        CatalogEntity::ExternalConnector(ext) => ext.to_row_detail(),
        CatalogEntity::ComputedTable { .. } => "{ kind: 'logical_view' }".to_string(),
    }
}
