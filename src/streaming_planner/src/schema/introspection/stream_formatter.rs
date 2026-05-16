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

use datafusion::arrow::datatypes::Schema;

use crate::common::constants::connection_table_role;
use crate::logical_node::logical::LogicalProgram;
use crate::schema::schema_provider::StreamTable;

use super::ddl_formatter::DdlBuilder;

impl StreamTable {
    pub fn to_ddl_string(&self) -> String {
        match self {
            StreamTable::Source {
                name,
                connector,
                schema,
                event_time_field: _,
                watermark_field,
                with_options,
            } => DdlBuilder::new(name, schema)
                .with_watermark(watermark_field.as_deref())
                .with_options(with_options, connection_table_role::SOURCE, connector)
                .to_string(),
            StreamTable::Sink { name, program } => {
                let schema: Arc<Schema> = program
                    .egress_arrow_schema()
                    .unwrap_or_else(|| Arc::new(Schema::empty()));

                let mut ddl = format!("CREATE STREAMING TABLE {name} AS SELECT ...\n\n");
                ddl.push_str("/* === SINK SCHEMA === */\n");
                let schema_ddl = DdlBuilder::new(name, &schema).to_string();
                ddl.push_str(&schema_ddl);
                ddl.push_str("\n\n/* === STREAMING TOPOLOGY === */\n");
                ddl.push_str(&format_pipeline(program));
                ddl
            }
        }
    }

    pub fn to_row_detail(&self) -> String {
        match self {
            StreamTable::Source {
                connector,
                event_time_field,
                watermark_field,
                with_options,
                ..
            } => format!(
                "{{ kind: 'stream_source', connector: '{}', event_time: '{}', watermark: '{}', options_count: {} }}",
                connector,
                event_time_field.as_deref().unwrap_or("none"),
                watermark_field.as_deref().unwrap_or("none"),
                with_options.len()
            ),
            StreamTable::Sink { program, .. } => format!(
                "{{ kind: 'streaming_sink', tasks: {}, nodes: {} }}",
                program.task_count(),
                program.graph.node_count()
            ),
        }
    }
}

pub fn show_create_stream_table(table: &StreamTable) -> String {
    table.to_ddl_string()
}

pub fn stream_table_row_detail(table: &StreamTable) -> String {
    table.to_row_detail()
}

fn format_pipeline(program: &LogicalProgram) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("Pipeline Hash : {}", program.get_hash()));
    lines.push(format!("Total Tasks   : {}", program.task_count()));
    lines.push(format!("Node Count    : {}", program.graph.node_count()));
    lines.push(String::from("Operator Chains:"));

    for nw in program.graph.node_weights() {
        let chain = nw
            .operator_chain
            .operators
            .iter()
            .map(|op| format!("{}[{}]", op.operator_name, op.operator_id))
            .collect::<Vec<_>>()
            .join(" -> ");

        lines.push(format!(
            "  Node {:<3} | Parallelism {:<3} | {}",
            nw.node_id, nw.parallelism, chain
        ));
    }

    let dot = program.dot();
    const MAX_DOT: usize = 5_000;
    if dot.len() > MAX_DOT {
        lines.push(format!(
            "\nGraphviz DOT (truncated, {} bytes omitted):\n{}...",
            dot.len() - MAX_DOT,
            &dot[..MAX_DOT]
        ));
    } else {
        lines.push(format!("\nGraphviz DOT:\n{dot}"));
    }

    lines.join("\n")
}
