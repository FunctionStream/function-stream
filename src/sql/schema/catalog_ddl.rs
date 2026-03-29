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

//! Best-effort SQL text for catalog introspection (`SHOW CREATE TABLE`).

use std::collections::BTreeMap;

use datafusion::arrow::datatypes::{DataType, TimeUnit};

use super::schema_provider::StreamTable;
use crate::sql::logical_node::logical::LogicalProgram;

fn data_type_sql(dt: &DataType) -> String {
    match dt {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 => "FLOAT".to_string(),
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".to_string(),
        DataType::Binary | DataType::LargeBinary => "VARBINARY".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(unit, tz) => match (unit, tz) {
            (TimeUnit::Second, None) => "TIMESTAMP(0)".to_string(),
            (TimeUnit::Millisecond, None) => "TIMESTAMP(3)".to_string(),
            (TimeUnit::Microsecond, None) => "TIMESTAMP(6)".to_string(),
            (TimeUnit::Nanosecond, None) => "TIMESTAMP(9)".to_string(),
            (_, Some(_)) => "TIMESTAMP WITH TIME ZONE".to_string(),
        },
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Decimal256(p, s) => format!("DECIMAL({p},{s})"),
        _ => dt.to_string(),
    }
}

fn format_columns(schema: &datafusion::arrow::datatypes::Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let null = if f.is_nullable() {
                ""
            } else {
                " NOT NULL"
            };
            format!("  {} {}{}", f.name(), data_type_sql(f.data_type()), null)
        })
        .collect()
}

fn format_with_clause(opts: &BTreeMap<String, String>) -> String {
    if opts.is_empty() {
        return "WITH ('connector' = '...', 'format' = '...');\n/* Original WITH options are not persisted in the stream catalog. */\n"
            .to_string();
    }
    let pairs: Vec<String> = opts
        .iter()
        .map(|(k, v)| {
            let k_esc = k.replace('\'', "''");
            let v_esc = v.replace('\'', "''");
            format!("  '{k_esc}' = '{v_esc}'")
        })
        .collect();
    format!("WITH (\n{}\n);\n", pairs.join(",\n"))
}

/// Single-line `col:TYPE` list for result grids.
pub fn schema_columns_one_line(schema: &datafusion::arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), data_type_sql(f.data_type())))
        .collect::<Vec<_>>()
        .join(", ")
}

fn pipeline_summary_short(program: &LogicalProgram) -> String {
    let mut parts: Vec<String> = Vec::new();
    parts.push(format!("tasks={}", program.task_count()));
    parts.push(format!("hash={}", program.get_hash()));
    for nw in program.graph.node_weights() {
        let chain = nw
            .operator_chain
            .operators
            .iter()
            .map(|o| format!("{}", o.operator_name))
            .collect::<Vec<_>>()
            .join("->");
        parts.push(format!("n{}:{}", nw.node_id, chain));
    }
    parts.join(" | ")
}

/// Extra fields for `SHOW TABLES` result grid (pipeline summary; no full Graphviz).
pub fn stream_table_row_detail(table: &StreamTable) -> String {
    match table {
        StreamTable::Source {
            event_time_field,
            watermark_field,
            with_options,
            ..
        } => {
            format!(
                "event_time={:?}, watermark={:?}, with_options={}",
                event_time_field,
                watermark_field,
                with_options.len()
            )
        }
        StreamTable::Sink { program, .. } => pipeline_summary_short(program),
    }
}

fn pipeline_text(program: &LogicalProgram) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("tasks_total: {}", program.task_count()));
    lines.push(format!("program_hash: {}", program.get_hash()));
    for nw in program.graph.node_weights() {
        let chain = nw
            .operator_chain
            .operators
            .iter()
            .map(|o| format!("{}[{}]", o.operator_name, o.operator_id))
            .collect::<Vec<_>>()
            .join(" -> ");
        lines.push(format!(
            "node {} (parallelism={}): {chain}",
            nw.node_id, nw.parallelism
        ));
    }
    let dot = program.dot();
    const MAX_DOT: usize = 12_000;
    if dot.len() > MAX_DOT {
        lines.push(format!(
            "graphviz_dot_truncated:\n{}... [{} more bytes]",
            &dot[..MAX_DOT],
            dot.len() - MAX_DOT
        ));
    } else {
        lines.push(format!("graphviz_dot:\n{dot}"));
    }
    lines.join("\n")
}

/// Human-readable `SHOW CREATE TABLE` text (sink `AS SELECT` is not stored).
pub fn show_create_stream_table(table: &StreamTable) -> String {
    match table {
        StreamTable::Source {
            name,
            schema,
            event_time_field,
            watermark_field,
            with_options,
        } => {
            let cols = format_columns(schema);
            let mut ddl = format!("CREATE TABLE {name} (\n{}\n)", cols.join(",\n"));
            if let Some(e) = event_time_field {
                ddl.push_str(&format!("\n/* EVENT TIME COLUMN: {e} */\n"));
            }
            if let Some(w) = watermark_field {
                ddl.push_str(&format!("/* WATERMARK: {w} */\n"));
            }
            ddl.push_str(&format_with_clause(with_options));
            ddl
        }
        StreamTable::Sink { name, program } => {
            let schema = program
                .egress_arrow_schema()
                .unwrap_or_else(|| std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            let cols = format_columns(&schema);
            let mut ddl = format!(
                "CREATE STREAMING TABLE {name}\nWITH ('connector' = '...') AS SELECT ...\n/* Sink WITH / AS SELECT text is not stored. Output schema:\n{}\n*/\n\n",
                cols.join(",\n")
            );
            ddl.push_str("-- Resolved logical pipeline:\n");
            ddl.push_str(&pipeline_text(program));
            ddl.push('\n');
            ddl
        }
    }
}
