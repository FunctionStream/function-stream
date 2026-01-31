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

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table, TableComponent};
use protocol::cli::{function_stream_service_client::FunctionStreamServiceClient, SqlRequest};
use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor, EditMode};
use std::io::{self, Cursor, Write};
use tonic::Request;

#[derive(Debug, thiserror::Error)]
pub enum ReplError {
    #[error("RPC error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

pub struct Repl {
    client: Option<FunctionStreamServiceClient<tonic::transport::Channel>>,
    server_host: String,
    server_port: u16,
    editor: Option<DefaultEditor>,
}

impl Repl {
    pub fn new(server_host: String, server_port: u16) -> Self {
        let config = Config::builder()
            .history_ignore_space(true)
            .edit_mode(EditMode::Emacs)
            .build();

        let editor = DefaultEditor::with_config(config).ok().map(|mut ed| {
            let _ = ed.load_history(".function-stream-cli-history");
            ed
        });

        Self {
            client: None,
            server_host,
            server_port,
            editor,
        }
    }

    fn server_address(&self) -> String {
        format!("http://{}:{}", self.server_host, self.server_port)
    }

    pub async fn connect(&mut self) -> Result<(), ReplError> {
        let addr = self.server_address();
        let client = FunctionStreamServiceClient::connect(addr.clone())
            .await
            .map_err(|e| ReplError::Connection(format!("Connect failed to {}: {}", addr, e)))?;
        self.client = Some(client);
        Ok(())
    }

    pub async fn execute_sql(&mut self, sql: &str) -> Result<(), ReplError> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ReplError::Connection("Client not connected".to_string()))?;

        let response = client
            .execute_sql(Request::new(SqlRequest {
                sql: sql.to_string(),
            }))
            .await?
            .into_inner();

        // 1. Handle non-success status codes immediately
        if response.status_code != 200 {
            eprintln!("Error ({}): {}", response.status_code, response.message);
            return Ok(());
        }

        // 2. Print the operational message (e.g., "CREATE FUNCTION successful")
        let clean_msg = response.message.trim();
        if !clean_msg.is_empty() {
            if clean_msg.ends_with("found") {
                println!("  ✓ {}", clean_msg);
            } else {
                println!("{}", clean_msg);
            }
        }

        // 3. Strict Data Check: Only proceed if data is explicitly present and non-empty
        if let Some(bytes) = response.data {
            if !bytes.is_empty() {
                // format_arrow_data returns Ok(Some(Table)) ONLY if row_count > 0
                match self.format_arrow_data(&bytes) {
                    Ok(Some(table)) => println!("{}", table),
                    Ok(None) => {
                        // Data was present but contained 0 rows (e.g., empty result set)
                        // We print nothing here to keep output clean as requested
                    }
                    Err(e) => eprintln!("Failed to parse result data: {}", e),
                }
            }
        }

        Ok(())
    }

    fn format_arrow_data(&self, bytes: &[u8]) -> Result<Option<Table>, ReplError> {
        let cursor = Cursor::new(bytes);
        let reader = match StreamReader::try_new(cursor, None) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            // 表头下方用单线，不用双线
            .set_style(TableComponent::LeftHeaderIntersection, '├')
            .set_style(TableComponent::HeaderLines, '─')
            .set_style(TableComponent::MiddleHeaderIntersections, '┼')
            .set_style(TableComponent::RightHeaderIntersection, '┤');

        let mut has_rows = false;
        let mut header_initialized = false;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| ReplError::Internal(e.to_string()))?;
            if batch.num_rows() == 0 {
                continue;
            }
            has_rows = true;

            if !header_initialized {
                let headers: Vec<Cell> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        Cell::new(f.name())
                            .fg(Color::Cyan)
                            .add_attribute(Attribute::Bold)
                    })
                    .collect();
                table.set_header(headers);
                header_initialized = true;
            }

            let field_names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            for row_idx in 0..batch.num_rows() {
                let mut row_cells: Vec<Cell> = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let cell_val = self.extract_value(batch.column(col_idx), row_idx);
                    let cell = if field_names.get(col_idx).map(|s| s.as_str()) == Some("status") {
                        let c = Cell::new(cell_val.as_str());
                        match cell_val.as_str() {
                            "Running" => c.fg(Color::Green),
                            "Stopped" | "Failed" => c.fg(Color::Red),
                            _ => c,
                        }
                    } else {
                        Cell::new(cell_val)
                    };
                    row_cells.push(cell);
                }
                table.add_row(row_cells);
            }
        }

        if has_rows {
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    fn extract_value(&self, column: &dyn Array, row: usize) -> String {
        if column.is_null(row) {
            return "NULL".to_string();
        }

        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Int32 => column
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Int64 => column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Float32 => {
                format!(
                    "{:.4}",
                    column
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .value(row)
                )
            }
            DataType::Float64 => {
                format!(
                    "{:.4}",
                    column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                )
            }
            DataType::Boolean => column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row)
                .to_string(),
            _ => "[unsupported]".to_string(),
        }
    }

    pub async fn run_async(&mut self) -> io::Result<()> {
        println!("Function Stream SQL Interface");
        println!("Server: {}\n", self.server_address());

        if let Err(e) = self.connect().await {
            eprintln!("Error: {}", e);
            return Ok(());
        }

        loop {
            let input = match self.read_sql_input() {
                Ok(sql) => sql,
                Err(ReadlineError::Interrupted) => continue,
                Err(ReadlineError::Eof) => break,
                Err(e) => {
                    eprintln!("Read Error: {}", e);
                    break;
                }
            };

            if input.trim().is_empty() {
                continue;
            }

            match input.trim().to_lowercase().as_str() {
                "exit" | "quit" | "q" => break,
                "help" | "h" => self.print_help(),
                _ => {
                    if let Err(e) = self.execute_sql(&input).await {
                        eprintln!("SQL Execution Error: {}", e);
                    }
                }
            }
            println!();
        }

        if let Some(ref mut ed) = self.editor {
            let _ = ed.save_history(".function-stream-cli-history");
        }
        Ok(())
    }

    fn read_sql_input(&mut self) -> Result<String, ReadlineError> {
        let mut lines = Vec::new();
        loop {
            let prompt = if lines.is_empty() { "sql> " } else { "  -> " };

            let line = match self.editor.as_mut() {
                Some(ed) => {
                    let l = ed.readline(prompt)?;
                    if !l.trim().is_empty() {
                        ed.add_history_entry(l.as_str()).ok();
                    }
                    l
                }
                None => {
                    print!("{}", prompt);
                    io::stdout().flush().unwrap();
                    let mut l = String::new();
                    io::stdin().read_line(&mut l).unwrap();
                    l
                }
            };

            lines.push(line);
            let trimmed = lines.last().map(|s| s.trim()).unwrap_or("");

            if trimmed.ends_with(';') || self.is_balanced(&lines.join(" ")) {
                return Ok(lines.join(" ").trim().to_string());
            }
        }
    }

    fn is_balanced(&self, sql: &str) -> bool {
        let open = sql.chars().filter(|&c| c == '(').count();
        let close = sql.chars().filter(|&c| c == ')').count();
        open == close && !sql.trim().is_empty()
    }

    fn print_help(&self) {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_style(TableComponent::LeftHeaderIntersection, '├')
            .set_style(TableComponent::HeaderLines, '─')
            .set_style(TableComponent::MiddleHeaderIntersections, '┼')
            .set_style(TableComponent::RightHeaderIntersection, '┤')
            .set_header(
                vec!["Command", "Usage"]
                    .into_iter()
                    .map(|s| {
                        Cell::new(s)
                            .fg(Color::Cyan)
                            .add_attribute(Attribute::Bold)
                    })
                    .collect::<Vec<_>>(),
            )
            .add_row(vec!["HELP", "Show this message"])
            .add_row(vec!["EXIT", "Close connection"]);
        println!("{}", table);
    }
}
