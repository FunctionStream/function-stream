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

// REPL (Read-Eval-Print Loop) for interactive SQL execution

use protocol::cli::{
    function_stream_service_client::FunctionStreamServiceClient,
    SqlRequest,
};
use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor, EditMode};
use std::io::{self, Write};
use tonic::Request;

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

        let editor = match DefaultEditor::with_config(config) {
            Ok(mut ed) => {
                let _ = ed.load_history(".function-stream-cli-history");
                Some(ed)
            }
            Err(_) => None,
        };

        Self {
            client: None,
            server_host,
            server_port,
            editor,
        }
    }

    pub fn server_address(&self) -> String {
        format!("http://{}:{}", self.server_host, self.server_port)
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        let addr = self.server_address();
        let client = FunctionStreamServiceClient::connect(addr.clone())
            .await
            .map_err(|e| format!("Failed to connect to server {}: {}", addr, e))?;
        self.client = Some(client);
        Ok(())
    }

    pub async fn execute_sql(&mut self, sql: &str) -> Result<String, String> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| "Not connected to server".to_string())?;

        let request = Request::new(SqlRequest {
            sql: sql.to_string(),
        });

        match client.execute_sql(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.status_code == 200 {
                    let formatted = self.format_output(&resp.message, resp.data.as_deref());
                    Ok(formatted)
                } else {
                    Err(format!(
                        "Error (status {}): {}",
                        resp.status_code, resp.message
                    ))
                }
            }
            Err(e) => Err(format!("Execution error: {}", e)),
        }
    }

    fn format_output(&self, message: &str, data: Option<&str>) -> String {
        let mut output = String::new();

        let clean_message = message
            .trim()
            .replace("        ", " ")
            .replace("  ", " ")
            .replace("WASMTASKs", "WASMTASKS");

        if let Some(data_str) = data {
            let data_trimmed = data_str.trim();
            if data_trimmed == "[]" {
                if !clean_message.is_empty() {
                    output.push_str(&clean_message);
                }
                return output;
            }
        }

        if !clean_message.is_empty() {
            output.push_str(&clean_message);
        }

        if let Some(data_str) = data {
            let data_trimmed = data_str.trim();
            if !data_trimmed.is_empty() && data_trimmed != "[]" {
                if data_trimmed.starts_with('[') && data_trimmed.ends_with(']') {
                    if let Ok(formatted) = self.format_json_array(data_trimmed) {
                        if !formatted.is_empty() {
                            if !output.is_empty() {
                                output.push_str("\n");
                            }
                            output.push_str(&formatted);
                        }
                    } else {
                        if !output.is_empty() {
                            output.push_str("\n");
                        }
                        output.push_str(data_trimmed);
                    }
                } else {
                    if !output.is_empty() {
                        output.push_str("\n");
                    }
                    output.push_str(data_trimmed);
                }
            }
        }

        output
    }

    fn format_json_array(&self, json_str: &str) -> Result<String, ()> {
        if json_str == "[]" {
            return Ok(String::new());
        }

        let trimmed = json_str.trim_start_matches('[').trim_end_matches(']');
        if trimmed.is_empty() {
            return Ok(String::new());
        }

        let items: Vec<&str> = trimmed.split("},{").collect();
        let mut rows = Vec::new();

        for item in items.iter() {
            let trimmed_item = item.trim();
            let mut clean_item = String::new();

            if !trimmed_item.starts_with('{') {
                clean_item.push('{');
            }
            clean_item.push_str(trimmed_item);
            if !trimmed_item.ends_with('}') {
                clean_item.push('}');
            }

            if let Ok((name, wasm_path, state, created_at, started_at)) =
                self.parse_task_json(&clean_item)
            {
                rows.push((name, wasm_path, state, created_at, started_at));
            }
        }

        if rows.is_empty() {
            return Ok(String::new());
        }

        let name_width = rows.iter().map(|r| r.0.len()).max().unwrap_or(10).max(4);
        let path_width = rows.iter().map(|r| r.1.len()).max().unwrap_or(20).max(9);
        let state_width = rows.iter().map(|r| r.2.len()).max().unwrap_or(7).max(5);
        let created_width = rows.iter().map(|r| r.3.len()).max().unwrap_or(19).max(10);
        let started_width = rows.iter().map(|r| r.4.len()).max().unwrap_or(19).max(10);

        let mut table = String::new();

        table.push_str(&format!(
            "+{}+{}+{}+{}+{}+\n",
            "-".repeat(name_width + 2),
            "-".repeat(path_width + 2),
            "-".repeat(state_width + 2),
            "-".repeat(created_width + 2),
            "-".repeat(started_width + 2)
        ));
        table.push_str(&format!(
            "| {:<width$} | {:<path$} | {:<state$} | {:<created$} | {:<started$} |\n",
            "Name",
            "WasmPath",
            "State",
            "CreatedAt",
            "StartedAt",
            width = name_width,
            path = path_width,
            state = state_width,
            created = created_width,
            started = started_width
        ));
        table.push_str(&format!(
            "+{}+{}+{}+{}+{}+\n",
            "-".repeat(name_width + 2),
            "-".repeat(path_width + 2),
            "-".repeat(state_width + 2),
            "-".repeat(created_width + 2),
            "-".repeat(started_width + 2)
        ));

        for (name, wasm_path, state, created_at, started_at) in rows {
            table.push_str(&format!(
                "| {:<width$} | {:<path$} | {:<state$} | {:<created$} | {:<started$} |\n",
                name,
                wasm_path,
                state,
                created_at,
                started_at,
                width = name_width,
                path = path_width,
                state = state_width,
                created = created_width,
                started = started_width
            ));
        }

        table.push_str(&format!(
            "+{}+{}+{}+{}+{}+\n",
            "-".repeat(name_width + 2),
            "-".repeat(path_width + 2),
            "-".repeat(state_width + 2),
            "-".repeat(created_width + 2),
            "-".repeat(started_width + 2)
        ));

        Ok(table)
    }

    fn parse_task_json(&self, json: &str) -> Result<(String, String, String, String, String), ()> {
        let name = self
            .extract_json_field(json, "name")
            .unwrap_or_else(|| "N/A".to_string());
        let wasm_path = self
            .extract_json_field(json, "wasm_path")
            .unwrap_or_else(|| "N/A".to_string());
        let state = self
            .extract_json_field(json, "state")
            .unwrap_or_else(|| "N/A".to_string());
        let created_at = self
            .extract_json_field(json, "created_at")
            .unwrap_or_else(|| "N/A".to_string());
        let started_at = self
            .extract_json_field(json, "started_at")
            .unwrap_or_else(|| "null".to_string());

        let created_at_formatted = if created_at != "N/A" {
            created_at
                .replace("SystemTime { tv_sec: ", "")
                .replace(", tv_nsec: ", ".")
                .replace(" }", "")
                .split('.')
                .next()
                .unwrap_or(&created_at)
                .to_string()
        } else {
            created_at
        };

        let started_at_formatted = if started_at == "null" || started_at.is_empty() {
            "-".to_string()
        } else {
            started_at
                .replace("SystemTime { tv_sec: ", "")
                .replace(", tv_nsec: ", ".")
                .replace(" }", "")
                .split('.')
                .next()
                .unwrap_or(&started_at)
                .to_string()
        };

        Ok((
            name,
            wasm_path,
            state,
            created_at_formatted,
            started_at_formatted,
        ))
    }

    fn extract_json_field(&self, json: &str, field: &str) -> Option<String> {
        let field_pattern = format!("\"{}\":", field);
        if let Some(start) = json.find(&field_pattern) {
            let value_start = start + field_pattern.len();
            let value_str = &json[value_start..];
            let value_str = value_str.trim_start();

            if value_str.starts_with('"') {
                let start = 1;
                if let Some(end) = value_str[start..].find('"') {
                    return Some(value_str[start..start + end].to_string());
                }
            } else if value_str.starts_with("null") {
                return Some("null".to_string());
            } else {
                let end = value_str
                    .find(',')
                    .or_else(|| value_str.find('}'))
                    .unwrap_or(value_str.len());
                return Some(value_str[..end].trim().to_string());
            }
        }
        None
    }

    fn is_sql_complete(&self, sql: &str) -> bool {
        let trimmed = sql.trim();

        if trimmed.is_empty() {
            return true;
        }

        let mut paren_count = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for ch in trimmed.chars() {
            if escape_next {
                escape_next = false;
                continue;
            }

            if ch == '\\' {
                escape_next = true;
                continue;
            }

            if ch == '\'' || ch == '"' {
                in_string = !in_string;
                continue;
            }

            if !in_string {
                match ch {
                    '(' => paren_count += 1,
                    ')' => {
                        if paren_count > 0 {
                            paren_count -= 1;
                        }
                    }
                    _ => {}
                }
            }
        }

        paren_count == 0 && !trimmed.ends_with('\\')
    }

    fn read_multiline_sql(&mut self) -> io::Result<String> {
        let mut lines = Vec::new();
        let mut is_first_line = true;

        loop {
            let prompt = if is_first_line {
                "FunctionStream> "
            } else {
                "         > "
            };

            let line = if let Some(ref mut editor) = self.editor {
                match editor.readline(prompt) {
                    Ok(line) => {
                        if !line.trim().is_empty() {
                            let _ = editor.add_history_entry(line.as_str());
                        }
                        line
                    }
                    Err(ReadlineError::Interrupted) => {
                        return Err(io::Error::new(io::ErrorKind::Interrupted, "Interrupted"));
                    }
                    Err(ReadlineError::Eof) => {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
                    }
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Readline error: {}", e),
                        ));
                    }
                }
            } else {
                print!("{}", prompt);
                io::stdout().flush()?;
                let mut line = String::new();
                io::stdin().read_line(&mut line)?;
                line
            };

            let trimmed_line = line.trim_start();
            let trimmed = trimmed_line.trim();

            if is_first_line && trimmed.is_empty() {
                continue;
            }

            lines.push(trimmed_line.to_string());
            let combined = lines.join(" ");

            if self.is_sql_complete(&combined) {
                return Ok(combined.trim().to_string());
            }

            is_first_line = false;
        }
    }

    pub async fn run_async(&mut self) -> io::Result<()> {
        println!("========================================");
        println!("  Function Stream SQL CLI");
        println!("========================================");
        println!("Server: {}", self.server_address());
        println!();

        if self.client.is_none() {
            if let Err(e) = self.connect().await {
                eprintln!("Failed to connect to server: {}", e);
                return Err(io::Error::new(io::ErrorKind::ConnectionRefused, e));
            }
        }

        println!("Connected to server.");
        println!("Type SQL statements or 'exit'/'quit' to exit.");
        println!("Type 'help' for help.");
        println!();

        loop {
            let input = match self.read_multiline_sql() {
                Ok(input) => input,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    println!("\n(Use 'exit' or 'quit' to exit)");
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    println!("\nGoodbye!");
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading input: {}", e);
                    break;
                }
            };

            if input.is_empty() {
                continue;
            }

            match input.trim().to_lowercase().as_str() {
                "exit" | "quit" | "q" => {
                    println!("Goodbye!");
                    break;
                }
                "help" | "h" => {
                    self.show_help();
                    continue;
                }
                _ => match self.execute_sql(&input).await {
                    Ok(result) => {
                        println!("{}", result);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                },
            }
            println!();
        }

        if let Some(ref mut editor) = self.editor {
            let _ = editor.save_history(".function-stream-cli-history");
        }

        Ok(())
    }

    fn show_help(&self) {
        println!();
        println!("Available commands:");
        println!("  help, h          - Show this help message");
        println!("  exit, quit, q     - Exit the CLI");
        println!();
        println!("SQL Statements:");
        println!("  CREATE WASMTASK <name> WITH (wasm-path='<path>', ...)");
        println!("  DROP WASMTASK <name>");
        println!("  START WASMTASK <name>");
        println!("  STOP WASMTASK <name>");
        println!("  SHOW WASMTASKS");
        println!();
        println!("Multi-line Input:");
        println!("  - Press Enter to continue on next line");
        println!("  - Press Enter on empty line to execute");
        println!("  - Parentheses are automatically matched");
        println!();
        println!("Command History:");
        println!("  - Use ↑/↓ arrow keys to browse previous commands");
        println!("  - History is saved to .function-stream-cli-history");
        println!();
        println!("Examples:");
        println!("  CREATE WASMTASK my_task WITH (wasm-path='./test.wasm')");
        println!("  CREATE WASMTASK my_task WITH (");
        println!("    wasm-path='./test.wasm',");
        println!("    config-path='./config.json'");
        println!("  )");
        println!("  START WASMTASK my_task");
        println!("  SHOW WASMTASKS");
        println!("  STOP WASMTASK my_task");
        println!("  DROP WASMTASK my_task");
        println!();
    }
}
