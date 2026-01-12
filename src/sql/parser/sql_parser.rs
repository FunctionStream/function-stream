use pest::Parser;
use pest_derive::Parser;

use super::ParseError;
use crate::sql::statement::{
    CreateWasmTask, DropWasmTask, ShowWasmTasks, StartWasmTask, Statement, StopWasmTask,
};
use std::collections::HashMap;

#[derive(Parser)]
#[grammar = "src/sql/grammar.pest"]
struct Grammar;

#[derive(Debug, Default)]
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(sql: &str) -> Result<Box<dyn Statement>, ParseError> {
        let pairs = Grammar::parse(Rule::statement, sql)
            .map_err(|e| ParseError::new(format!("Parse error: {}", e)))?;

        for pair in pairs {
            return match pair.as_rule() {
                Rule::create_stmt => {
                    handle_create_stmt(pair).map(|stmt| stmt as Box<dyn Statement>)
                }
                Rule::drop_stmt => handle_drop_stmt(pair).map(|stmt| stmt as Box<dyn Statement>),
                Rule::start_stmt => handle_start_stmt(pair).map(|stmt| stmt as Box<dyn Statement>),
                Rule::stop_stmt => handle_stop_stmt(pair).map(|stmt| stmt as Box<dyn Statement>),
                Rule::show_stmt => handle_show_stmt(pair).map(|stmt| stmt as Box<dyn Statement>),
                _ => continue,
            };
        }

        Err(ParseError::new("Unknown statement type"))
    }
}

fn handle_create_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Box<CreateWasmTask>, ParseError> {
    let mut inner = pair.into_inner();
    // Note: name is read from config file, not from SQL statement
    // Pass empty string here, name will be read from config file later
    let properties = inner
        .next()
        .map(parse_properties)
        .ok_or_else(|| ParseError::new("Missing WITH clause"))?;

    Ok(Box::new(CreateWasmTask::new(String::new(), properties)))
}

fn handle_drop_stmt(pair: pest::iterators::Pair<Rule>) -> Result<Box<DropWasmTask>, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().map(extract_string).unwrap_or_default();
    Ok(Box::new(DropWasmTask::new(name)))
}

fn handle_start_stmt(pair: pest::iterators::Pair<Rule>) -> Result<Box<StartWasmTask>, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().map(extract_string).unwrap_or_default();
    Ok(Box::new(StartWasmTask::new(name)))
}

fn handle_stop_stmt(pair: pest::iterators::Pair<Rule>) -> Result<Box<StopWasmTask>, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().map(extract_string).unwrap_or_default();
    Ok(Box::new(StopWasmTask::new(name)))
}

fn handle_show_stmt(_pair: pest::iterators::Pair<Rule>) -> Result<Box<ShowWasmTasks>, ParseError> {
    Ok(Box::new(ShowWasmTasks::new()))
}

fn extract_string(pair: pest::iterators::Pair<Rule>) -> String {
    match pair.as_rule() {
        Rule::string_literal => {
            let s = pair.as_str();
            if (s.starts_with('\'') && s.ends_with('\''))
                || (s.starts_with('"') && s.ends_with('"'))
            {
                unescape_string(&s[1..s.len() - 1])
            } else {
                unescape_string(s)
            }
        }
        Rule::identifier => pair.as_str().to_string(),
        _ => pair.as_str().to_string(),
    }
}

fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(&next) = chars.peek() {
                chars.next();
                match next {
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    'r' => result.push('\r'),
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    '"' => result.push('"'),
                    _ => {
                        result.push('\\');
                        result.push(next);
                    }
                }
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

fn parse_properties(pair: pest::iterators::Pair<Rule>) -> HashMap<String, String> {
    let mut properties = HashMap::new();

    for prop in pair.into_inner() {
        if prop.as_rule() == Rule::property {
            let mut inner = prop.into_inner();
            if let (Some(key_pair), Some(val_pair)) = (inner.next(), inner.next()) {
                let key = key_pair
                    .into_inner()
                    .next()
                    .map(extract_string)
                    .unwrap_or_default();
                let value = val_pair
                    .into_inner()
                    .next()
                    .map(extract_string)
                    .unwrap_or_default();
                properties.insert(key, value);
            }
        }
    }

    properties
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_wasm_task() {
        let sql = "CREATE WASMTASK WITH ('wasm-path'='./test.wasm', 'config-path'='./config.yml')";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_create_wasm_task_minimal() {
        let sql = "CREATE WASMTASK WITH ('wasm-path'='./processor.wasm')";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_drop_wasm_task() {
        let sql = "DROP WASMTASK my_task";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_start_wasm_task() {
        let sql = "START WASMTASK my_task";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_stop_wasm_task() {
        let sql = "STOP WASMTASK my_task";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_show_wasm_tasks() {
        let sql = "SHOW WASMTASKS";
        let stmt = SqlParser::parse(sql).unwrap();
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let sql1 = "create wasmtask with ('wasm-path'='./test.wasm')";
        let stmt1 = SqlParser::parse(sql1).unwrap();

        let sql2 = "Create WasmTask With ('wasm-path'='./test.wasm')";
        let stmt2 = SqlParser::parse(sql2).unwrap();

        let sql3 = "show wasmtasks";
        let stmt3 = SqlParser::parse(sql3).unwrap();

        let sql4 = "start wasmtask my_task";
        let stmt4 = SqlParser::parse(sql4).unwrap();
    }

    #[test]
    fn test_with_extra_properties() {
        let sql = r#"CREATE WASMTASK WITH (
            'wasm-path'='./test.wasm',
            'config-path'='./config.yml',
            'parallelism'='4',
            'memory-limit'='256mb'
        )"#;
        let stmt = SqlParser::parse(sql).unwrap();
    }
}
