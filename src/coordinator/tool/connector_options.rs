use std::collections::HashMap;
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use datafusion::common::{Result as DFResult, plan_datafusion_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{Expr, Ident, SqlOption, Value as SqlValue, ValueWithSpan};
use tracing::warn;

pub trait FromOpts: Sized {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self>;
}

pub struct ConnectorOptions {
    options: HashMap<String, Expr>,
    partitions: Vec<Expr>,
}

impl ConnectorOptions {
    pub fn new(sql_opts: &[SqlOption], partition_by: &Option<Vec<Expr>>) -> DFResult<Self> {
        let mut options = HashMap::new();

        for option in sql_opts {
            let SqlOption::KeyValue { key, value } = option else {
                return Err(plan_datafusion_err!(
                    "invalid with option: '{}'; expected an `=` delimited key-value pair",
                    option
                ));
            };

            options.insert(key.value.clone(), value.clone());
        }

        Ok(Self {
            options,
            partitions: partition_by.clone().unwrap_or_default(),
        })
    }

    pub fn partitions(&self) -> &[Expr] {
        &self.partitions
    }

    pub fn pull_struct<T: FromOpts>(&mut self) -> DFResult<T> {
        T::from_opts(self)
    }

    pub fn pull_opt_str(&mut self, name: &str) -> DFResult<Option<String>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => Ok(Some(s)),
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be a single-quoted string, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_str(&mut self, name: &str) -> DFResult<String> {
        self.pull_opt_str(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_bool(&mut self, name: &str) -> DFResult<Option<bool>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Boolean(b),
                span: _,
            })) => Ok(Some(b)),
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => match s.as_str() {
                "true" | "yes" => Ok(Some(true)),
                "false" | "no" => Ok(Some(false)),
                _ => Err(plan_datafusion_err!(
                    "expected with option '{}' to be a boolean, but it was `'{}'`",
                    name,
                    s
                )),
            },
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be a boolean, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_opt_u64(&mut self, name: &str) -> DFResult<Option<u64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<u64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be an unsigned integer, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be an unsigned integer, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_opt_nonzero_u64(&mut self, name: &str) -> DFResult<Option<NonZero<u64>>> {
        match self.pull_opt_u64(name)? {
            Some(0) => Err(plan_datafusion_err!(
                "expected with option '{name}' to be greater than 0, but it was 0"
            )),
            Some(i) => Ok(Some(NonZeroU64::new(i).unwrap())),
            None => Ok(None),
        }
    }

    pub fn pull_opt_data_size_bytes(&mut self, name: &str) -> DFResult<Option<u64>> {
        self.pull_opt_str(name)?
            .map(|s| {
                s.parse::<u64>().map_err(|_| {
                    plan_datafusion_err!(
                        "expected with option '{}' to be a size in bytes (unsigned integer), but it was `{}`",
                        name,
                        s
                    )
                })
            })
            .transpose()
    }

    pub fn pull_opt_i64(&mut self, name: &str) -> DFResult<Option<i64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<i64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be an integer, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be an integer, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_i64(&mut self, name: &str) -> DFResult<i64> {
        self.pull_opt_i64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_u64(&mut self, name: &str) -> DFResult<u64> {
        self.pull_opt_u64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_f64(&mut self, name: &str) -> DFResult<Option<f64>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::Number(s, _),
                span: _,
            }))
            | Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => s.parse::<f64>().map(Some).map_err(|_| {
                plan_datafusion_err!(
                    "expected with option '{}' to be a double, but it was `{}`",
                    name,
                    s
                )
            }),
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be a double, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_f64(&mut self, name: &str) -> DFResult<f64> {
        self.pull_opt_f64(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_bool(&mut self, name: &str) -> DFResult<bool> {
        self.pull_opt_bool(name)?
            .ok_or_else(|| plan_datafusion_err!("required option '{}' not set", name))
    }

    pub fn pull_opt_duration(&mut self, name: &str) -> DFResult<Option<Duration>> {
        match self.options.remove(name) {
            Some(e) => Ok(Some(duration_from_sql_expr(&e).map_err(|e| {
                plan_datafusion_err!("in with clause '{name}': {}", e)
            })?)),
            None => Ok(None),
        }
    }

    pub fn pull_opt_field(&mut self, name: &str) -> DFResult<Option<String>> {
        match self.options.remove(name) {
            Some(Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span: _,
            })) => {
                warn!(
                    "Referred to a field in `{name}` with a string—this is deprecated and will be unsupported after Arroyo 0.14"
                );
                Ok(Some(s))
            }
            Some(Expr::Identifier(Ident { value, .. })) => Ok(Some(value)),
            Some(e) => Err(plan_datafusion_err!(
                "expected with option '{}' to be a field, but it was `{:?}`",
                name,
                e
            )),
            None => Ok(None),
        }
    }

    pub fn pull_opt_array(&mut self, name: &str) -> Option<Vec<Expr>> {
        Some(match self.options.remove(name)? {
            Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                span,
            }) => s
                .split(',')
                .map(|p| {
                    Expr::Value(ValueWithSpan {
                        value: SqlValue::SingleQuotedString(p.to_string()),
                        span: span.clone(),
                    })
                })
                .collect(),
            Expr::Array(a) => a.elem,
            e => vec![e],
        })
    }

    pub fn pull_opt_parsed<T: FromStr>(&mut self, name: &str) -> DFResult<Option<T>> {
        Ok(match self.pull_opt_str(name)? {
            Some(s) => Some(
                s.parse()
                    .map_err(|_| plan_datafusion_err!("invalid value '{s}' for {name}"))?,
            ),
            None => None,
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.options.keys()
    }

    pub fn keys_with_prefix<'a, 'b>(
        &'a self,
        prefix: &'b str,
    ) -> impl Iterator<Item = &'a String> + 'b
    where
        'a: 'b,
    {
        self.options.keys().filter(move |k| k.starts_with(prefix))
    }

    pub fn insert_str(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> DFResult<Option<String>> {
        let name = name.into();
        let value = value.into();
        let existing = self.pull_opt_str(&name)?;
        self.options.insert(
            name,
            Expr::Value(SqlValue::SingleQuotedString(value).with_empty_span()),
        );
        Ok(existing)
    }

    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.options.contains_key(key)
    }
}

fn duration_from_sql_expr(expr: &Expr) -> Result<Duration, DataFusionError> {
    match expr {
        Expr::Interval(interval) => {
            let s = match interval.value.as_ref() {
                Expr::Value(ValueWithSpan {
                    value: SqlValue::SingleQuotedString(s),
                    ..
                }) => s.clone(),
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "expected interval string literal, found {other}"
                    )));
                }
            };
            parse_interval_to_duration(&s)
        }
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) => parse_interval_to_duration(s),
        other => Err(DataFusionError::Plan(format!(
            "expected an interval expression, found {other}"
        ))),
    }
}

fn parse_interval_to_duration(s: &str) -> Result<Duration, DataFusionError> {
    let parts: Vec<&str> = s.trim().split_whitespace().collect();
    if parts.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "invalid interval string '{s}'; expected '<value> <unit>'"
        )));
    }
    let value: u64 = parts[0]
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("invalid interval number: {}", parts[0])))?;
    let duration = match parts[1].to_lowercase().as_str() {
        "second" | "seconds" | "s" => Duration::from_secs(value),
        "minute" | "minutes" | "min" => Duration::from_secs(value * 60),
        "hour" | "hours" | "h" => Duration::from_secs(value * 3600),
        "day" | "days" | "d" => Duration::from_secs(value * 86400),
        unit => {
            return Err(DataFusionError::Plan(format!(
                "unsupported interval unit '{unit}'"
            )));
        }
    };
    Ok(duration)
}
