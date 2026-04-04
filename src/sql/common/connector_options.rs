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

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use datafusion::common::{Result as DFResult, plan_datafusion_err};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{Expr, Ident, SqlOption, Value as SqlValue, ValueWithSpan};
use tracing::warn;

use super::constants::{interval_duration_unit, with_opt_bool_str};

pub trait FromOpts: Sized {
    fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self>;
}

pub struct ConnectorOptions {
    options: HashMap<String, Expr>,
    partitions: Vec<Expr>,
}

fn sql_expr_to_catalog_string(e: &Expr) -> String {
    match e {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
            SqlValue::NationalStringLiteral(s) => s.clone(),
            SqlValue::HexStringLiteral(s) => s.clone(),
            SqlValue::Number(n, _) => n.clone(),
            SqlValue::Boolean(b) => b.to_string(),
            SqlValue::Null => "NULL".to_string(),
            other => other.to_string(),
        },
        Expr::Identifier(ident) => ident.value.clone(),
        other => other.to_string(),
    }
}

impl ConnectorOptions {
    /// Build options from persisted catalog string maps (same semantics as SQL `WITH` literals).
    pub fn from_flat_string_map(map: HashMap<String, String>) -> DFResult<Self> {
        let mut options = HashMap::with_capacity(map.len());
        for (k, v) in map {
            options.insert(
                k,
                Expr::Value(SqlValue::SingleQuotedString(v).with_empty_span()),
            );
        }
        Ok(Self {
            options,
            partitions: Vec::new(),
        })
    }

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
                with_opt_bool_str::TRUE | with_opt_bool_str::YES => Ok(Some(true)),
                with_opt_bool_str::FALSE | with_opt_bool_str::NO => Ok(Some(false)),
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
                        span,
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

    /// Drain all remaining options into string values (for connector runtime config).
    pub fn drain_remaining_string_values(&mut self) -> DFResult<HashMap<String, String>> {
        let taken = std::mem::take(&mut self.options);
        let mut out = HashMap::with_capacity(taken.len());
        for (k, v) in taken {
            out.insert(k, format!("{v}"));
        }
        Ok(out)
    }

    /// Snapshot of all current `WITH` key/value pairs for catalog persistence (`SHOW CREATE TABLE`).
    /// Call before any `pull_*` consumes options.
    pub fn snapshot_for_catalog(&self) -> BTreeMap<String, String> {
        self.options
            .iter()
            .map(|(k, v)| (k.clone(), sql_expr_to_catalog_string(v)))
            .collect()
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
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "invalid interval string '{s}'; expected '<value> <unit>'"
        )));
    }
    let value: u64 = parts[0]
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("invalid interval number: {}", parts[0])))?;
    let unit_lc = parts[1].to_lowercase();
    let unit = unit_lc.as_str();
    let duration = match unit {
        interval_duration_unit::SECOND
        | interval_duration_unit::SECONDS
        | interval_duration_unit::S => Duration::from_secs(value),
        interval_duration_unit::MINUTE
        | interval_duration_unit::MINUTES
        | interval_duration_unit::MIN => Duration::from_secs(value * 60),
        interval_duration_unit::HOUR
        | interval_duration_unit::HOURS
        | interval_duration_unit::H => Duration::from_secs(value * 3600),
        interval_duration_unit::DAY | interval_duration_unit::DAYS | interval_duration_unit::D => {
            Duration::from_secs(value * 86400)
        }
        unit => {
            return Err(DataFusionError::Plan(format!(
                "unsupported interval unit '{unit}'"
            )));
        }
    };
    Ok(duration)
}
