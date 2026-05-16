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

//! Pure AST inspection helpers.
//!
//! This module is deliberately stateless: it only knows how to pull data out
//! of `sqlparser` AST nodes. Nothing here touches `StreamSchemaProvider`,
//! connectors, Builders, or logical plans. Put any piece of code that would
//! be "fine" to unit test on a naked AST here — and only here.

use datafusion::common::{Result, plan_err};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, SqlOption, TableConstraint};

use crate::common::with_option_keys as opt;

/// Namespace for AST extraction helpers.
pub struct AstUtils;

impl AstUtils {
    /// Extract the single PRIMARY KEY column list from the constraint
    /// clauses of a `CREATE TABLE`. Rejects multiple PRIMARY KEY declarations.
    pub fn parse_primary_keys(constraints: &[TableConstraint]) -> Result<Vec<String>> {
        let mut keys = None;
        for constraint in constraints {
            if let TableConstraint::PrimaryKey { columns, .. } = constraint {
                if keys.is_some() {
                    return plan_err!(
                        "Constraint Violation: Multiple PRIMARY KEY constraints are forbidden"
                    );
                }
                keys = Some(columns.iter().map(|ident| ident.value.clone()).collect());
            }
        }
        Ok(keys.unwrap_or_default())
    }

    /// Extract the (at most one) `WATERMARK FOR col [AS expr]` clause from
    /// the constraint list. The resulting tuple is `(column_name, opt_expr)`
    /// so the caller can decide whether it is legal in its context.
    pub fn parse_watermark_strategy(
        constraints: &[TableConstraint],
    ) -> Result<Option<(String, Option<SqlExpr>)>> {
        let mut strategy = None;
        for constraint in constraints {
            if let TableConstraint::Watermark {
                column_name,
                watermark_expr,
            } = constraint
            {
                if strategy.is_some() {
                    return plan_err!(
                        "Constraint Violation: Only a single WATERMARK FOR clause is permitted"
                    );
                }
                strategy = Some((column_name.value.clone(), watermark_expr.clone()));
            }
        }
        Ok(strategy)
    }

    /// True iff the WITH clause declares a `connector=` property. Used by
    /// the router to decide whether to hand off to the external-table
    /// compiler.
    pub fn contains_connector_property(options: &[SqlOption]) -> bool {
        options.iter().any(|o| match o {
            SqlOption::KeyValue { key, .. } => key.value.eq_ignore_ascii_case(opt::CONNECTOR),
            _ => false,
        })
    }

    /// Peek at the declared `type` in the WITH clause without consuming it,
    /// returning its lowercased value. Used by the router to split the
    /// source- and sink-compile paths before either Builder runs.
    pub fn peek_table_role(with_options: &[SqlOption]) -> Option<String> {
        with_options.iter().find_map(|o| match o {
            SqlOption::KeyValue { key, value } if key.value.eq_ignore_ascii_case(opt::TYPE) => {
                Some(value.to_string().trim_matches('\'').to_ascii_lowercase())
            }
            _ => None,
        })
    }
}
