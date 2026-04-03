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

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::Expr;
use datafusion_expr::ExprSchemable;

pub trait SchemaContext {
    fn resolve_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expr>;
    fn extract_datatype(&self, expr: &Expr, schema: &Schema) -> Result<DataType>;
}

/// [`SchemaContext`] backed by a [`DFSchema`] built from the physical Arrow schema.
pub struct DfSchemaContext;

impl SchemaContext for DfSchemaContext {
    fn resolve_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expr> {
        let df = DFSchema::try_from(schema.clone())?;
        let _ = expr.get_type(&df)?;
        Ok(expr.clone())
    }

    fn extract_datatype(&self, expr: &Expr, schema: &Schema) -> Result<DataType> {
        let df = DFSchema::try_from(schema.clone())?;
        expr.get_type(&df)
    }
}
