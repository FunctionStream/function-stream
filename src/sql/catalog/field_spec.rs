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

use datafusion::arrow::datatypes::Field;
use datafusion::logical_expr::Expr;

/// Describes how a field in a connector table should be interpreted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldSpec {
    /// A regular struct field that maps to a column in the data.
    Struct(Field),
    /// A metadata field extracted from message metadata (e.g., Kafka headers).
    Metadata { field: Field, key: String },
    /// A virtual field computed from an expression over other fields.
    Virtual { field: Field, expression: Box<Expr> },
}

impl FieldSpec {
    pub fn is_virtual(&self) -> bool {
        matches!(self, FieldSpec::Virtual { .. })
    }

    pub fn field(&self) -> &Field {
        match self {
            FieldSpec::Struct(f) => f,
            FieldSpec::Metadata { field, .. } => field,
            FieldSpec::Virtual { field, .. } => field,
        }
    }

    pub fn metadata_key(&self) -> Option<&str> {
        match self {
            FieldSpec::Metadata { key, .. } => Some(key.as_str()),
            _ => None,
        }
    }
}

impl From<Field> for FieldSpec {
    fn from(value: Field) -> Self {
        FieldSpec::Struct(value)
    }
}
