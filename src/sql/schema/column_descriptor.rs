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

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::logical_expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnDescriptor {
    Physical(Field),
    SystemMeta {
        field: Field,
        meta_key: String,
    },
    Computed {
        field: Field,
        logic: Box<Expr>,
    },
}

impl ColumnDescriptor {
    #[inline]
    pub fn new_physical(field: Field) -> Self {
        Self::Physical(field)
    }

    #[inline]
    pub fn new_system_meta(field: Field, meta_key: impl Into<String>) -> Self {
        Self::SystemMeta {
            field,
            meta_key: meta_key.into(),
        }
    }

    #[inline]
    pub fn new_computed(field: Field, logic: Expr) -> Self {
        Self::Computed {
            field,
            logic: Box::new(logic),
        }
    }

    #[inline]
    pub fn arrow_field(&self) -> &Field {
        match self {
            Self::Physical(f) => f,
            Self::SystemMeta { field: f, .. } => f,
            Self::Computed { field: f, .. } => f,
        }
    }

    #[inline]
    pub fn into_arrow_field(self) -> Field {
        match self {
            Self::Physical(f) => f,
            Self::SystemMeta { field: f, .. } => f,
            Self::Computed { field: f, .. } => f,
        }
    }

    #[inline]
    pub fn is_computed(&self) -> bool {
        matches!(self, Self::Computed { .. })
    }

    #[inline]
    pub fn is_physical(&self) -> bool {
        matches!(self, Self::Physical(_))
    }

    #[inline]
    pub fn system_meta_key(&self) -> Option<&str> {
        if let Self::SystemMeta { meta_key, .. } = self {
            Some(meta_key.as_str())
        } else {
            None
        }
    }

    #[inline]
    pub fn computation_logic(&self) -> Option<&Expr> {
        if let Self::Computed { logic, .. } = self {
            Some(logic)
        } else {
            None
        }
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        self.arrow_field().data_type()
    }

    pub fn force_precision(&mut self, unit: TimeUnit) {
        match self {
            Self::Physical(f) => {
                if let DataType::Timestamp(_, tz) = f.data_type() {
                    *f = Field::new(f.name(), DataType::Timestamp(unit, tz.clone()), f.is_nullable());
                }
            }
            Self::SystemMeta { field, .. } => {
                if let DataType::Timestamp(_, tz) = field.data_type() {
                    *field = Field::new(
                        field.name(),
                        DataType::Timestamp(unit, tz.clone()),
                        field.is_nullable(),
                    );
                }
            }
            Self::Computed { field, .. } => {
                if let DataType::Timestamp(_, tz) = field.data_type() {
                    *field = Field::new(
                        field.name(),
                        DataType::Timestamp(unit, tz.clone()),
                        field.is_nullable(),
                    );
                }
            }
        }
    }
}

impl From<Field> for ColumnDescriptor {
    #[inline]
    fn from(field: Field) -> Self {
        Self::Physical(field)
    }
}
