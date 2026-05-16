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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Column, DFSchema, Result, TableReference};

// ============================================================================
// QualifiedField (Strongly-typed Field Wrapper)
// ============================================================================

/// Arrow [`Field`] plus optional SQL [`TableReference`] qualifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedField {
    qualifier: Option<TableReference>,
    field: FieldRef,
}

// ============================================================================
// Type Conversions (Interoperability with DataFusion)
// ============================================================================

impl From<(Option<TableReference>, FieldRef)> for QualifiedField {
    fn from((qualifier, field): (Option<TableReference>, FieldRef)) -> Self {
        Self { qualifier, field }
    }
}

impl From<(Option<&TableReference>, &Field)> for QualifiedField {
    fn from((qualifier, field): (Option<&TableReference>, &Field)) -> Self {
        Self {
            qualifier: qualifier.cloned(),
            field: Arc::new(field.clone()),
        }
    }
}

impl From<QualifiedField> for (Option<TableReference>, FieldRef) {
    fn from(value: QualifiedField) -> Self {
        (value.qualifier, value.field)
    }
}

// ============================================================================
// Core API
// ============================================================================

impl QualifiedField {
    pub fn new(
        qualifier: Option<TableReference>,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            qualifier,
            field: Arc::new(Field::new(name, data_type, nullable)),
        }
    }

    pub fn new_unqualified(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            qualifier: None,
            field: Arc::new(Field::new(name, data_type, nullable)),
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.field.name()
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    #[inline]
    pub fn metadata(&self) -> &HashMap<String, String> {
        self.field.metadata()
    }

    #[inline]
    pub fn qualifier(&self) -> Option<&TableReference> {
        self.qualifier.as_ref()
    }

    #[inline]
    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(qualifier) => format!("{}.{}", qualifier, self.field.name()),
            None => self.field.name().to_owned(),
        }
    }

    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.field.name().to_string(),
            spans: Default::default(),
        }
    }

    pub fn unqualified_column(&self) -> Column {
        Column {
            relation: None,
            name: self.field.name().to_string(),
            spans: Default::default(),
        }
    }

    pub fn strip_qualifier(mut self) -> Self {
        self.qualifier = None;
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        if self.field.is_nullable() == nullable {
            return self;
        }
        let field = Arc::try_unwrap(self.field).unwrap_or_else(|arc| (*arc).clone());
        self.field = Arc::new(field.with_nullable(nullable));
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        let field = Arc::try_unwrap(self.field).unwrap_or_else(|arc| (*arc).clone());
        self.field = Arc::new(field.with_metadata(metadata));
        self
    }
}

// ============================================================================
// Schema Collection Helpers
// ============================================================================

pub fn extract_qualified_fields(schema: &DFSchema) -> Vec<QualifiedField> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let (qualifier, _) = schema.qualified_field(i);
            QualifiedField {
                qualifier: qualifier.cloned(),
                field: field.clone(),
            }
        })
        .collect()
}

pub fn build_df_schema(fields: &[QualifiedField]) -> Result<DFSchema> {
    build_df_schema_with_metadata(fields, HashMap::new())
}

pub fn build_df_schema_with_metadata(
    fields: &[QualifiedField],
    metadata: HashMap<String, String>,
) -> Result<DFSchema> {
    DFSchema::new_with_metadata(fields.iter().map(|f| f.clone().into()).collect(), metadata)
}
