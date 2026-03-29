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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DFField {
    qualifier: Option<TableReference>,
    field: FieldRef,
}

impl From<(Option<TableReference>, FieldRef)> for DFField {
    fn from(value: (Option<TableReference>, FieldRef)) -> Self {
        Self {
            qualifier: value.0,
            field: value.1,
        }
    }
}

impl From<(Option<&TableReference>, &Field)> for DFField {
    fn from(value: (Option<&TableReference>, &Field)) -> Self {
        Self {
            qualifier: value.0.cloned(),
            field: Arc::new(value.1.clone()),
        }
    }
}

impl From<DFField> for (Option<TableReference>, FieldRef) {
    fn from(value: DFField) -> Self {
        (value.qualifier, value.field)
    }
}

impl DFField {
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
        DFField {
            qualifier: None,
            field: Arc::new(Field::new(name, data_type, nullable)),
        }
    }

    pub fn name(&self) -> &String {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        self.field.metadata()
    }

    pub fn qualified_name(&self) -> String {
        if let Some(qualifier) = &self.qualifier {
            format!("{}.{}", qualifier, self.field.name())
        } else {
            self.field.name().to_owned()
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

    pub fn qualifier(&self) -> Option<&TableReference> {
        self.qualifier.as_ref()
    }

    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    pub fn strip_qualifier(mut self) -> Self {
        self.qualifier = None;
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        let f = self.field().as_ref().clone().with_nullable(nullable);
        self.field = f.into();
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        let f = self.field().as_ref().clone().with_metadata(metadata);
        self.field = f.into();
        self
    }
}

pub fn fields_with_qualifiers(schema: &DFSchema) -> Vec<DFField> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (schema.qualified_field(i).0.cloned(), f.clone()).into())
        .collect()
}

pub fn schema_from_df_fields(fields: &[DFField]) -> Result<DFSchema> {
    schema_from_df_fields_with_metadata(fields, HashMap::new())
}

pub fn schema_from_df_fields_with_metadata(
    fields: &[DFField],
    metadata: HashMap<String, String>,
) -> Result<DFSchema> {
    DFSchema::new_with_metadata(fields.iter().map(|t| t.clone().into()).collect(), metadata)
}
