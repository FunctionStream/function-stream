use datafusion::arrow::array::builder::{ArrayBuilder, make_builder};
use datafusion::arrow::array::{RecordBatch, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaBuilder, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result as DFResult};
use std::sync::Arc;

use super::TIMESTAMP_FIELD;
use crate::sql::types::StreamSchema;

pub type FsSchemaRef = Arc<FsSchema>;

/// Core streaming schema with timestamp and key tracking.
/// Analogous to Arroyo's `ArroyoSchema`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FsSchema {
    pub schema: Arc<Schema>,
    pub timestamp_index: usize,
    key_indices: Option<Vec<usize>>,
    routing_key_indices: Option<Vec<usize>>,
}

impl FsSchema {
    pub fn new(
        schema: Arc<Schema>,
        timestamp_index: usize,
        key_indices: Option<Vec<usize>>,
        routing_key_indices: Option<Vec<usize>>,
    ) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices,
            routing_key_indices,
        }
    }

    pub fn new_unkeyed(schema: Arc<Schema>, timestamp_index: usize) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        }
    }

    pub fn new_keyed(schema: Arc<Schema>, timestamp_index: usize, key_indices: Vec<usize>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
            routing_key_indices: None,
        }
    }

    pub fn from_fields(mut fields: Vec<Field>) -> Self {
        if !fields.iter().any(|f| f.name() == TIMESTAMP_FIELD) {
            fields.push(Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
        }

        Self::from_schema_keys(Arc::new(Schema::new(fields)), vec![]).unwrap()
    }

    pub fn from_schema_unkeyed(schema: Arc<Schema>) -> DFResult<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema, schema is {schema:?}"
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        })
    }

    pub fn from_schema_keys(schema: Arc<Schema>, key_indices: Vec<usize>) -> DFResult<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema, schema is {schema:?}"
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
            routing_key_indices: None,
        })
    }

    pub fn schema_without_timestamp(&self) -> Schema {
        let mut builder = SchemaBuilder::from(self.schema.fields());
        builder.remove(self.timestamp_index);
        builder.finish()
    }

    pub fn remove_timestamp_column(&self, batch: &mut RecordBatch) {
        batch.remove_column(self.timestamp_index);
    }

    pub fn builders(&self) -> Vec<Box<dyn ArrayBuilder>> {
        self.schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 8))
            .collect()
    }

    pub fn timestamp_column<'a>(&self, batch: &'a RecordBatch) -> &'a TimestampNanosecondArray {
        batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
    }

    pub fn has_routing_keys(&self) -> bool {
        self.routing_keys().map(|k| !k.is_empty()).unwrap_or(false)
    }

    pub fn routing_keys(&self) -> Option<&Vec<usize>> {
        self.routing_key_indices
            .as_ref()
            .or(self.key_indices.as_ref())
    }

    pub fn storage_keys(&self) -> Option<&Vec<usize>> {
        self.key_indices.as_ref()
    }

    pub fn sort_field_indices(&self, with_timestamp: bool) -> Vec<usize> {
        let mut indices = vec![];
        if let Some(keys) = &self.key_indices {
            indices.extend(keys.iter().copied());
        }
        if with_timestamp {
            indices.push(self.timestamp_index);
        }
        indices
    }

    pub fn value_indices(&self, with_timestamp: bool) -> Vec<usize> {
        let field_count = self.schema.fields().len();
        match &self.key_indices {
            None => {
                let mut indices: Vec<usize> = (0..field_count).collect();
                if !with_timestamp {
                    indices.remove(self.timestamp_index);
                }
                indices
            }
            Some(keys) => (0..field_count)
                .filter(|index| {
                    !keys.contains(index) && (with_timestamp || *index != self.timestamp_index)
                })
                .collect(),
        }
    }

    pub fn unkeyed_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        if self.key_indices.is_none() {
            return Ok(batch.clone());
        }
        let columns: Vec<_> = (0..batch.num_columns())
            .filter(|index| !self.key_indices.as_ref().unwrap().contains(index))
            .collect();
        batch.project(&columns)
    }

    pub fn schema_without_keys(&self) -> Result<Self, ArrowError> {
        if self.key_indices.is_none() {
            return Ok(self.clone());
        }
        let key_indices = self.key_indices.as_ref().unwrap();
        let unkeyed_schema = Schema::new(
            self.schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(index, _)| !key_indices.contains(index))
                .map(|(_, field)| field.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        let timestamp_index = unkeyed_schema.index_of(TIMESTAMP_FIELD)?;
        Ok(Self {
            schema: Arc::new(unkeyed_schema),
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        })
    }

    pub fn with_fields(&self, fields: Vec<FieldRef>) -> Result<Self, ArrowError> {
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata.clone(),
        ));

        let timestamp_index = schema.index_of(TIMESTAMP_FIELD)?;
        let max_index = *[&self.key_indices, &self.routing_key_indices]
            .iter()
            .map(|indices| indices.as_ref().and_then(|k| k.iter().max()))
            .max()
            .flatten()
            .unwrap_or(&0);

        if schema.fields.len() - 1 < max_index {
            return Err(ArrowError::InvalidArgumentError(format!(
                "expected at least {} fields, but were only {}",
                max_index + 1,
                schema.fields.len()
            )));
        }

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: self.key_indices.clone(),
            routing_key_indices: self.routing_key_indices.clone(),
        })
    }

    pub fn with_additional_fields(
        &self,
        new_fields: impl Iterator<Item = Field>,
    ) -> Result<Self, ArrowError> {
        let mut fields = self.schema.fields.to_vec();
        fields.extend(new_fields.map(Arc::new));
        self.with_fields(fields)
    }
}

/// Proto serialization: convert between FsSchema and the proto `FsSchema` message.
///
/// Schema is encoded as JSON using Arrow's `SchemaRef` JSON representation.
/// This approach avoids depending on serde for `arrow_schema::Schema` directly.
impl FsSchema {
    pub fn to_proto(&self) -> protocol::grpc::api::FsSchema {
        let arrow_schema = schema_to_json_string(&self.schema);
        let timestamp_index = self.timestamp_index as u32;

        let has_keys = self.key_indices.is_some();
        let key_indices = self
            .key_indices
            .as_ref()
            .map(|ks| ks.iter().map(|i| *i as u32).collect())
            .unwrap_or_default();

        let has_routing_keys = self.routing_key_indices.is_some();
        let routing_key_indices = self
            .routing_key_indices
            .as_ref()
            .map(|ks| ks.iter().map(|i| *i as u32).collect())
            .unwrap_or_default();

        protocol::grpc::api::FsSchema {
            arrow_schema,
            timestamp_index,
            key_indices,
            has_keys,
            routing_key_indices,
            has_routing_keys,
        }
    }

    pub fn from_proto(proto: protocol::grpc::api::FsSchema) -> Result<Self, DataFusionError> {
        let schema = schema_from_json_string(&proto.arrow_schema)?;
        let timestamp_index = proto.timestamp_index as usize;

        let key_indices = proto
            .has_keys
            .then(|| proto.key_indices.into_iter().map(|i| i as usize).collect());

        let routing_key_indices = proto.has_routing_keys.then(|| {
            proto
                .routing_key_indices
                .into_iter()
                .map(|i| i as usize)
                .collect()
        });

        Ok(Self {
            schema: Arc::new(schema),
            timestamp_index,
            key_indices,
            routing_key_indices,
        })
    }
}

fn schema_to_json_string(schema: &Schema) -> String {
    let json_fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    serde_json::to_string(&json_fields).unwrap()
}

fn schema_from_json_string(s: &str) -> Result<Schema, DataFusionError> {
    let json_fields: Vec<serde_json::Value> = serde_json::from_str(s)
        .map_err(|e| DataFusionError::Plan(format!("Invalid schema JSON: {e}")))?;

    let fields: Vec<Field> = json_fields
        .into_iter()
        .map(|v| {
            let name = v["name"]
                .as_str()
                .ok_or_else(|| DataFusionError::Plan("missing field name".into()))?
                .to_string();
            let nullable = v["nullable"].as_bool().unwrap_or(true);
            let dt_str = v["data_type"]
                .as_str()
                .ok_or_else(|| DataFusionError::Plan("missing data_type".into()))?;
            let data_type = parse_debug_data_type(dt_str)?;
            Ok(Field::new(name, data_type, nullable))
        })
        .collect::<Result<_, DataFusionError>>()?;

    Ok(Schema::new(fields))
}

fn parse_debug_data_type(s: &str) -> Result<DataType, DataFusionError> {
    match s {
        "Boolean" => Ok(DataType::Boolean),
        "Int8" => Ok(DataType::Int8),
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "UInt8" => Ok(DataType::UInt8),
        "UInt16" => Ok(DataType::UInt16),
        "UInt32" => Ok(DataType::UInt32),
        "UInt64" => Ok(DataType::UInt64),
        "Float16" => Ok(DataType::Float16),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        "Utf8" => Ok(DataType::Utf8),
        "LargeUtf8" => Ok(DataType::LargeUtf8),
        "Binary" => Ok(DataType::Binary),
        "LargeBinary" => Ok(DataType::LargeBinary),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        "Null" => Ok(DataType::Null),
        s if s.starts_with("Timestamp(Nanosecond") => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        s if s.starts_with("Timestamp(Microsecond") => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        s if s.starts_with("Timestamp(Millisecond") => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        s if s.starts_with("Timestamp(Second") => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported data type in schema JSON: {s}"
        ))),
    }
}

impl From<StreamSchema> for FsSchema {
    fn from(s: StreamSchema) -> Self {
        FsSchema {
            schema: s.schema,
            timestamp_index: s.timestamp_index,
            key_indices: s.key_indices,
            routing_key_indices: None,
        }
    }
}

impl From<StreamSchema> for Arc<FsSchema> {
    fn from(s: StreamSchema) -> Self {
        Arc::new(FsSchema::from(s))
    }
}
