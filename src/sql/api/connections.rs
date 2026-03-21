use crate::sql::common::formats::{BadData, Format, Framing};
use crate::sql::common::{FsExtensionType, FsSchema};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Connector {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub description: String,
    pub table_config: String,
    pub enabled: bool,
    pub source: bool,
    pub sink: bool,
    pub custom_schemas: bool,
    pub testing: bool,
    pub hidden: bool,
    pub connection_config: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionProfilePost {
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    Source,
    Sink,
    Lookup,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "SOURCE"),
            ConnectionType::Sink => write!(f, "SINK"),
            ConnectionType::Lookup => write!(f, "LOOKUP"),
        }
    }
}

impl TryFrom<String> for ConnectionType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "source" => Ok(ConnectionType::Source),
            "sink" => Ok(ConnectionType::Sink),
            "lookup" => Ok(ConnectionType::Lookup),
            _ => Err(format!("Invalid connection type: {value}")),
        }
    }
}

// ─────────────────── Field Types ───────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldType {
    Int32,
    Int64,
    Uint32,
    Uint64,
    #[serde(alias = "f32")]
    Float32,
    #[serde(alias = "f64")]
    Float64,
    Decimal128(DecimalField),
    Bool,
    #[serde(alias = "utf8")]
    String,
    #[serde(alias = "binary")]
    Bytes,
    Timestamp(TimestampField),
    Json,
    Struct(StructField),
    List(ListField),
}

impl FieldType {
    pub fn sql_type(&self) -> String {
        match self {
            FieldType::Int32 => "INTEGER".into(),
            FieldType::Int64 => "BIGINT".into(),
            FieldType::Uint32 => "INTEGER UNSIGNED".into(),
            FieldType::Uint64 => "BIGINT UNSIGNED".into(),
            FieldType::Float32 => "FLOAT".into(),
            FieldType::Float64 => "DOUBLE".into(),
            FieldType::Decimal128(f) => format!("DECIMAL({}, {})", f.precision, f.scale),
            FieldType::Bool => "BOOLEAN".into(),
            FieldType::String => "TEXT".into(),
            FieldType::Bytes => "BINARY".into(),
            FieldType::Timestamp(t) => format!("TIMESTAMP({})", t.unit.precision()),
            FieldType::Json => "JSON".into(),
            FieldType::List(item) => format!("{}[]", item.items.field_type.sql_type()),
            FieldType::Struct(StructField { fields, .. }) => {
                format!(
                    "STRUCT <{}>",
                    fields
                        .iter()
                        .map(|f| format!("{} {}", f.name, f.field_type.sql_type()))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    #[serde(alias = "s")]
    Second,
    #[default]
    #[serde(alias = "ms")]
    Millisecond,
    #[serde(alias = "µs", alias = "us")]
    Microsecond,
    #[serde(alias = "ns")]
    Nanosecond,
}

impl TimestampUnit {
    pub fn precision(&self) -> u8 {
        match self {
            TimestampUnit::Second => 0,
            TimestampUnit::Millisecond => 3,
            TimestampUnit::Microsecond => 6,
            TimestampUnit::Nanosecond => 9,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TimestampField {
    #[serde(default)]
    pub unit: TimestampUnit,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DecimalField {
    pub precision: u8,
    pub scale: i8,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StructField {
    pub fields: Vec<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ListField {
    pub items: Box<ListFieldItem>,
}

fn default_item_name() -> String {
    "item".to_string()
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ListFieldItem {
    #[serde(default = "default_item_name")]
    pub name: String,
    #[serde(flatten)]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub sql_name: Option<String>,
}

impl From<ListFieldItem> for Field {
    fn from(value: ListFieldItem) -> Self {
        SourceField {
            name: value.name,
            field_type: value.field_type,
            required: value.required,
            sql_name: None,
            metadata_key: None,
        }
        .into()
    }
}

impl Serialize for ListFieldItem {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut f = Serializer::serialize_map(s, None)?;
        f.serialize_entry("name", &self.name)?;
        serialize_field_type_flat(&self.field_type, &mut f)?;
        f.serialize_entry("required", &self.required)?;
        f.serialize_entry("sql_name", &self.field_type.sql_type())?;
        f.end()
    }
}

impl TryFrom<Field> for ListFieldItem {
    type Error = String;

    fn try_from(value: Field) -> Result<Self, Self::Error> {
        let source_field: SourceField = value.try_into()?;
        Ok(Self {
            name: source_field.name,
            field_type: source_field.field_type,
            required: source_field.required,
            sql_name: None,
        })
    }
}

fn serialize_field_type_flat<M: SerializeMap>(ft: &FieldType, map: &mut M) -> Result<(), M::Error> {
    let type_tag = match ft {
        FieldType::Int32 => "int32",
        FieldType::Int64 => "int64",
        FieldType::Uint32 => "uint32",
        FieldType::Uint64 => "uint64",
        FieldType::Float32 => "float32",
        FieldType::Float64 => "float64",
        FieldType::Decimal128(_) => "decimal128",
        FieldType::Bool => "bool",
        FieldType::String => "string",
        FieldType::Bytes => "bytes",
        FieldType::Timestamp(_) => "timestamp",
        FieldType::Json => "json",
        FieldType::Struct(_) => "struct",
        FieldType::List(_) => "list",
    };
    map.serialize_entry("type", type_tag)?;

    match ft {
        FieldType::Decimal128(d) => {
            map.serialize_entry("precision", &d.precision)?;
            map.serialize_entry("scale", &d.scale)?;
        }
        FieldType::Timestamp(t) => {
            map.serialize_entry("unit", &t.unit)?;
        }
        FieldType::Struct(s) => {
            map.serialize_entry("fields", &s.fields)?;
        }
        FieldType::List(l) => {
            map.serialize_entry("items", &l.items)?;
        }
        _ => {}
    }
    Ok(())
}

// ─────────────────── Source Field ───────────────────

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SourceField {
    pub name: String,
    #[serde(flatten)]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub sql_name: Option<String>,
    #[serde(default)]
    pub metadata_key: Option<String>,
}

impl Serialize for SourceField {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut f = Serializer::serialize_map(s, None)?;
        f.serialize_entry("name", &self.name)?;
        serialize_field_type_flat(&self.field_type, &mut f)?;
        f.serialize_entry("required", &self.required)?;
        if let Some(metadata_key) = &self.metadata_key {
            f.serialize_entry("metadata_key", metadata_key)?;
        }
        f.serialize_entry("sql_name", &self.field_type.sql_type())?;
        f.end()
    }
}

impl From<SourceField> for Field {
    fn from(f: SourceField) -> Self {
        let (t, ext) = match f.field_type {
            FieldType::Int32 => (DataType::Int32, None),
            FieldType::Int64 => (DataType::Int64, None),
            FieldType::Uint32 => (DataType::UInt32, None),
            FieldType::Uint64 => (DataType::UInt64, None),
            FieldType::Float32 => (DataType::Float32, None),
            FieldType::Float64 => (DataType::Float64, None),
            FieldType::Bool => (DataType::Boolean, None),
            FieldType::String => (DataType::Utf8, None),
            FieldType::Bytes => (DataType::Binary, None),
            FieldType::Decimal128(d) => (DataType::Decimal128(d.precision, d.scale), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Second,
            }) => (DataType::Timestamp(TimeUnit::Second, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Millisecond,
            }) => (DataType::Timestamp(TimeUnit::Millisecond, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Microsecond,
            }) => (DataType::Timestamp(TimeUnit::Microsecond, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Nanosecond,
            }) => (DataType::Timestamp(TimeUnit::Nanosecond, None), None),
            FieldType::Json => (DataType::Utf8, Some(FsExtensionType::JSON)),
            FieldType::Struct(s) => (
                DataType::Struct(Fields::from(
                    s.fields
                        .into_iter()
                        .map(|t| t.into())
                        .collect::<Vec<Field>>(),
                )),
                None,
            ),
            FieldType::List(t) => (DataType::List(Arc::new((*t.items).into())), None),
        };

        FsExtensionType::add_metadata(ext, Field::new(f.name, t, !f.required))
    }
}

impl TryFrom<Field> for SourceField {
    type Error = String;

    fn try_from(f: Field) -> Result<Self, Self::Error> {
        let field_type = match (f.data_type(), FsExtensionType::from_map(f.metadata())) {
            (DataType::Boolean, None) => FieldType::Bool,
            (DataType::Int32, None) => FieldType::Int32,
            (DataType::Int64, None) => FieldType::Int64,
            (DataType::UInt32, None) => FieldType::Uint32,
            (DataType::UInt64, None) => FieldType::Uint64,
            (DataType::Float32, None) => FieldType::Float32,
            (DataType::Float64, None) => FieldType::Float64,
            (DataType::Decimal128(p, s), None) => FieldType::Decimal128(DecimalField {
                precision: *p,
                scale: *s,
            }),
            (DataType::Binary, None) | (DataType::LargeBinary, None) => FieldType::Bytes,
            (DataType::Timestamp(TimeUnit::Second, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Second,
                })
            }
            (DataType::Timestamp(TimeUnit::Millisecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Millisecond,
                })
            }
            (DataType::Timestamp(TimeUnit::Microsecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Microsecond,
                })
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Nanosecond,
                })
            }
            (DataType::Utf8, None) => FieldType::String,
            (DataType::Utf8, Some(FsExtensionType::JSON)) => FieldType::Json,
            (DataType::Struct(fields), None) => {
                let fields: Result<_, String> = fields
                    .into_iter()
                    .map(|f| (**f).clone().try_into())
                    .collect();
                FieldType::Struct(StructField { fields: fields? })
            }
            (DataType::List(item), None) => FieldType::List(ListField {
                items: Box::new((**item).clone().try_into()?),
            }),
            dt => return Err(format!("Unsupported data type {dt:?}")),
        };

        Ok(SourceField {
            name: f.name().clone(),
            field_type,
            required: !f.is_nullable(),
            sql_name: None,
            metadata_key: None,
        })
    }
}

// ─────────────────── Schema Definitions ───────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SchemaDefinition {
    JsonSchema {
        schema: String,
    },
    ProtobufSchema {
        schema: String,
        #[serde(default)]
        dependencies: HashMap<String, String>,
    },
    AvroSchema {
        schema: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    #[serde(default)]
    pub bad_data: Option<BadData>,
    #[serde(default)]
    pub framing: Option<Framing>,
    #[serde(default)]
    pub fields: Vec<SourceField>,
    #[serde(default)]
    pub definition: Option<SchemaDefinition>,
    #[serde(default)]
    pub inferred: Option<bool>,
    #[serde(default)]
    pub primary_keys: HashSet<String>,
}

impl ConnectionSchema {
    pub fn try_new(
        format: Option<Format>,
        bad_data: Option<BadData>,
        framing: Option<Framing>,
        fields: Vec<SourceField>,
        definition: Option<SchemaDefinition>,
        inferred: Option<bool>,
        primary_keys: HashSet<String>,
    ) -> anyhow::Result<Self> {
        let s = ConnectionSchema {
            format,
            bad_data,
            framing,
            fields,
            definition,
            inferred,
            primary_keys,
        };
        s.validate()
    }

    pub fn validate(self) -> anyhow::Result<Self> {
        let non_metadata_fields: Vec<_> = self
            .fields
            .iter()
            .filter(|f| f.metadata_key.is_none())
            .collect();

        if let Some(Format::RawString(_)) = &self.format {
            if non_metadata_fields.len() != 1
                || non_metadata_fields.first().unwrap().field_type != FieldType::String
                || non_metadata_fields.first().unwrap().name != "value"
            {
                anyhow::bail!(
                    "raw_string format requires a schema with a single field called `value` of type TEXT"
                );
            }
        }

        if let Some(Format::Json(json_format)) = &self.format {
            if json_format.unstructured
                && (non_metadata_fields.len() != 1
                    || non_metadata_fields.first().unwrap().field_type != FieldType::Json
                    || non_metadata_fields.first().unwrap().name != "value")
            {
                anyhow::bail!(
                    "json format with unstructured flag enabled requires a schema with a single field called `value` of type JSON"
                );
            }
        }

        Ok(self)
    }

    pub fn fs_schema(&self) -> Arc<FsSchema> {
        let fields: Vec<Field> = self.fields.iter().map(|f| f.clone().into()).collect();
        Arc::new(FsSchema::from_fields(fields))
    }
}

impl From<ConnectionSchema> for FsSchema {
    fn from(val: ConnectionSchema) -> Self {
        let fields: Vec<Field> = val.fields.into_iter().map(|f| f.into()).collect();
        FsSchema::from_fields(fields)
    }
}

// ─────────────────── Connection Table ───────────────────

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionTable {
    #[serde(skip_serializing)]
    pub id: i64,
    #[serde(rename = "id")]
    pub pub_id: String,
    pub name: String,
    pub created_at: u64,
    pub connector: String,
    pub connection_profile: Option<ConnectionProfile>,
    pub table_type: ConnectionType,
    pub config: serde_json::Value,
    pub schema: ConnectionSchema,
    pub consumers: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionTablePost {
    pub name: String,
    pub connector: String,
    pub connection_profile_id: Option<String>,
    pub config: serde_json::Value,
    pub schema: Option<ConnectionSchema>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionAutocompleteResp {
    pub values: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct TestSourceMessage {
    pub error: bool,
    pub done: bool,
    pub message: String,
}

impl TestSourceMessage {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            error: false,
            done: false,
            message: message.into(),
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: true,
            done: false,
            message: message.into(),
        }
    }
    pub fn done(message: impl Into<String>) -> Self {
        Self {
            error: false,
            done: true,
            message: message.into(),
        }
    }
    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            error: true,
            done: true,
            message: message.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConfluentSchema {
    pub schema: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ConfluentSchemaQueryParams {
    pub endpoint: String,
    pub topic: String,
}
