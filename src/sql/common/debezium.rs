use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;

pub trait Key:
    Debug + Clone + Encode + Decode<()> + std::hash::Hash + PartialEq + Eq + Send + 'static
{
}
impl<T: Debug + Clone + Encode + Decode<()> + std::hash::Hash + PartialEq + Eq + Send + 'static> Key
    for T
{
}

pub trait Data: Debug + Clone + Encode + Decode<()> + Send + 'static {}
impl<T: Debug + Clone + Encode + Decode<()> + Send + 'static> Data for T {}

#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum UpdatingData<T: Data> {
    Retract(T),
    Update { old: T, new: T },
    Append(T),
}

impl<T: Data> UpdatingData<T> {
    pub fn lower(&self) -> T {
        match self {
            UpdatingData::Retract(_) => panic!("cannot lower retractions"),
            UpdatingData::Update { new, .. } => new.clone(),
            UpdatingData::Append(t) => t.clone(),
        }
    }

    pub fn unwrap_append(&self) -> &T {
        match self {
            UpdatingData::Append(t) => t,
            _ => panic!("UpdatingData is not an append"),
        }
    }
}

#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq)]
#[serde(try_from = "DebeziumShadow<T>")]
pub struct Debezium<T: Data> {
    pub before: Option<T>,
    pub after: Option<T>,
    pub op: DebeziumOp,
}

#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq)]
struct DebeziumShadow<T: Data> {
    before: Option<T>,
    after: Option<T>,
    op: DebeziumOp,
}

impl<T: Data> TryFrom<DebeziumShadow<T>> for Debezium<T> {
    type Error = &'static str;

    fn try_from(value: DebeziumShadow<T>) -> Result<Self, Self::Error> {
        match (value.op, &value.before, &value.after) {
            (DebeziumOp::Create, _, None) => {
                Err("`after` must be set for Debezium create messages")
            }
            (DebeziumOp::Update, None, _) => {
                Err("`before` must be set for Debezium update messages")
            }
            (DebeziumOp::Update, _, None) => {
                Err("`after` must be set for Debezium update messages")
            }
            (DebeziumOp::Delete, None, _) => {
                Err("`before` must be set for Debezium delete messages")
            }
            _ => Ok(Debezium {
                before: value.before,
                after: value.after,
                op: value.op,
            }),
        }
    }
}

#[derive(Copy, Clone, Encode, Decode, Debug, PartialEq)]
pub enum DebeziumOp {
    Create,
    Update,
    Delete,
}

#[allow(clippy::to_string_trait_impl)]
impl ToString for DebeziumOp {
    fn to_string(&self) -> String {
        match self {
            DebeziumOp::Create => "c",
            DebeziumOp::Update => "u",
            DebeziumOp::Delete => "d",
        }
        .to_string()
    }
}

impl<'de> Deserialize<'de> for DebeziumOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "c" | "r" => Ok(DebeziumOp::Create),
            "u" => Ok(DebeziumOp::Update),
            "d" => Ok(DebeziumOp::Delete),
            _ => Err(serde::de::Error::custom(format!("Invalid DebeziumOp {s}"))),
        }
    }
}

impl Serialize for DebeziumOp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DebeziumOp::Create => serializer.serialize_str("c"),
            DebeziumOp::Update => serializer.serialize_str("u"),
            DebeziumOp::Delete => serializer.serialize_str("d"),
        }
    }
}

#[derive(Copy, Clone, Encode, Decode, Debug, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
