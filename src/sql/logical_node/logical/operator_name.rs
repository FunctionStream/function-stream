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

use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::{Display, EnumString};

use crate::sql::common::constants::operator_feature;

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumString, Display)]
pub enum OperatorName {
    ExpressionWatermark,
    ArrowValue,
    ArrowKey,
    Projection,
    AsyncUdf,
    Join,
    InstantJoin,
    LookupJoin,
    WindowFunction,
    TumblingWindowAggregate,
    SlidingWindowAggregate,
    SessionWindowAggregate,
    UpdatingAggregate,
    KeyBy,
    ConnectorSource,
    ConnectorSink,
}

impl OperatorName {
    pub fn feature_tag(self) -> Option<&'static str> {
        match self {
            Self::ExpressionWatermark | Self::ArrowValue | Self::ArrowKey | Self::Projection => None,
            Self::AsyncUdf => Some(operator_feature::ASYNC_UDF),
            Self::Join => Some(operator_feature::JOIN_WITH_EXPIRATION),
            Self::InstantJoin => Some(operator_feature::WINDOWED_JOIN),
            Self::WindowFunction => Some(operator_feature::SQL_WINDOW_FUNCTION),
            Self::LookupJoin => Some(operator_feature::LOOKUP_JOIN),
            Self::TumblingWindowAggregate => Some(operator_feature::SQL_TUMBLING_WINDOW_AGGREGATE),
            Self::SlidingWindowAggregate => Some(operator_feature::SQL_SLIDING_WINDOW_AGGREGATE),
            Self::SessionWindowAggregate => Some(operator_feature::SQL_SESSION_WINDOW_AGGREGATE),
            Self::UpdatingAggregate => Some(operator_feature::SQL_UPDATING_AGGREGATE),
            Self::KeyBy => Some(operator_feature::KEY_BY_ROUTING),
            Self::ConnectorSource => Some(operator_feature::CONNECTOR_SOURCE),
            Self::ConnectorSink => Some(operator_feature::CONNECTOR_SINK),
        }
    }
}

impl Serialize for OperatorName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OperatorName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}
