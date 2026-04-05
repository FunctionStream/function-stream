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

use datafusion::arrow::datatypes::DataType;
use datafusion_proto::protobuf::ArrowType;
use prost::Message;
use protocol::function_stream_graph;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct DylibUdfConfig {
    pub dylib_path: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub aggregate: bool,
    pub is_async: bool,
}

impl From<DylibUdfConfig> for function_stream_graph::DylibUdfConfig {
    fn from(from: DylibUdfConfig) -> Self {
        function_stream_graph::DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    ArrowType::try_from(t)
                        .expect("unsupported data type")
                        .encode_to_vec()
                })
                .collect(),
            return_type: ArrowType::try_from(&from.return_type)
                .expect("unsupported data type")
                .encode_to_vec(),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}

impl From<function_stream_graph::DylibUdfConfig> for DylibUdfConfig {
    fn from(from: function_stream_graph::DylibUdfConfig) -> Self {
        DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    DataType::try_from(
                        &ArrowType::decode(&mut t.as_slice()).expect("invalid arrow type"),
                    )
                    .expect("invalid arrow type")
                })
                .collect(),
            return_type: DataType::try_from(
                &ArrowType::decode(&mut from.return_type.as_slice()).unwrap(),
            )
            .expect("invalid arrow type"),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}
