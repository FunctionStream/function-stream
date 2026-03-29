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

mod macros;

pub(crate) mod streaming_operator_blueprint;
pub(crate) use streaming_operator_blueprint::{CompiledTopologyNode, StreamingOperatorBlueprint};

pub(crate) mod aggregate;
pub(crate) mod debezium;
pub(crate) mod join;
pub(crate) mod key_calculation;
pub(crate) mod lookup;
pub(crate) mod projection;
pub(crate) mod remote_table;
pub(crate) mod sink;
pub(crate) mod table_source;
pub(crate) mod updating_aggregate;
pub(crate) mod watermark_node;
pub(crate) mod windows_function;

pub(crate) mod timestamp_append;
pub(crate) use timestamp_append::SystemTimestampInjectorNode;

pub(crate) mod async_udf;
pub(crate) use async_udf::AsyncFunctionExecutionNode;

pub(crate) mod is_retract;
pub(crate) use is_retract::IsRetractExtension;

mod extension_try_from;
