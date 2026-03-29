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

//! 算子构造协议：与具体连接器实现解耦，供 [`super::OperatorFactory`] 与 `connector` 共用。

use anyhow::Result;
use std::sync::Arc;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::global::Registry;

/// 算子构造器 trait：每个实现者负责从 protobuf 字节流反序列化配置并构造 [`ConstructedOperator`]。
///
/// 外部插件可实现此 trait 并通过 [`crate::runtime::streaming::factory::OperatorFactory::register`] 注入。
pub trait OperatorConstructor: Send + Sync {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator>;
}
