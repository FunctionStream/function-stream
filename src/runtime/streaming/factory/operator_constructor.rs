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

use anyhow::Result;
use std::sync::Arc;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::global::Registry;

/// Builds a [`ConstructedOperator`] from serialized configuration and a [`Registry`].
pub trait OperatorConstructor: Send + Sync {
    fn with_config(&self, config: &[u8], registry: Arc<Registry>) -> Result<ConstructedOperator>;
}
