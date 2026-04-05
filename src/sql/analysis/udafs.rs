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

use datafusion::arrow::array::ArrayRef;
use datafusion::error::Result;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use std::fmt::Debug;

/// Fake UDAF used just for plan-time placeholder.
#[derive(Debug)]
pub struct EmptyUdaf {}

impl Accumulator for EmptyUdaf {
    fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        unreachable!()
    }

    fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }
}
