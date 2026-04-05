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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

// ============================================================================
// PlanningPlaceholderUdf
// ============================================================================

/// Logical-planning-only UDF: satisfies type checking until real functions are wired in.
pub(crate) struct PlanningPlaceholderUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl Debug for PlanningPlaceholderUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlanningPlaceholderUDF<{}>", self.name)
    }
}

impl ScalarUDFImpl for PlanningPlaceholderUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "PlanningPlaceholderUDF '{}' was invoked during physical execution. \
             This indicates a bug in the stream query compiler: placeholders must be \
             swapped with actual physical UDFs before execution begins.",
            self.name
        )
    }
}

impl PlanningPlaceholderUdf {
    pub fn new_with_return(
        name: impl Into<String>,
        args: Vec<DataType>,
        return_type: DataType,
    ) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::new_from_impl(Self {
            name: name.into(),
            signature: Signature::exact(args, Volatility::Volatile),
            return_type,
        }))
    }
}
