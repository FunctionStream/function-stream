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
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[allow(clippy::type_complexity)]
pub(crate) struct PlaceholderUdf {
    name: String,
    signature: Signature,
    return_type: Arc<dyn Fn(&[DataType]) -> Result<DataType> + Send + Sync + 'static>,
}

impl Debug for PlaceholderUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlaceholderUDF<{}>", self.name)
    }
}

impl ScalarUDFImpl for PlaceholderUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        (self.return_type)(args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unimplemented!("PlaceholderUdf should never be called at execution time");
    }
}

impl PlaceholderUdf {
    pub fn with_return(
        name: impl Into<String>,
        args: Vec<DataType>,
        ret: DataType,
    ) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::new_from_impl(PlaceholderUdf {
            name: name.into(),
            signature: Signature::exact(args, Volatility::Volatile),
            return_type: Arc::new(move |_| Ok(ret.clone())),
        }))
    }
}
