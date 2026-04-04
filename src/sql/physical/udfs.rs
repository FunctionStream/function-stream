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
use std::sync::Arc;

use datafusion::arrow::array::StructArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::make_udf_function;
use crate::sql::common::constants::window_function_udf;
use crate::sql::schema::utils::window_arrow_struct;

// ============================================================================
// WindowFunctionUdf (User-Defined Scalar Function)
// ============================================================================

/// UDF that packs two nanosecond timestamps into the canonical window `Struct` type.
///
/// Stream SQL uses a single struct column `[start, end)` for tumbling/hopping windows;
/// this keeps `GROUP BY` and physical codec alignment on one Arrow shape.
#[derive(Debug)]
pub struct WindowFunctionUdf {
    signature: Signature,
}

impl Default for WindowFunctionUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for WindowFunctionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        window_function_udf::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(window_arrow_struct())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = args.args;

        if columns.len() != 2 {
            return exec_err!(
                "Window UDF expected exactly 2 arguments, but received {}",
                columns.len()
            );
        }

        let DataType::Struct(fields) = window_arrow_struct() else {
            return exec_err!(
                "Internal Engine Error: window_arrow_struct() must return a Struct DataType"
            );
        };

        let start_val = &columns[0];
        let end_val = &columns[1];

        if !matches!(
            start_val.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return exec_err!("Window UDF expected first argument to be a Nanosecond Timestamp");
        }
        if !matches!(
            end_val.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return exec_err!("Window UDF expected second argument to be a Nanosecond Timestamp");
        }

        match (start_val, end_val) {
            (ColumnarValue::Array(start_arr), ColumnarValue::Array(end_arr)) => {
                let struct_array =
                    StructArray::try_new(fields, vec![start_arr.clone(), end_arr.clone()], None)?;
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }

            (ColumnarValue::Array(start_arr), ColumnarValue::Scalar(end_scalar)) => {
                let end_arr = end_scalar.to_array_of_size(start_arr.len())?;
                let struct_array =
                    StructArray::try_new(fields, vec![start_arr.clone(), end_arr], None)?;
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }

            (ColumnarValue::Scalar(start_scalar), ColumnarValue::Array(end_arr)) => {
                let start_arr = start_scalar.to_array_of_size(end_arr.len())?;
                let struct_array =
                    StructArray::try_new(fields, vec![start_arr, end_arr.clone()], None)?;
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }

            (ColumnarValue::Scalar(start_scalar), ColumnarValue::Scalar(end_scalar)) => {
                let struct_array = StructArray::try_new(
                    fields,
                    vec![start_scalar.to_array()?, end_scalar.to_array()?],
                    None,
                )?;
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                    struct_array,
                ))))
            }
        }
    }
}

make_udf_function!(WindowFunctionUdf, WINDOW_FUNCTION, window);
