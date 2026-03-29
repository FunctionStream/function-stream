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
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::make_udf_function;
use crate::sql::common::constants::{window_function_udf, window_interval_field};
use crate::sql::schema::utils::window_arrow_struct;

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

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(window_arrow_struct())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = args.args;
        if columns.len() != 2 {
            return plan_err!(
                "window function expected 2 arguments, got {}",
                columns.len()
            );
        }
        if columns[0].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected first argument to be a timestamp, got {:?}",
                columns[0].data_type()
            );
        }
        if columns[1].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected second argument to be a timestamp, got {:?}",
                columns[1].data_type()
            );
        }
        let fields = vec![
            Arc::new(Field::new(
                window_interval_field::START,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(Field::new(
                window_interval_field::END,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
        ]
        .into();

        match (&columns[0], &columns[1]) {
            (ColumnarValue::Array(start), ColumnarValue::Array(end)) => {
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start.clone(), end.clone()],
                    None,
                ))))
            }
            (ColumnarValue::Array(start), ColumnarValue::Scalar(end)) => {
                let end = end.to_array_of_size(start.len())?;
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start.clone(), end],
                    None,
                ))))
            }
            (ColumnarValue::Scalar(start), ColumnarValue::Array(end)) => {
                let start = start.to_array_of_size(end.len())?;
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start, end.clone()],
                    None,
                ))))
            }
            (ColumnarValue::Scalar(start), ColumnarValue::Scalar(end)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(
                    StructArray::new(fields, vec![start.to_array()?, end.to_array()?], None).into(),
                )))
            }
        }
    }
}

make_udf_function!(WindowFunctionUdf, WINDOW_FUNCTION, window);
