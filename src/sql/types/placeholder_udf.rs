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
