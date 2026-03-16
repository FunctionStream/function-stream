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
