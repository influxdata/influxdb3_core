use std::sync::Arc;

use crate::error;
use crate::window::difference::window_difference;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::DataType;
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::TimeUnit::Nanosecond;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};

#[derive(Debug)]
pub(super) struct ElapsedUDWF {
    signature: Signature,
}

impl ElapsedUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for ElapsedUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "elapsed"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ElapsedPartitionEvaluator {}))
    }
}

#[derive(Debug)]
struct ElapsedPartitionEvaluator {}

impl PartitionEvaluator for ElapsedPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 3);
        assert!(matches!(
            values[2].data_type(),
            &DataType::Timestamp(Nanosecond, _)
        ));

        if values[1].data_type() != &DataType::Interval(MonthDayNano) {
            return Err(error::map::query("expected interval duration type"));
        }

        let unit = ScalarValue::try_from_array(&values[1], 0)?;
        let unit = if let ScalarValue::IntervalMonthDayNano(Some(unit)) = unit {
            unit as i64
        } else {
            1 // default unit is nanoseconds
        };

        let times = Arc::clone(&values[2]);
        let results = window_difference(times)?;

        let pr = results
            .as_primitive_opt::<arrow::datatypes::DurationNanosecondType>()
            .ok_or(error::map::internal(
                "failed to convert elapsed to requested unit",
            ))?;

        Ok(Arc::new(PrimitiveArray::from_iter(
            pr.into_iter()
                .map(|x| x.unwrap_or(0).checked_div(unit).unwrap_or(0)),
        )))
    }
}
