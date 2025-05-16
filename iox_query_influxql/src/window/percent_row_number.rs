use crate::error;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, downcast_value};
use datafusion::logical_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct PercentRowNumberUDWF {
    signature: Signature,
}

impl PercentRowNumberUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for PercentRowNumberUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "percent_row_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::UInt64, true))
    }

    /// Include this as a workaround for <https://github.com/apache/datafusion/issues/13168>
    fn expressions(&self, expr_args: ExpressionArgs<'_>) -> Vec<Arc<dyn PhysicalExpr>> {
        expr_args.input_exprs().into()
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PercentRowNumberPartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns the row number at which the nth
/// percentile of the data will occur.
///
/// This evaluator calculates the row_number accross the entire partition,
/// any data that should not be included must be filtered out before
/// evaluating the window function.
#[derive(Debug)]
struct PercentRowNumberPartitionEvaluator {}

impl PartitionEvaluator for PercentRowNumberPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 1);

        let array = Arc::clone(&values[0]);
        let mut builder = UInt64Array::builder(array.len());
        match array.data_type() {
            DataType::Int64 => builder.extend(downcast_value!(array, Int64Array).iter().map(|o| {
                o.and_then(|v| percentile_idx(num_rows, v as f64).map(|v| v as u64))
                    .or(Some(0))
            })),
            DataType::Float64 => {
                builder.extend(downcast_value!(array, Float64Array).iter().map(|o| {
                    o.and_then(|v| percentile_idx(num_rows, v).map(|v| v as u64))
                        .or(Some(0))
                }))
            }
            dt => {
                return error::internal(format!(
                    "invalid data type ({dt}) for PERCENTILE n argument"
                ));
            }
        };
        Ok(Arc::new(builder.finish()))
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

/// Calculate the location in an ordered list of len items where the
/// location of the item at the given percentile would be found.
///
/// Note that row numbers are 1-based so this returns values in the
/// range \[1,len\].
///
/// This uses the same algorithm as the original influxdb implementation
/// of percentile as can be found in
/// <https://github.com/influxdata/influxdb/blob/75a8bcfae2af7b0043933be9f96b98c0741ceee3/influxql/query/call_iterator.go#L1087>.
fn percentile_idx(len: usize, percentile: f64) -> Option<usize> {
    match TryInto::<usize>::try_into(((len as f64) * percentile / 100.0 + 0.5).floor() as isize) {
        Ok(idx) if 0 < idx && idx < len => Some(idx),
        _ => None,
    }
}
