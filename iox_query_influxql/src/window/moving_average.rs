use crate::{error, NUMERICS};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct MovingAverageUDWF {
    signature: Signature,
}

impl MovingAverageUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .map(|dt| TypeSignature::Exact(vec![dt.clone(), DataType::Int64]))
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for MovingAverageUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "moving_average"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, true))
    }

    /// Include this as a workaround for <https://github.com/apache/datafusion/issues/13168>
    fn expressions(&self, expr_args: ExpressionArgs<'_>) -> Vec<Arc<dyn PhysicalExpr>> {
        expr_args.input_exprs().into()
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(AvgNPartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns a moving average of the input data..
#[derive(Debug)]
struct AvgNPartitionEvaluator {}

impl PartitionEvaluator for AvgNPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 2, "AVG_N expects two arguments");

        // The second element of the values array is the second argument to the `moving_average`
        // function, which specifies the minimum number of values that must be aggregated.
        //
        // INVARIANT:
        // The planner and rewriter guarantee that the second argument is
        // always a numeric constant.
        //
        // See: FieldChecker::check_moving_average
        let n_values = downcast_value!(&values[1], Int64Array);
        let n = n_values.value(0);

        let array = &values[0];
        let mut deq: VecDeque<f64> = VecDeque::new();
        let mut avg_n: Vec<ScalarValue> = vec![];
        for idx in 0..array.len() {
            let value = match ScalarValue::try_from_array(&array, idx)? {
                ScalarValue::Float64(o) => o,
                ScalarValue::Int64(o) => o.map(|v| v as f64),
                ScalarValue::UInt64(o) => o.map(|v| v as f64),
                _ => {
                    return error::internal(format!(
                        "unsupported data type for moving_average ({})",
                        array.data_type()
                    ));
                }
            };
            match value {
                None => {
                    avg_n.push(ScalarValue::Float64(None));
                    continue;
                }
                Some(v) => {
                    deq.push_back(v);
                    if deq.len() > n as usize {
                        deq.pop_front();
                    }
                    if deq.len() != n as usize {
                        avg_n.push(ScalarValue::Float64(None));
                        continue;
                    }
                    avg_n.push(ScalarValue::Float64(Some(
                        deq.iter().sum::<f64>() / n as f64,
                    )));
                }
            }
        }
        Ok(Arc::new(ScalarValue::iter_to_array(avg_n)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}
