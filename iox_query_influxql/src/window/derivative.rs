use crate::{error, NUMERICS};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, IntervalUnit::MonthDayNano, TimeUnit};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl, TIMEZONE_WILDCARD,
};
use observability_deps::tracing::warn;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct DerivativeUDWF {
    signature: Signature,
}

impl DerivativeUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .flat_map(|dt| {
                        [
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Interval(MonthDayNano),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Interval(MonthDayNano),
                                DataType::Timestamp(
                                    TimeUnit::Nanosecond,
                                    Some(TIMEZONE_WILDCARD.into()),
                                ),
                            ]),
                        ]
                    })
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for DerivativeUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "derivative"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(DifferencePartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns the derivative between input values,
/// in the provided units.
#[derive(Debug)]
struct DifferencePartitionEvaluator {}

impl PartitionEvaluator for DifferencePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 3);

        let array = Arc::clone(&values[0]);
        let times = Arc::clone(&values[2]);

        // The second element of the values array is the second argument to
        // the 'derivative' function. This specifies the unit duration for the
        // derivation to use.
        //
        // INVARIANT:
        // The planner guarantees that the second argument is always a duration
        // literal.
        let unit = ScalarValue::try_from_array(&values[1], 0)?;

        let mut idx: usize = 0;
        let mut last: ScalarValue = array.data_type().try_into()?;
        let mut last_time: ScalarValue = times.data_type().try_into()?;
        let mut derivative: Vec<ScalarValue> = vec![];

        while idx < array.len() {
            last = ScalarValue::try_from_array(&array, idx)?;
            last_time = ScalarValue::try_from_array(&times, idx)?;
            derivative.push(ScalarValue::Float64(None));
            idx += 1;
            if !last.is_null() {
                break;
            }
        }
        while idx < array.len() {
            let v = ScalarValue::try_from_array(&array, idx)?;
            let t = ScalarValue::try_from_array(&times, idx)?;
            if v.is_null() {
                derivative.push(ScalarValue::Float64(None));
            } else {
                derivative.push(ScalarValue::Float64(Some(
                    delta(&v, &last)? / delta_time(&t, &last_time, &unit)?,
                )));
                last = v.clone();
                last_time = t.clone();
            }
            idx += 1;
        }
        Ok(Arc::new(ScalarValue::iter_to_array(derivative)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

/// Calculate the absolute different of two numerical values.
fn delta(curr: &ScalarValue, prev: &ScalarValue) -> Result<f64> {
    match (curr, prev) {
        (ScalarValue::Float64(Some(curr)), ScalarValue::Float64(Some(prev))) => Ok(*curr - *prev),
        (ScalarValue::Int64(Some(curr)), ScalarValue::Int64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        (ScalarValue::UInt64(Some(curr)), ScalarValue::UInt64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        _ => error::internal("derivative attempted on unsupported values"),
    }
}

/// Calculate the nanosecond time difference, scaled to the unit.
fn delta_time(curr: &ScalarValue, prev: &ScalarValue, unit: &ScalarValue) -> Result<f64> {
    if let (
        ScalarValue::TimestampNanosecond(Some(curr), tz_curr),
        ScalarValue::TimestampNanosecond(Some(prev), tz_prev),
        ScalarValue::IntervalMonthDayNano(Some(unit)),
    ) = (curr, prev, unit)
    {
        if !tz_curr.eq(tz_prev) {
            warn!("timezones do not match for the delta_time comparison, however, the scalar nanoseconds should always be in UTC")
        }
        Ok((*curr as f64 - *prev as f64) / unit.nanoseconds as f64)
    } else {
        error::internal("derivative attempted on unsupported values")
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::IntervalMonthDayNano;
    use assert_matches::assert_matches;

    use super::*;

    const NS_PER_SECOND: i64 = 1000000000;
    const NS_PER_HOUR: i64 = NS_PER_SECOND * 60 * 60;

    struct Case {
        curr_ns: Option<i64>,
        prev_ns: Option<i64>,
        unit: Option<IntervalMonthDayNano>,
        expected: Result<f64>,
    }

    type TZ = Arc<str>;

    fn run_test(delta_test_cases: Vec<Case>) {
        let timezones: Vec<(Option<TZ>, Option<TZ>)> = vec![
            (None, None),
            // UTCs
            (Some("UTC".into()), None),
            (None, Some("UTC".into())),
            (Some("UTC".into()), Some("UTC".into())),
            // non-UTCs
            (Some("PDT".into()), None),
            (None, Some("PDT".into())),
            (Some("PDT".into()), Some("PDT".into())),
            // diff tz
            (Some("UTC".into()), Some("PDT".into())),
        ];

        for Case {
            curr_ns,
            prev_ns,
            unit,
            expected,
        } in delta_test_cases
        {
            for (curr_tz, prev_tz) in timezones.clone() {
                let res = delta_time(
                    &ScalarValue::TimestampNanosecond(curr_ns, curr_tz),
                    &ScalarValue::TimestampNanosecond(prev_ns, prev_tz),
                    &ScalarValue::IntervalMonthDayNano(unit),
                );
                match expected {
                    Ok(expected) => {
                        assert_matches!(
                            res,
                            Ok(res) if res == expected,
                            "should have {} difference measured in {:?}, instead found {:?}", expected, unit, res
                        );
                    }
                    Err(_) => {
                        assert_matches!(res, Err(_), "should have error, instead found {:?}", res);
                    }
                }
            }
        }
    }

    #[test]
    fn test_delta_time() {
        let delta_test_cases = vec![
            // with prev as Some(0)
            Case {
                curr_ns: Some(NS_PER_HOUR), // one hour later
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)), // diff in hours
                expected: Ok(1.0),
            },
            Case {
                curr_ns: Some(NS_PER_HOUR), // one hour later
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND)), // diff in seconds
                expected: Ok(60.0 * 60.0),
            },
            // with prev as > Some(0)
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)), // diff in hours
                expected: Ok(1.0 * 12.0),
            },
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND)), // diff in seconds
                expected: Ok(60.0 * 60.0 * 12.0),
            },
        ];

        run_test(delta_test_cases);
    }

    #[test]
    fn test_delta_time_different_units() {
        let inf = f64::INFINITY;

        let delta_test_cases = vec![
            // nanoseconds
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND * 60 * 60)), // diff in hours
                expected: Ok(12.0),
            },
            // days, incorrect
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 48), // 48 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 1, 0)), // diff in days
                // the contract of IntervalMonthDayNano assumes that the nanosecond units will always exist
                // TODO: should we be returning infinity in this case? or erroring?
                expected: Ok(inf),
            },
            // days, correct
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 48), // 48 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 1, NS_PER_HOUR * 24)), // diff in days
                expected: Ok(2.0),
            },
            // month, incorrect
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 24 * 31), // 1 month later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(1, 0, 0)), // diff in months
                // the contract of IntervalMonthDayNano assumes that the nanosecond units will always exist
                // TODO: should we be returning infinity in this case? or erroring?
                expected: Ok(inf),
            },
            // month, correct
            Case {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 24 * 31), // 1 month later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(1, 0, NS_PER_HOUR * 24 * 31)), //  diff in months
                expected: Ok(1.0),
            },
        ];

        run_test(delta_test_cases);
    }

    #[test]
    fn test_delta_time_should_error() {
        let delta_test_cases = vec![
            // curr & prev are None
            Case {
                curr_ns: None,
                prev_ns: None,
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("derivative attempted on unsupported values"),
            },
            // with prev as None
            Case {
                curr_ns: Some(NS_PER_HOUR),
                prev_ns: None,
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("derivative attempted on unsupported values"),
            },
            // with curr as None
            Case {
                curr_ns: None,
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("derivative attempted on unsupported values"),
            },
            // with unit as None
            Case {
                curr_ns: Some(NS_PER_HOUR),
                prev_ns: Some(0),
                unit: None,
                expected: error::internal("derivative attempted on unsupported values"),
            },
        ];

        run_test(delta_test_cases);
    }
}
