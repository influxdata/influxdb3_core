//! ## Overview
//!
//! *Selector functions* are special IOx SQL aggregate functions,
//! designed to provide the same semantics as the [selector functions]
//! in [InfluxQL]
//!
//! Selector functions are similar to standard aggregate functions in
//! that they collapse (aggregate) an input set of rows into a single
//! row.
//!
//! Selector functions are different than regular aggregate functions
//! because they rely on and return a `time` in addition to the
//! `value`. Time is implicit in InfluxQL, but not in SQL, so the
//! selector function invocation is slightly different in IOx SQL than
//! in InfluxQL.
//!
//! Each selector function returns a two part value, the `value` of
//! the first argument as well as the value for the corresponding
//! second argument, which must timestamp.
//!
//! ## Example
//!
//! Given the following input:
//!
//! ```text
//! +----------------------+-------------+
//! | time                 | water_level |
//! +----------------------+-------------+
//! | 2019-08-28T07:22:00Z | 9.8         |
//! | 2019-08-28T07:23:00Z | 9.7         |
//! | 2019-08-28T07:24:00Z | 10.00       |
//! | 2019-08-28T07:25:00Z | 9.9         |
//! +----------------------+-------------+
//! ```
//!
//! Using the SQL `min` aggregate function (not a selector) finds the
//! minimum `water_level` value, `9.7` in this case:
//!
//! ```sql
//! select min(water_level) from "h2o_feet";
//!
//! +-------------+
//! | water_level |
//! +-------------+
//! | 9.7         |
//! +-------------+
//! ```
//!
//! There is no easy way in SQL to determine at which value of `time`
//! the minimum value occurred, however `selector_min` returns this as well:
//!
//! ```sql
//! select selector_min(water_level, time) from "h2o_feet";
//!
//! +----------------------------------------------+
//! | selector_min(water_level,time)               |
//! +----------------------------------------------+
//! | {"value": 9.7, "time": 2019-08-28T07:23:00Z} |
//! +----------------------------------------------+
//! ```
//!
//! Note that the output is a `struct` with two fields, `value` and
//! `time`. To access the values, you can use the field reference
//! `['field_name']` syntax (note the use of single quotes `'` around
//! the field names):
//!
//! ```sql
//! select
//!   selector_min(water_level, time)['time'],
//!   selector_min(water_level, time)['value']
//! from "h2o_feet";
//! +----------------------------------------+-----------------------------------------+
//! | selector_first(water_level,time)[time] | selector_first(water_level,time)[value] |
//! +----------------------------------------+-----------------------------------------+
//! | 2019-08-28T07:23:00Z                   | 9.7                                     |
//! +----------------------------------------+-----------------------------------------+
//! ```
//!
//! ## Supported Selectors
//!
//! IOx supports the following selectors:
//!
//! 1. `selector_first`: `time` and `value` of the row with earliest `time` in the group
//! 2. `selector_last`: `time` and `value` of the row with latest `time` in the group
//! 3. `selector_min`: `time` and `value` of the row with smallest `value` in the group
//! 4. `selector_max`: `time` and `value` of the row with largest `value` in the group
//!
//! For `selector_first` / `selector_last`, if there are multiple
//! rows with same minimum / maximum timestamp, the value returned is
//! arbitrary
//!
//! For `selector_min` / `selector_max`, if there are multiple rows
//! with the same minimum / maximum value, the value with the smallest
//! timestamp is chosen.
//!
//! [InfluxQL]: https://docs.influxdata.com/influxdb/v1.8/query_language/
//! [selector functions]: https://docs.influxdata.com/influxdb/v1.8/query_language/functions/#selectors
use std::any::Any;
use std::fmt::Debug;
use std::sync::OnceLock;

use arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::AggregateUDFImpl;
use datafusion::{
    error::Result as DataFusionResult,
    logical_expr::{function::AccumulatorArgs, AggregateUDF, Signature, Volatility},
    physical_plan::{expressions::format_state_name, Accumulator},
    prelude::SessionContext,
};

mod internal;
use internal::{Comparison, Selector, Target};

mod type_handling;
use type_handling::AggType;

static SELECTOR_FIRST: OnceLock<AggregateUDF> = OnceLock::new();
static SELECTOR_LAST: OnceLock<AggregateUDF> = OnceLock::new();
static SELECTOR_MIN: OnceLock<AggregateUDF> = OnceLock::new();
static SELECTOR_MAX: OnceLock<AggregateUDF> = OnceLock::new();

/// registers selector functions so they can be invoked via SQL
pub fn register_selector_aggregates(ctx: &SessionContext) {
    ctx.register_udaf(selector_first());
    ctx.register_udaf(selector_last());
    ctx.register_udaf(selector_min());
    ctx.register_udaf(selector_max());
}

/// Returns a DataFusion user defined aggregate function for computing
/// the first(value, time) selector function, returning a struct:
///
/// first(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the minimum of the time column.
///   time: value of the minimum time column
/// }
/// ```
///
/// If there are multiple rows with the minimum timestamp value, the
/// value returned is arbitrary
pub fn selector_first() -> AggregateUDF {
    SELECTOR_FIRST
        .get_or_init(|| {
            AggregateUDF::new_from_impl(SelectorUDAFImpl::new(
                "selector_first",
                SelectorType::First,
            ))
        })
        .clone()
}

/// Returns a DataFusion user defined aggregate function for computing
/// the last(value, time) selector function, returning a struct:
///
/// last(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the maximum of the time column.
///   time: value of the maximum time column
/// }
/// ```
///
/// If there are multiple rows with the maximum timestamp value, the
/// value is arbitrary
pub fn selector_last() -> AggregateUDF {
    SELECTOR_LAST
        .get_or_init(|| {
            AggregateUDF::new_from_impl(SelectorUDAFImpl::new("selector_last", SelectorType::Last))
        })
        .clone()
}

/// Returns a DataFusion user defined aggregate function for computing
/// the min(value, time) selector function, returning a struct:
///
/// min(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with minimum value
///   time: value of time for row with minimum value
/// }
/// ```
///
/// If there are multiple rows with the same minimum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_min() -> AggregateUDF {
    SELECTOR_MIN
        .get_or_init(|| {
            AggregateUDF::new_from_impl(SelectorUDAFImpl::new("selector_min", SelectorType::Min))
        })
        .clone()
}

/// Returns a DataFusion user defined aggregate function for computing
/// the max(value, time) selector function, returning a struct:
///
/// max(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with maximum value
///   time: value of time for row with maximum value
/// }
/// ```
///
/// If there are multiple rows with the same maximum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_max() -> AggregateUDF {
    SELECTOR_MAX
        .get_or_init(|| {
            AggregateUDF::new_from_impl(SelectorUDAFImpl::new("selector_max", SelectorType::Max))
        })
        .clone()
}

#[derive(Debug, Clone, Copy)]
enum SelectorType {
    First,
    Last,
    Min,
    Max,
}

/// DataFusion user defined Aggregate Function (UDAF) for Selector functions
#[derive(Debug)]
struct SelectorUDAFImpl {
    name: String,
    selector_type: SelectorType,
    signature: Signature,
}

impl SelectorUDAFImpl {
    fn new(name: impl Into<String>, selector_type: SelectorType) -> Self {
        Self {
            name: name.into(),
            selector_type,
            signature: Signature::variadic_any(Volatility::Stable),
        }
    }
}

impl AggregateUDFImpl for SelectorUDAFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(AggType::try_from_arg_types(arg_types, &self.name)?.return_type())
    }

    fn accumulator(&self, arg: AccumulatorArgs<'_>) -> DataFusionResult<Box<dyn Accumulator>> {
        let agg_type = AggType::try_from_return_type(arg.data_type)?;
        let value_type = agg_type.value_type;
        let timezone = match agg_type.time_type {
            DataType::Timestamp(_, tz) => tz.clone(),
            _ => None,
        };
        let other_types = agg_type.other_types;

        let accumulator: Box<dyn Accumulator> = match self.selector_type {
            SelectorType::First => Box::new(Selector::new(
                Comparison::Min,
                Target::Time,
                timezone,
                value_type,
                other_types.iter().cloned(),
            )?),
            SelectorType::Last => Box::new(Selector::new(
                Comparison::Max,
                Target::Time,
                timezone,
                value_type,
                other_types.iter().cloned(),
            )?),
            SelectorType::Min => Box::new(Selector::new(
                Comparison::Min,
                Target::Value,
                timezone,
                value_type,
                other_types.iter().cloned(),
            )?),
            SelectorType::Max => Box::new(Selector::new(
                Comparison::Max,
                Target::Value,
                timezone,
                value_type,
                other_types.iter().cloned(),
            )?),
        };
        Ok(accumulator)
    }

    fn state_fields(
        &self,
        name: &str,
        value_type: DataType,
        _ordering_fields: Vec<arrow::datatypes::Field>,
    ) -> DataFusionResult<Vec<arrow::datatypes::Field>> {
        let fields = AggType::try_from_return_type(&value_type)?
            .state_datatypes()
            .into_iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(format_state_name(name, &format!("{i}")), data_type, true)
            })
            .collect::<Vec<_>>();

        Ok(fields)
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
            UInt64Array,
        },
        datatypes::{Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{datasource::MemTable, prelude::*};

    use super::*;
    use utils::{run_case, run_case_tz, run_cases_err};

    mod first {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_first().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_first(t.f64_value,t.time)             |",
                    "+------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_1_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_2_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_3_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_4_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_4_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: 1.0, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_5_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_5_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_first().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_first(t.i64_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 20, time: 1970-01-01T00:00:00.000001} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_first().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_first(t.u64_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 20, time: 1970-01-01T00:00:00.000001} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_first().call(vec![col("string_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_first(t.string_value,t.time)          |",
                    "+------------------------------------------------+",
                    "| {value: two, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_first().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.bool_value,t.time)             |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_with_other() {
            run_case(
                selector_first().call(vec![col("f64_value"), col("time"), col("bool_value"), col("f64_not_normal_1_value"), col("i64_2_value")]),
                vec![
                    "+----------------------------------------------------------------------------------------+",
                    "| selector_first(t.f64_value,t.time,t.bool_value,t.f64_not_normal_1_value,t.i64_2_value) |",
                    "+----------------------------------------------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001, other_1: true, other_2: NaN, other_3: } |",
                    "+----------------------------------------------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("i64_2_value"), col("time"), col("bool_value"), col("f64_not_normal_1_value"), col("i64_2_value")]),
                vec![
                    "+------------------------------------------------------------------------------------------+",
                    "| selector_first(t.i64_2_value,t.time,t.bool_value,t.f64_not_normal_1_value,t.i64_2_value) |",
                    "+------------------------------------------------------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005, other_1: false, other_2: inf, other_3: 50} |",
                    "+------------------------------------------------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_time_tie_breaker() {
            run_case(
                selector_first().call(vec![col("f64_value"), col("time_dup")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_first(t.f64_value,t.time_dup)         |",
                    "+------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_err() {
            run_cases_err(selector_first(), "selector_first").await;
        }

        #[tokio::test]
        async fn test_i64_tz() {
            run_case_tz(
                selector_first().call(vec![col("i64_value"), col("time")]),
                Some("Australia/Hobart".into()),
                vec![
                    "+-----------------------------------------------------+",
                    "| selector_first(t.i64_value,t.time)                  |",
                    "+-----------------------------------------------------+",
                    "| {value: 20, time: 1970-01-01T11:00:00.000001+11:00} |",
                    "+-----------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod last {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_last().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_value,t.time)              |",
                    "+------------------------------------------------+",
                    "| {value: 3.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_1_value,t.time)  |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000006} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_2_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: inf, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_3_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_4_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_4_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: 3.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_5_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_5_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: 4.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_last().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_last(t.i64_value,t.time)             |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_last().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_last(t.u64_value,t.time)             |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_last().call(vec![col("string_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_last(t.string_value,t.time)             |",
                    "+--------------------------------------------------+",
                    "| {value: three, time: 1970-01-01T00:00:00.000006} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_last().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_last(t.bool_value,t.time)               |",
                    "+--------------------------------------------------+",
                    "| {value: false, time: 1970-01-01T00:00:00.000006} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_with_other() {
            run_case(
                selector_last().call(vec![col("f64_value"), col("time"), col("bool_value"), col("f64_not_normal_3_value"), col("i64_2_value")]),
                vec![
                    "+-------------------------------------------------------------------------------------------+",
                    "| selector_last(t.f64_value,t.time,t.bool_value,t.f64_not_normal_3_value,t.i64_2_value)     |",
                    "+-------------------------------------------------------------------------------------------+",
                    "| {value: 3.0, time: 1970-01-01T00:00:00.000006, other_1: false, other_2: NaN, other_3: 30} |",
                    "+-------------------------------------------------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("u64_2_value"), col("time"), col("bool_value"), col("f64_not_normal_4_value"), col("i64_2_value")]),
                vec![
                    "+------------------------------------------------------------------------------------------+",
                    "| selector_last(t.u64_2_value,t.time,t.bool_value,t.f64_not_normal_4_value,t.i64_2_value)  |",
                    "+------------------------------------------------------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005, other_1: false, other_2: inf, other_3: 50} |",
                    "+------------------------------------------------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_time_tie_breaker() {
            run_case(
                selector_last().call(vec![col("f64_value"), col("time_dup")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_value,t.time_dup)          |",
                    "+------------------------------------------------+",
                    "| {value: 5.0, time: 1970-01-01T00:00:00.000003} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_err() {
            run_cases_err(selector_last(), "selector_last").await;
        }

        #[tokio::test]
        async fn test_i64_tz() {
            run_case_tz(
                selector_last().call(vec![col("i64_value"), col("time")]),
                Some("Australia/Adelaide".into()),
                vec![
                    "+-----------------------------------------------------+",
                    "| selector_last(t.i64_value,t.time)                   |",
                    "+-----------------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T09:30:00.000006+09:30} |",
                    "+-----------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod min {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_min().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_min(t.f64_value,t.time)               |",
                    "+------------------------------------------------+",
                    "| {value: 1.0, time: 1970-01-01T00:00:00.000004} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_1_value,t.time)   |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000003} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_2_value,t.time)   |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_3_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_min().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.i64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T00:00:00.000004} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("i64_2_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.i64_2_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_min().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.u64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T00:00:00.000004} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_min().call(vec![col("string_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_min(t.string_value,t.time)              |",
                    "+--------------------------------------------------+",
                    "| {value: a_one, time: 1970-01-01T00:00:00.000004} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_min().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_min(t.bool_value,t.time)                |",
                    "+--------------------------------------------------+",
                    "| {value: false, time: 1970-01-01T00:00:00.000002} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_with_other() {
            run_case(
                selector_min().call(vec![col("u64_value"), col("time"), col("bool_value"), col("f64_not_normal_1_value"), col("i64_2_value")]),
                vec![
                    "+---------------------------------------------------------------------------------------+",
                    "| selector_min(t.u64_value,t.time,t.bool_value,t.f64_not_normal_1_value,t.i64_2_value)  |",
                    "+---------------------------------------------------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T00:00:00.000004, other_1: true, other_2: NaN, other_3: } |",
                    "+---------------------------------------------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_time_tie_breaker() {
            run_case(
                selector_min().call(vec![col("f64_not_normal_2_value"), col("time_dup")]),
                vec![
                    "+---------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_2_value,t.time_dup) |",
                    "+---------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000001}   |",
                    "+---------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("bool_const"), col("time_dup")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_min(t.bool_const,t.time_dup)           |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_err() {
            run_cases_err(selector_min(), "selector_min").await;
        }

        #[tokio::test]
        async fn test_i64_tz() {
            run_case_tz(
                selector_min().call(vec![col("i64_value"), col("time")]),
                Some("Pacific/Chatham".into()),
                vec![
                    "+-----------------------------------------------------+",
                    "| selector_min(t.i64_value,t.time)                    |",
                    "+-----------------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T12:45:00.000004+12:45} |",
                    "+-----------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod max {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_max().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_value,t.time)               |",
                    "+------------------------------------------------+",
                    "| {value: 5.0, time: 1970-01-01T00:00:00.000005} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_1_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_2_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: inf, time: 1970-01-01T00:00:00.000004} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_3_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_max().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.i64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("i64_2_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.i64_2_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_max().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.u64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_max().call(vec![col("string_value"), col("time")]),
                vec![
                    "+---------------------------------------------------+",
                    "| selector_max(t.string_value,t.time)               |",
                    "+---------------------------------------------------+",
                    "| {value: z_five, time: 1970-01-01T00:00:00.000005} |",
                    "+---------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_max().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_max(t.bool_value,t.time)               |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_with_other() {
            run_case(
                selector_max().call(vec![col("u64_value"), col("time"), col("bool_value"), col("f64_not_normal_1_value"), col("i64_2_value")]),
                vec![
                    "+------------------------------------------------------------------------------------------+",
                    "| selector_max(t.u64_value,t.time,t.bool_value,t.f64_not_normal_1_value,t.i64_2_value)     |",
                    "+------------------------------------------------------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005, other_1: false, other_2: inf, other_3: 50} |",
                    "+------------------------------------------------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("bool_const"), col("time"), col("bool_value"), col("f64_not_normal_1_value"), col("i64_value")]),
                vec![
                    "+-------------------------------------------------------------------------------------------+",
                    "| selector_max(t.bool_const,t.time,t.bool_value,t.f64_not_normal_1_value,t.i64_value)       |",
                    "+-------------------------------------------------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001, other_1: true, other_2: NaN, other_3: 20} |",
                    "+-------------------------------------------------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_time_tie_breaker() {
            run_case(
                selector_max().call(vec![col("f64_not_normal_2_value"), col("time_dup")]),
                vec![
                    "+---------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_2_value,t.time_dup) |",
                    "+---------------------------------------------------+",
                    "| {value: inf, time: 1970-01-01T00:00:00.000002}    |",
                    "+---------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("bool_const"), col("time_dup")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_max(t.bool_const,t.time_dup)           |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_err() {
            run_cases_err(selector_max(), "selector_max").await;
        }

        #[tokio::test]
        async fn test_i64_tz() {
            run_case_tz(
                selector_max().call(vec![col("i64_value"), col("time")]),
                Some("-05:00".into()),
                vec![
                    "+-----------------------------------------------------+",
                    "| selector_max(t.i64_value,t.time)                    |",
                    "+-----------------------------------------------------+",
                    "| {value: 50, time: 1969-12-31T19:00:00.000005-05:00} |",
                    "+-----------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod utils {
        use arrow::datatypes::TimeUnit;
        use std::sync::Arc;

        use super::*;

        /// Runs the expr using `run_plan` and compares the result to `expected`
        pub async fn run_case(expr: Expr, expected: Vec<&'static str>) {
            run_case_tz(expr, None, expected).await
        }

        /// Runs the expr using `run_plan` in the requested timezone and compares the result to `expected`
        pub async fn run_case_tz(expr: Expr, tz: Option<Arc<str>>, expected: Vec<&'static str>) {
            if let Some(tz) = tz.as_ref() {
                println!("Running case for {expr} in timezone {tz}");
            } else {
                println!("Running case for {expr}");
            }

            let actual = run_plan(vec![expr.clone()], tz).await;

            assert_eq!(
                expected, actual,
                "\n\nexpr: {expr}\n\nEXPECTED:\n{expected:#?}\nACTUAL:\n{actual:#?}\n"
            );
        }

        pub async fn run_case_err(expr: Expr, expected: &str) {
            println!("Running error case for {expr}");

            let (schema, input) = input(None);
            let actual = run_with_inputs(Arc::clone(&schema), vec![expr.clone()], input.clone())
                .await
                .unwrap_err()
                .to_string();

            assert_eq!(
                expected, actual,
                "\n\nexpr: {expr}\n\nEXPECTED:\n{expected:#?}\nACTUAL:\n{actual:#?}\n"
            );
        }

        pub async fn run_cases_err(selector: AggregateUDF, name: &str) {
            run_case_err(
                selector.call(vec![]),
                &format!("Error during planning: {name} requires at least 2 arguments, got 0"),
            )
            .await;

            run_case_err(
                selector.call(vec![col("f64_value")]),
                &format!("Error during planning: {name} requires at least 2 arguments, got 1"),
            )
            .await;

            run_case_err(
                selector.call(vec![col("time"), col("f64_value")]),
                &format!("Error during planning: {name} second argument must be a timestamp, but got Float64"),
            )
            .await;

            run_case_err(
                selector.call(vec![col("time"), col("f64_value"), col("bool_value")]),
                &format!("Error during planning: {name} second argument must be a timestamp, but got Float64"),
            )
            .await;

            run_case_err(
                selector.call(vec![col("f64_value"), col("bool_value"), col("time")]),
                &format!("Error during planning: {name} second argument must be a timestamp, but got Boolean"),
            )
            .await;
        }

        fn input(tz: Option<Arc<str>>) -> (SchemaRef, Vec<RecordBatch>) {
            // define a schema for input
            // (value) and timestamp
            let schema = Arc::new(Schema::new(vec![
                Field::new("f64_value", DataType::Float64, true),
                Field::new("f64_not_normal_1_value", DataType::Float64, true),
                Field::new("f64_not_normal_2_value", DataType::Float64, true),
                Field::new("f64_not_normal_3_value", DataType::Float64, true),
                Field::new("f64_not_normal_4_value", DataType::Float64, true),
                Field::new("f64_not_normal_5_value", DataType::Float64, true),
                Field::new("i64_value", DataType::Int64, true),
                Field::new("i64_2_value", DataType::Int64, true),
                Field::new("u64_value", DataType::UInt64, true),
                Field::new("u64_2_value", DataType::UInt64, true),
                Field::new("string_value", DataType::Utf8, true),
                Field::new("bool_value", DataType::Boolean, true),
                Field::new("bool_const", DataType::Boolean, true),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                    true,
                ),
                Field::new(
                    "time_dup",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                    true,
                ),
            ]));

            // define data in two partitions
            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![Some(2.0), Some(4.0), None])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(1.0),
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(2.0),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(20), Some(40), None])),
                    Arc::new(Int64Array::from(vec![None, None, None])),
                    Arc::new(UInt64Array::from(vec![Some(20), Some(40), None])),
                    Arc::new(UInt64Array::from(vec![Some(20), Some(40), None])),
                    Arc::new(StringArray::from(vec![Some("two"), Some("four"), None])),
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                    Arc::new(BooleanArray::from(vec![Some(true), Some(true), Some(true)])),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![1000, 2000, 3000])
                            .with_timezone_opt(tz.clone()),
                    ),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![1000, 1000, 2000])
                            .with_timezone_opt(tz.clone()),
                    ),
                ],
            )
            .unwrap();

            // No values in this batch
            let batch2 = match RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                    Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                    Arc::new(UInt64Array::from(vec![] as Vec<Option<u64>>)),
                    Arc::new(UInt64Array::from(vec![] as Vec<Option<u64>>)),
                    Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                    Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                    Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![] as Vec<i64>)
                            .with_timezone_opt(tz.clone()),
                    ),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![] as Vec<i64>)
                            .with_timezone_opt(tz.clone()),
                    ),
                ],
            ) {
                Ok(a) => a,
                _ => unreachable!(),
            };

            let batch3 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![Some(1.0), Some(5.0), Some(3.0)])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                        Some(3.0),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(4.0),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(10), Some(50), Some(30)])),
                    Arc::new(Int64Array::from(vec![None, Some(50), Some(30)])),
                    Arc::new(UInt64Array::from(vec![Some(10), Some(50), Some(30)])),
                    Arc::new(UInt64Array::from(vec![Some(10), Some(50), None])),
                    Arc::new(StringArray::from(vec![
                        Some("a_one"),
                        Some("z_five"),
                        Some("three"),
                    ])),
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(false),
                        Some(false),
                    ])),
                    Arc::new(BooleanArray::from(vec![Some(true), Some(true), Some(true)])),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![4000, 5000, 6000])
                            .with_timezone_opt(tz.clone()),
                    ),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![2000, 3000, 3000])
                            .with_timezone_opt(tz.clone()),
                    ),
                ],
            )
            .unwrap();

            let input = vec![batch1, batch2, batch3];

            (schema, input)
        }

        /// Run a plan against the following input table as "t"
        ///
        /// ```text
        /// +-----------+-----------+-----------+--------------+------------+----------------------------+,
        /// | f64_value | i64_value | u64_value | string_value | bool_value | time                       |,
        /// +-----------+-----------+--------------+------------+----------------------------+,
        /// | 2         | 20        | 20        | two          | true       | 1970-01-01T00:00:00.000001 |,
        /// | 4         | 40        | 40        | four         | false      | 1970-01-01T00:00:00.000002 |,
        /// |           |           |           |              |            | 1970-01-01T00:00:00.000003 |,
        /// | 1         | 10        | 10        | a_one        | true       | 1970-01-01T00:00:00.000004 |,
        /// | 5         | 50        | 50        | z_five       | false      | 1970-01-01T00:00:00.000005 |,
        /// | 3         | 30        | 30        | three        | false      | 1970-01-01T00:00:00.000006 |,
        /// +-----------+-----------+--------------+------------+----------------------------+,
        /// ```
        async fn run_plan(aggs: Vec<Expr>, tz: Option<Arc<str>>) -> Vec<String> {
            let (schema, input) = input(tz);

            // Ensure the answer is the same regardless of the order of inputs
            let input_string = pretty_format_batches(&input).unwrap();
            let results = run_with_inputs(Arc::clone(&schema), aggs.clone(), input.clone())
                .await
                .unwrap();

            use itertools::Itertools;
            // Get all permutations of the input
            for p in input.iter().permutations(3) {
                let p_batches = p.into_iter().cloned().collect::<Vec<_>>();
                let p_input_string = pretty_format_batches(&p_batches).unwrap();
                let p_results = run_with_inputs(Arc::clone(&schema), aggs.clone(), p_batches)
                    .await
                    .unwrap();
                assert_eq!(
                    results, p_results,
                    "Mismatch with permutation.\n\
                            Input1 \n\n\
                            {input_string}\n\n\
                            produces output:\n\n\
                            {results:#?}\n\n\
                            Input 2\n\n\
                            {p_input_string}\n\n\
                            produces output:\n\n\
                            {p_results:#?}\n\n"
                );
            }

            results
        }

        async fn run_with_inputs(
            schema: SchemaRef,
            aggs: Vec<Expr>,
            inputs: Vec<RecordBatch>,
        ) -> DataFusionResult<Vec<String>> {
            let provider = MemTable::try_new(Arc::clone(&schema), vec![inputs])?;
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(provider))?;

            let df = ctx.table("t").await?;
            let df = df.aggregate(vec![], aggs)?;

            // execute the query
            let record_batches = df.collect().await?;

            Ok(pretty_format_batches(&record_batches)?
                .to_string()
                .split('\n')
                .map(|s| s.to_owned())
                .collect())
        }
    }
}
