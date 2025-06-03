//! Evaluate the parameters to be used for gap filling.
use std::ops::Bound;
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, IntervalMonthDayNanoType, SchemaRef},
    record_batch::RecordBatch,
};
use chrono::Duration;
use datafusion::{
    common::exec_err,
    error::{DataFusionError, Result},
    functions::datetime::date_bin::DateBinFunc,
    physical_expr::PhysicalExpr,
    physical_plan::{expressions::Column, ColumnarValue},
    scalar::ScalarValue,
};
use hashbrown::HashMap;
use query_functions::date_bin_wallclock::DateBinWallclockUDF;

use super::{
    date_bin_gap_expander::DateBinGapExpander,
    date_bin_wallclock_gap_expander::DateBinWallclockGapExpander, try_map_bound, try_map_range,
    FillStrategy, GapExpander, GapFillExecParams,
};

/// The parameters to gap filling. Included here are the parameters
/// that remain constant during gap filling, i.e., not the streaming table
/// data, or anything else.
/// When we support `locf` for aggregate columns, that will be tracked here.
#[derive(Clone, Debug)]
pub(crate) struct GapFillParams {
    /// The gap_expander used to find gaps in the output rows.
    pub gap_expander: Arc<dyn GapExpander + Send + Sync>,
    /// The first timestamp (inclusive) to be output for each series,
    /// in nanoseconds since the epoch. `None` means gap filling should
    /// start from the first timestamp in each series.
    pub first_ts: Option<i64>,
    /// The last timestamp (inclusive!) to be output for each series,
    /// in nanoseconds since the epoch.
    pub last_ts: i64,
    /// What to do when filling gaps in aggregate columns.
    /// The map is keyed on the columns offset in the schema.
    pub fill_strategy: HashMap<usize, FillStrategy>,
}

impl GapFillParams {
    /// Create a new [GapFillParams] by figuring out the actual values (as native i64) for the stride,
    /// first and last timestamp for gap filling.
    pub(super) fn try_new(schema: SchemaRef, params: &GapFillExecParams) -> Result<Self> {
        let time_data_type = params.time_column.data_type(schema.as_ref())?;
        let DataType::Timestamp(_, tz) = time_data_type else {
            return exec_err!("invalid data type for time column: {time_data_type}");
        };

        let batch = RecordBatch::new_empty(schema);
        let stride = params.stride.evaluate(&batch)?;
        let origin = params
            .origin
            .as_ref()
            .map(|e| e.evaluate(&batch))
            .transpose()?;

        // Evaluate the upper and lower bounds of the time range
        let range = try_map_range(&params.time_range, |b| {
            try_map_bound(b.as_ref(), |pe| {
                extract_timestamp_nanos(&pe.evaluate(&batch)?)
            })
        })?;

        // Find the smallest timestamp that might appear in the
        // range. There might not be one, which is okay.
        let first_ts = match range.start {
            Bound::Included(v) => Some(v),
            Bound::Excluded(v) => Some(v + 1),
            Bound::Unbounded => None,
        };

        // Find the largest timestamp that might appear in the
        // range
        let last_ts = match range.end {
            Bound::Included(v) => v,
            Bound::Excluded(v) => v - 1,
            Bound::Unbounded => {
                return Err(DataFusionError::Execution(
                    "missing upper time bound for gap filling".to_string(),
                ));
            }
        };

        let stride_nanos = extract_interval_nanos(&stride)?;

        // Call date_bin on the timestamps to find the first and last time bins
        // for each series
        let mut args = vec![stride, i64_to_columnar_ts(first_ts, &tz)];
        if let Some(v) = origin {
            args.push(v)
        }
        let first_ts = first_ts
            .map(|_| extract_timestamp_nanos(&params.date_bin_udf.invoke_batch(&args, args.len())?))
            .transpose()?;
        args[1] = i64_to_columnar_ts(Some(last_ts), &tz);
        let last_ts =
            extract_timestamp_nanos(&params.date_bin_udf.invoke_batch(&args, args.len())?)?;

        let gap_expander: Arc<dyn GapExpander + Send + Sync> =
            if params.date_bin_udf.inner().as_any().is::<DateBinFunc>() {
                Arc::new(DateBinGapExpander::new(stride_nanos))
            } else if params
                .date_bin_udf
                .inner()
                .as_any()
                .is::<DateBinWallclockUDF>()
            {
                Arc::new(DateBinWallclockGapExpander::try_from_df_args(&args)?)
            } else {
                return Err(DataFusionError::Execution(format!(
                    "gap filling not supported for {}",
                    params.date_bin_udf.name()
                )));
            };

        let fill_strategy = params
            .fill_strategy
            .iter()
            .map(|(e, fs)| {
                let idx = e
                    .as_any()
                    .downcast_ref::<Column>()
                    .ok_or(DataFusionError::Internal(format!(
                        "fill strategy aggr expr was not a column: {e:?}",
                    )))?
                    .index();
                Ok((idx, fs.clone()))
            })
            .collect::<Result<HashMap<usize, FillStrategy>>>()?;

        Ok(Self {
            gap_expander,
            first_ts,
            last_ts,
            fill_strategy,
        })
    }
}

fn i64_to_columnar_ts(i: Option<i64>, tz: &Option<Arc<str>>) -> ColumnarValue {
    match i {
        Some(i) => ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(i), tz.clone())),
        None => ColumnarValue::Scalar(ScalarValue::Null),
    }
}

fn extract_timestamp_nanos(cv: &ColumnarValue) -> Result<i64> {
    Ok(match cv {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        _ => {
            return Err(DataFusionError::Execution(
                "gap filling argument must be a scalar timestamp".to_string(),
            ));
        }
    })
}

fn extract_interval_nanos(cv: &ColumnarValue) -> Result<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            if months != 0 {
                return Err(DataFusionError::Execution(
                    "gap filling does not support month intervals".to_string(),
                ));
            }

            let nanos = (Duration::try_days(days as i64).expect("days must be in bounds")
                + Duration::nanoseconds(nanos))
            .num_nanoseconds();
            nanos.ok_or_else(|| {
                DataFusionError::Execution("gap filling argument is too large".to_string())
            })
        }
        _ => Err(DataFusionError::Execution(
            "gap filling expects a stride parameter to be a scalar interval".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        ops::{Bound, Range},
        sync::Arc,
    };

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        datasource::empty::EmptyTable,
        error::Result,
        logical_expr::ScalarUDF,
        physical_plan::{
            expressions::{Column, Literal},
            PhysicalExpr,
        },
        scalar::ScalarValue,
    };
    use hashbrown::HashMap;

    use crate::exec::{
        gapfill::{FillStrategy, GapFillExec, GapFillExecParams},
        Executor,
    };

    #[tokio::test]
    async fn test_evaluate_params() -> Result<()> {
        test_helpers::maybe_start_logging();
        let params = plan_statement_and_get_params(
            "select\
               \n    date_bin_gapfill(interval '1 minute', time) minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
        )
        .await?;
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT60S]"
        );
        assert_eq!(params.first_ts, Some(441_820_500_000_000_000));
        assert_eq!(params.last_ts, 441_820_800_000_000_000);
        assert_eq!(params.fill_strategy, HashMap::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_default_origin() -> Result<()> {
        // as above but the default origin is explicity specified.
        test_helpers::maybe_start_logging();
        let params = plan_statement_and_get_params(
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT60S]"
        );
        assert_eq!(params.first_ts, Some(441_820_500_000_000_000));
        assert_eq!(params.last_ts, 441_820_800_000_000_000);
        assert_eq!(params.fill_strategy, HashMap::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_exclude_end() -> Result<()> {
        test_helpers::maybe_start_logging();
        let params = plan_statement_and_get_params(
            "select\
               \n    date_bin_gapfill(interval '1 minute', time) minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time < timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
        )
        .await?;
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT60S]" // 1 minute
        );
        assert_eq!(params.first_ts, Some(441_820_500_000_000_000)); // Sunday, January 1, 1984 3:55:00 PM

        // Last bin at 16:00 is excluded
        assert_eq!(params.last_ts, 441_820_740_000_000_000); // Sunday, January 1, 1984 3:59:00 PM
        assert_eq!(params.fill_strategy, HashMap::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_exclude_start() -> Result<()> {
        test_helpers::maybe_start_logging();
        let params = plan_statement_and_get_params(
            "select\
               \n    date_bin_gapfill(interval '1 minute', time) minute\
               \nfrom t\
               \nwhere time > timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
        )
        .await?;
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT60S]" // 1 minute
        );
        // First bin not exluded since it truncates to 15:55:00
        assert_eq!(params.first_ts, Some(441_820_500_000_000_000)); // Sunday, January 1, 1984 3:55:00 PM
        assert_eq!(params.last_ts, 441_820_800_000_000_000); // Sunday, January 1, 1984 3:59:00 PM
        assert_eq!(params.fill_strategy, HashMap::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_origin() -> Result<()> {
        test_helpers::maybe_start_logging();
        let params = plan_statement_and_get_params(
            // origin is 9s after the epoch
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:09Z') minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT60S]" // 1 minute
        );
        // First bin not exluded since it truncates to 15:55:00
        assert_eq!(params.first_ts, Some(441_820_449_000_000_000)); // Sunday, January 1, 1984 3:54:09 PM
        assert_eq!(params.last_ts, 441_820_749_000_000_000); // Sunday, January 1, 1984 3:59:09 PM
        assert_eq!(params.fill_strategy, HashMap::new());
        Ok(())
    }

    fn interval(ns: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::new_interval_mdn(0, 0, ns)))
    }

    fn timestamp(ns: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
            Some(ns),
            None,
        )))
    }

    #[test]
    fn test_params_no_start() {
        let exec_params = GapFillExecParams {
            date_bin_udf: Arc::new(ScalarUDF::new_from_impl(DateBinFunc::new())),
            stride: interval(1_000_000_000),
            time_column: Column::new("time", 0),
            origin: None,
            time_range: Range {
                start: Bound::Unbounded,
                end: Bound::Excluded(timestamp(20_000_000_000)),
            },
            fill_strategy: std::iter::once((
                Arc::new(Column::new("a0", 1)) as Arc<dyn PhysicalExpr>,
                FillStrategy::Default(ScalarValue::Null),
            ))
            .collect(),
        };

        let params = GapFillParams::try_new(schema().into(), &exec_params).unwrap();
        assert_eq!(
            params.gap_expander.to_string(),
            "DateBinGapExpander [stride=PT1S]"
        );
        assert_eq!(params.first_ts, None);
        assert_eq!(params.last_ts, 19_000_000_000);
        assert_eq!(params.fill_strategy, simple_fill_strategy());
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "other_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    async fn plan_statement_and_get_params(sql: &str) -> Result<GapFillParams> {
        let executor = Executor::new_testing();
        let context = executor.new_context();
        context
            .inner()
            .register_table("t", Arc::new(EmptyTable::new(Arc::new(schema()))))?;
        let physical_plan = context.sql_to_physical_plan(sql).await?;
        let gapfill_node = &physical_plan.children()[0];
        let gapfill_node = gapfill_node.as_any().downcast_ref::<GapFillExec>().unwrap();
        let exec_params = &gapfill_node.params;
        let schema = schema();
        GapFillParams::try_new(schema.into(), exec_params)
    }

    fn simple_fill_strategy() -> HashMap<usize, FillStrategy> {
        std::iter::once((1, FillStrategy::Default(ScalarValue::Null))).collect()
    }
}
