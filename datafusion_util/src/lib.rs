#![allow(clippy::clone_on_ref_ptr)]

//! This module contains various DataFusion utility functions.
//!
//! Almost everything for manipulating DataFusion `Expr`s IOx should be in DataFusion already
//! (or if not it should be upstreamed).
//!
//! For example, check out
//! [datafusion_optimizer::utils](https://docs.rs/datafusion-optimizer/13.0.0/datafusion_optimizer/utils/index.html)
//! for expression manipulation functions.

use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use std::collections::HashSet;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod config;
pub mod sender;
pub mod watch;

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, ToDFSchema};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::utils::inspect_expr_pre;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::{collect, EmptyRecordBatchStream, ExecutionPlan};
use datafusion::prelude::{lit, Column, Expr, SessionContext};
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
    scalar::ScalarValue,
};
use futures::{Stream, StreamExt};
use schema::TIME_DATA_TIMEZONE;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use watch::WatchedTask;

/// Traits to help creating DataFusion [`Expr`]s
pub trait AsExpr {
    /// Creates a DataFusion expr
    fn as_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn as_sort_expr(&self) -> Expr {
        Expr::Sort(Sort {
            expr: Box::new(self.as_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        })
    }
}

impl AsExpr for Arc<str> {
    fn as_expr(&self) -> Expr {
        self.as_ref().as_expr()
    }
}

impl AsExpr for str {
    fn as_expr(&self) -> Expr {
        // note using `col(<ident>)` will parse identifiers and try to
        // split them on `.`.
        //
        // So it would treat 'foo.bar' as table 'foo', column 'bar'
        //
        // This is not correct for influxrpc, so instead treat it
        // like the column "foo.bar"
        Expr::Column(Column {
            relation: None,
            name: self.into(),
        })
    }
}

impl AsExpr for Expr {
    fn as_expr(&self) -> Expr {
        self.clone()
    }
}

/// `coalsce` expr_fn function that returns the first non-null argument.
///
/// workaround for <https://github.com/apache/datafusion/issues/10320> issue in
/// DataFusion. Should use [datafusion::prelude::coalesce]  when that is fixed
pub fn coalesce(args: Vec<Expr>) -> Expr {
    datafusion::functions::core::coalesce().call(args)
}

/// Creates an `Expr` that represents a Dictionary encoded string (e.g
/// the type of constant that a tag would be compared to)
pub fn lit_dict(value: impl Into<String>) -> Expr {
    lit(dict(value))
}

/// Creates an `ScalarValue` that represents a Dictionary encoded string (e.g
/// the type of constant that a tag would be compared to)
pub fn dict(value: impl Into<String>) -> ScalarValue {
    // expr has been type coerced
    ScalarValue::Dictionary(
        Box::new(DataType::Int32),
        Box::new(ScalarValue::new_utf8(value)),
    )
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    // We need to cast the start and end values to timestamps
    // the equivalent of:
    let ts_start = timestamptz_nano(start);
    let ts_end = timestamptz_nano(end);

    let time_col = time.as_ref().as_expr();
    let ts_low = lit(ts_start).lt_eq(time_col.clone());
    let ts_high = time_col.lt(lit(ts_end));

    ts_low.and(ts_high)
}

/// Ensures all columns referred to in `filters` are in the `projection`, if
/// any, adding them if necessary.
pub fn extend_projection_for_filters(
    schema: &Schema,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
) -> Result<Option<Vec<usize>>, DataFusionError> {
    let Some(mut projection) = projection.cloned() else {
        return Ok(None);
    };

    let mut seen_cols: HashSet<usize> = projection.iter().cloned().collect();
    for filter in filters {
        inspect_expr_pre(filter, |expr| {
            if let Expr::Column(c) = expr {
                let idx = schema.index_of(&c.name)?;
                // if haven't seen this column before, add it to the list
                if seen_cols.insert(idx) {
                    projection.push(idx);
                }
            }
            Ok(()) as Result<(), DataFusionError>
        })?;
    }
    Ok(Some(projection))
}

// TODO port this upstream to datafusion (maybe as From<Option> for Precision)
/// Maps `Option::Some(T)` to `Precision::Exact(T)` and `Option::None` to
/// `Precision::Absent`
pub fn option_to_precision<T: std::fmt::Debug + Clone + PartialEq + Eq + PartialOrd>(
    option: Option<T>,
) -> Precision<T> {
    match option {
        Some(value) => Precision::Exact(value),
        None => Precision::Absent,
    }
}

/// A RecordBatchStream created from in-memory RecordBatches.
#[derive(Debug)]
pub struct MemoryStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemoryStream {
    /// Create new stream.
    ///
    /// Must at least pass one record batch!
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        assert!(!batches.is_empty(), "must at least pass one record batch");
        Self {
            schema: batches[0].schema(),
            batches,
        }
    }

    /// Create new stream with provided schema.
    pub fn new_with_schema(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self { schema, batches }
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(self.batches.remove(0))))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.batches.len(), Some(self.batches.len()))
    }
}

#[derive(Debug)]
/// Implements a [`SendableRecordBatchStream`] to help create DataFusion outputs
/// from tokio channels.
///
/// It sends streams of RecordBatches from a tokio channel *and* crucially knows
/// up front the schema each batch will have be used.
pub struct AdapterStream<T> {
    /// Schema
    schema: SchemaRef,
    /// channel for getting deduplicated batches
    inner: T,

    /// Optional join handles of underlying tasks.
    #[allow(dead_code)]
    task: Arc<WatchedTask>,
}

impl AdapterStream<ReceiverStream<Result<RecordBatch, DataFusionError>>> {
    /// Create a new stream which wraps the `inner` channel which produces
    /// [`RecordBatch`]es that each have the specified schema
    ///
    /// Not called `new` because it returns a pinned reference rather than the
    /// object itself.
    pub fn adapt(
        schema: SchemaRef,
        rx: Receiver<Result<RecordBatch, DataFusionError>>,
        task: Arc<WatchedTask>,
    ) -> SendableRecordBatchStream {
        let inner = ReceiverStream::new(rx);
        Box::pin(Self {
            schema,
            inner,
            task,
        })
    }
}

impl AdapterStream<UnboundedReceiverStream<Result<RecordBatch, DataFusionError>>> {
    /// Create a new stream which wraps the `inner` unbounded channel which
    /// produces [`RecordBatch`]es that each have the specified schema
    ///
    /// Not called `new` because it returns a pinned reference rather than the
    /// object itself.
    pub fn adapt_unbounded(
        schema: SchemaRef,
        rx: UnboundedReceiver<Result<RecordBatch, DataFusionError>>,
        task: Arc<WatchedTask>,
    ) -> SendableRecordBatchStream {
        let inner = UnboundedReceiverStream::new(rx);
        Box::pin(Self {
            schema,
            inner,
            task,
        })
    }
}

impl<T> Stream for AdapterStream<T>
where
    T: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<T> RecordBatchStream for AdapterStream<T>
where
    T: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Create a SendableRecordBatchStream a RecordBatch
pub fn stream_from_batch(schema: SchemaRef, batch: RecordBatch) -> SendableRecordBatchStream {
    stream_from_batches(schema, vec![batch])
}

/// Create a SendableRecordBatchStream from Vec of RecordBatches with the same schema
pub fn stream_from_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> SendableRecordBatchStream {
    if batches.is_empty() {
        return Box::pin(EmptyRecordBatchStream::new(schema));
    }
    Box::pin(MemoryStream::new_with_schema(batches, schema))
}

/// Create a SendableRecordBatchStream that sends back no RecordBatches with a specific schema
pub fn stream_from_schema(schema: SchemaRef) -> SendableRecordBatchStream {
    stream_from_batches(schema, vec![])
}

/// Execute the [ExecutionPlan] with a default [SessionContext] and
/// collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let session_ctx = SessionContext::new();
    let task_ctx = Arc::new(TaskContext::from(&session_ctx));
    collect(plan, task_ctx).await.unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [SessionContext] returning the resulting stream.
///
/// # Panics
/// If an an error occurs
pub async fn test_execute_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> SendableRecordBatchStream {
    let session_ctx = SessionContext::new();
    let task_ctx = Arc::new(TaskContext::from(&session_ctx));
    plan.execute(partition, task_ctx).unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [SessionContext] and collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Vec<RecordBatch> {
    let stream = test_execute_partition(plan, partition).await;
    datafusion::physical_plan::common::collect(stream)
        .await
        .unwrap()
}

/// Filter data from RecordBatch
///
/// Borrowed from DF's <https://github.com/apache/arrow-datafusion/blob/ecd0081bde98e9031b81aa6e9ae2a4f309fcec12/datafusion/src/physical_plan/filter.rs#L186>.
// TODO: if we make DF batch_filter public, we can call that function directly
pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Filter predicate evaluated to non-boolean value".to_string(),
                    )
                })
                // apply filter array to record batch
                .and_then(|filter_array| {
                    filter_record_batch(batch, filter_array)
                        .map_err(|err| DataFusionError::ArrowError(err, None))
                })
        })
}

/// Returns a new schema where all the fields are nullable
pub fn nullable_schema(schema: SchemaRef) -> SchemaRef {
    // they are all already nullable
    if schema.fields().iter().all(|f| f.is_nullable()) {
        schema
    } else {
        // make a new schema with all nullable fields
        let new_fields: Fields = schema
            .fields()
            .iter()
            .map(|f| {
                // make a copy of the field, but allow it to be nullable
                f.as_ref().clone().with_nullable(true)
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }
}

/// Returns a [`PhysicalExpr`] from the logical [`Expr`] and Arrow [`SchemaRef`]
pub fn create_physical_expr_from_schema(
    props: &ExecutionProps,
    expr: &Expr,
    schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let df_schema = Arc::clone(schema).to_dfschema_ref()?;
    create_physical_expr(expr, df_schema.as_ref(), props)
}

/// Returns a [`PruningPredicate`] from the logical [`Expr`] and Arrow [`SchemaRef`]
pub fn create_pruning_predicate(
    props: &ExecutionProps,
    expr: &Expr,
    schema: &SchemaRef,
) -> Result<PruningPredicate, DataFusionError> {
    let expr = create_physical_expr_from_schema(props, expr, schema)?;
    PruningPredicate::try_new(expr, Arc::clone(schema))
}

/// Create a memory pool that has no limit
pub fn unbounded_memory_pool() -> Arc<dyn MemoryPool> {
    Arc::new(UnboundedMemoryPool::default())
}

/// Create a timestamp literal for the given UTC nanosecond offset in
/// the timezone specified by [TIME_DATA_TIMEZONE].
///
/// N.B. If [TIME_DATA_TIMEZONE] specifies the None timezone then this
/// function behaves identially to [datafusion::prelude::lit_timestamp_nano].
pub fn lit_timestamptz_nano(ns: i64) -> Expr {
    lit(timestamptz_nano(ns))
}

/// Create a scalar timestamp value for the given UTC nanosecond offset
/// in the timezone specified by [TIME_DATA_TIMEZONE].
pub fn timestamptz_nano(ns: i64) -> ScalarValue {
    ScalarValue::TimestampNanosecond(Some(ns), TIME_DATA_TIMEZONE())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let ts_predicate_expr = make_range_expr(101, 202, "time");
        let expected_timezone = match TIME_DATA_TIMEZONE() {
            Some(tz) => format!("Some(\"{tz}\")"),
            None => "None".into(),
        };
        let expected_string =
            format!("TimestampNanosecond(101, {expected_timezone}) <= time AND time < TimestampNanosecond(202, {expected_timezone})");
        let actual_string = format!("{ts_predicate_expr}");

        assert_eq!(actual_string, expected_string);
    }

    #[test]
    fn test_nullable_schema_nullable() {
        // schema is all nullable
        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, true),
            Field::new("bar", DataType::Utf8, true),
        ]));

        assert_eq!(schema, nullable_schema(schema.clone()))
    }

    #[test]
    fn test_nullable_schema_non_nullable() {
        // schema has one nullable column
        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Utf8, true),
        ]));

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, true),
            Field::new("bar", DataType::Utf8, true),
        ]));

        assert_eq!(expected_schema, nullable_schema(schema))
    }

    #[tokio::test]
    async fn test_adapter_stream_panic_handling() {
        let schema = SchemaBuilder::new().timestamp().build().unwrap().as_arrow();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let tx_captured = tx.clone();
        let fut = async move {
            let _tx = tx_captured;
            if true {
                panic!("epic fail");
            }

            Ok(())
        };
        let join_handle = WatchedTask::new(fut, vec![tx], "test");
        let stream = AdapterStream::adapt(schema, rx, join_handle);
        datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap_err();
    }
}
