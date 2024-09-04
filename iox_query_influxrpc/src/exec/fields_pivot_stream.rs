use arrow::array::{Int32Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use futures::ready;
use futures::stream::Stream;
use hashbrown::HashSet;
use observability_deps::tracing::{debug, trace};
use schema::InfluxFieldType;
use snafu::Snafu;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Input missing time column"))]
    InputMissingTimeColumn,
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        Self::External(Box::new(e))
    }
}

pub(super) struct FieldsPivotStream<S> {
    input: S,

    schema: SchemaRef,
    fields: HashSet<String>,
    memory_reservation: MemoryReservation,
    metrics: BaselineMetrics,
}

impl<S> FieldsPivotStream<S> {
    pub(super) fn new(
        input: S,
        schema: SchemaRef,
        fields: HashSet<String>,
        memory_reservation: MemoryReservation,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            input,
            schema,
            fields,
            memory_reservation,
            metrics,
        }
    }
}

impl<S> RecordBatchStream for FieldsPivotStream<S>
where
    Self: Stream<Item = Result<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<S> Stream for FieldsPivotStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Send + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.fields.is_empty() {
            // There has alredy been a non-null value for every field.
            // This executor is done.
            return Poll::Ready(None);
        }

        let input = pin!(&mut this.input);

        let maybe_batch = ready!(input.poll_next(cx));
        let compute_timer = this.metrics.elapsed_compute().timer();
        match maybe_batch {
            Some(Ok(batch)) => {
                // Find any non-null values that haven't been seen before.
                let schema = batch.schema();
                trace!(?schema, "fields_pivot_stream schema");

                let time_col = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(n, f)| {
                        if matches!(f.data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, _)) {
                            Some(n)
                        } else {
                            None
                        }
                    })
                    .next()
                    .ok_or(Error::InputMissingTimeColumn)?;
                let times = as_timestamp_nanosecond_array(batch.column(time_col))?;
                trace!(?times, "time column");

                let fields = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(n, _)| *n != time_col)
                    .filter(|(_, f)| this.fields.contains(f.name()))
                    .filter(|(n, _)| {
                        let col = batch.column(*n);
                        col.null_count() < col.len()
                    })
                    .map(|(n, f)| (n, f.name().to_string(), map_data_type(f.data_type())))
                    .map(|(n, key, r#type)| {
                        let arr = batch.column(n);
                        trace!(?key, ?arr, "field");
                        let idx = (0..arr.len()).find(|i| !arr.is_null(*i)).unwrap();
                        (key, r#type, times.value(idx))
                    })
                    .inspect(|(key, r#type, time)| debug!(?key, ?r#type, ?time, "field"))
                    .collect::<Vec<_>>();

                let mut keys = StringBuilder::new();
                let mut types = Int32Builder::new();
                let mut timestamps = Int64Builder::new();

                for (key, r#type, timestamp) in fields {
                    let stored = this.fields.take(&key);
                    this.memory_reservation
                        .shrink(std::mem::size_of_val(&stored));
                    keys.append_value(&key);
                    match r#type {
                        Ok(data_type) => types.append_value(data_type),
                        Err(_) => {
                            types.append_null();
                        }
                    }
                    timestamps.append_value(timestamp);
                }
                let output = RecordBatch::try_new(
                    Arc::clone(&this.schema),
                    vec![
                        Arc::new(keys.finish()),
                        Arc::new(types.finish()),
                        Arc::new(timestamps.finish()),
                    ],
                )
                .map_err(DataFusionError::from)
                .record_output(&this.metrics)?;
                compute_timer.done();
                Poll::Ready(Some(Ok(output)))
            }
            Some(Err(err)) => {
                compute_timer.done();
                Poll::Ready(Some(Err(err)))
            }
            None => {
                // The input stream is done. There will be no more
                // non-null values for any field.
                this.fields
                    .drain()
                    .for_each(|name| this.memory_reservation.shrink(std::mem::size_of_val(&name)));
                compute_timer.done();
                this.metrics.done();
                Poll::Ready(None)
            }
        }
    }
}

/// Map a data type to the integer representation
/// used in the gRPC protocol.
fn map_data_type(dt: &DataType) -> Result<i32, <InfluxFieldType as TryFrom<DataType>>::Error> {
    InfluxFieldType::try_from(dt.clone()).map(|ft| ft as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::plan::fields_pivot_schema;

    use arrow::array::TimestampNanosecondArray;
    use datafusion::common::cast::{as_int32_array, as_int64_array};
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::{
        arrow::{
            array::{
                BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
                Int64Builder, StringArray, StringBuilder, TimestampNanosecondBuilder, UInt64Array,
                UInt64Builder,
            },
            datatypes::{Field, Schema},
        },
        common::cast::as_string_array,
    };
    use futures::stream::{pending, StreamExt};
    use schema::{TIME_DATA_TIMEZONE, TIME_DATA_TYPE};

    #[tokio::test]
    async fn exhausted_fields_returns_none() {
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let (mut stream, _) = fields_pivot_stream(pending(), Vec::<&str>::new(), metrics.clone());
        assert!(fields_pivot_schema().matches_arrow_schema(stream.schema().as_ref()));
        assert!(pin!(stream).next().await.is_none());
        assert_eq!(metrics.output_rows().value(), 0);
    }

    #[tokio::test]
    async fn empty_record_batch() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("time", TIME_DATA_TYPE(), false),
            Field::new("field1", InfluxFieldType::Float.into(), true),
            Field::new("field2", InfluxFieldType::Integer.into(), true),
            Field::new("field3", InfluxFieldType::UInteger.into(), true),
            Field::new("field4", InfluxFieldType::String.into(), true),
            Field::new("field5", InfluxFieldType::Boolean.into(), true),
        ]));
        let input = futures::stream::iter(vec![Ok(RecordBatch::new_empty(input_schema))]);
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let (mut stream, _) = fields_pivot_stream(
            input,
            vec!["field1", "field2", "field3", "field4", "field5"],
            metrics.clone(),
        );
        let batch = pin!(stream).next().await.unwrap().unwrap();
        assert!(fields_pivot_schema().matches_arrow_schema(batch.schema().as_ref()));
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(metrics.output_rows().value(), 0);
    }

    #[tokio::test]
    async fn full_record_batch() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("time", TIME_DATA_TYPE(), false),
            Field::new("field1", InfluxFieldType::Float.into(), true),
            Field::new("field2", InfluxFieldType::Integer.into(), true),
            Field::new("field3", InfluxFieldType::UInteger.into(), true),
            Field::new("field4", InfluxFieldType::String.into(), true),
            Field::new("field5", InfluxFieldType::Boolean.into(), true),
        ]));

        let time = time_array([500, 400, 300, 200, 100]);
        let field1 = float_array([Some(1.0), None, None, Some(4.0), Some(5.0)]);
        let field2 = integer_array([None, Some(2), Some(3), Some(4), Some(5)]);
        let field3 = unsigned_array([None, None, Some(3_u32), None, Some(5_u32)]);
        let field4 = string_array([None, None, None, None, Some("5")]);
        let field5 = boolean_array([None, None, None, Some(true), Some(true)]);

        let input = futures::stream::iter(vec![Ok(RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(time),
                Arc::new(field1),
                Arc::new(field2),
                Arc::new(field3),
                Arc::new(field4),
                Arc::new(field5),
            ],
        )
        .unwrap())]);
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let (mut stream, memory_pool) = fields_pivot_stream(
            input,
            ["field1", "field2", "field3", "field4", "field5"],
            metrics.clone(),
        );
        let batch = pin!(stream).next().await.unwrap().unwrap();
        assert!(fields_pivot_schema().matches_arrow_schema(batch.schema().as_ref()));
        assert_eq!(batch.num_rows(), 5);
        let fields = as_string_array(batch.column(0)).unwrap();
        let types = as_int32_array(batch.column(1)).unwrap();
        let timestamps = as_int64_array(batch.column(2)).unwrap();
        assert_eq!(
            fields
                .into_iter()
                .map(Option::unwrap)
                .map(String::from)
                .collect::<Vec<_>>(),
            vec!["field1", "field2", "field3", "field4", "field5"],
        );
        assert_eq!(
            types.into_iter().map(Option::unwrap).collect::<Vec<_>>(),
            vec![
                InfluxFieldType::Float as i32,
                InfluxFieldType::Integer as i32,
                InfluxFieldType::UInteger as i32,
                InfluxFieldType::String as i32,
                InfluxFieldType::Boolean as i32
            ],
        );
        assert_eq!(
            timestamps
                .into_iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>(),
            vec![500, 400, 300, 100, 200],
        );
        assert_eq!(memory_pool.reserved(), 0);
        assert_eq!(metrics.output_rows().value(), 5);
    }

    #[tokio::test]
    async fn ignore_already_found() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("time", TIME_DATA_TYPE(), false),
            Field::new("field1", InfluxFieldType::Float.into(), true),
            Field::new("field2", InfluxFieldType::Integer.into(), true),
            Field::new("field3", InfluxFieldType::UInteger.into(), true),
            Field::new("field4", InfluxFieldType::String.into(), true),
            Field::new("field5", InfluxFieldType::Boolean.into(), true),
        ]));

        let time = time_array([1500, 1400, 1300, 1200, 1100]);
        let time_2 = time_array([500, 400, 300, 200, 100]);
        let field1 = Arc::new(float_array([Some(1.0), None, None, Some(4.0), Some(5.0)])) as _;
        let field2 = Arc::new(integer_array([None, Some(2), Some(3), Some(4), Some(5)])) as _;
        let field3 = Arc::new(unsigned_array([None, None, Some(3_u32), None, Some(5_u32)])) as _;
        let field4 = string_array([Option::<&str>::None, None, None, None, None]);
        let field4_2 = string_array([None, None, None, None, Some("5")]);
        let field5 = boolean_array([Option::<bool>::None, None, None, None, None]);
        let field5_2 = boolean_array([None, None, None, Some(true), None]);

        let input = futures::stream::iter(vec![
            Ok(RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![
                    Arc::new(time),
                    Arc::clone(&field1),
                    Arc::clone(&field2),
                    Arc::clone(&field3),
                    Arc::new(field4),
                    Arc::new(field5),
                ],
            )
            .unwrap()),
            Ok(RecordBatch::try_new(
                input_schema,
                vec![
                    Arc::new(time_2),
                    Arc::clone(&field1),
                    Arc::clone(&field2),
                    Arc::clone(&field3),
                    Arc::new(field4_2),
                    Arc::new(field5_2),
                ],
            )
            .unwrap()),
        ]);
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let (mut stream, memory_pool) = fields_pivot_stream(
            input,
            ["field1", "field2", "field3", "field4", "field5"],
            metrics.clone(),
        );
        let mut stream = pin!(stream);
        let batch = stream.next().await.unwrap().unwrap();
        assert!(fields_pivot_schema().matches_arrow_schema(batch.schema().as_ref()));
        assert_eq!(batch.num_rows(), 3);
        let fields = as_string_array(batch.column(0)).unwrap();
        let types = as_int32_array(batch.column(1)).unwrap();
        let timestamps = as_int64_array(batch.column(2)).unwrap();
        assert_eq!(
            fields
                .into_iter()
                .map(Option::unwrap)
                .map(String::from)
                .collect::<Vec<_>>(),
            vec!["field1", "field2", "field3"],
        );
        assert_eq!(
            types.into_iter().map(Option::unwrap).collect::<Vec<_>>(),
            vec![
                InfluxFieldType::Float as i32,
                InfluxFieldType::Integer as i32,
                InfluxFieldType::UInteger as i32,
            ],
        );
        assert_eq!(
            timestamps
                .into_iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>(),
            vec![1500, 1400, 1300],
        );

        let batch = stream.next().await.unwrap().unwrap();
        assert!(fields_pivot_schema().matches_arrow_schema(batch.schema().as_ref()));
        assert_eq!(batch.num_rows(), 2);
        let fields = as_string_array(batch.column(0)).unwrap();
        let types = as_int32_array(batch.column(1)).unwrap();
        let timestamps = as_int64_array(batch.column(2)).unwrap();
        assert_eq!(
            fields
                .into_iter()
                .map(Option::unwrap)
                .map(String::from)
                .collect::<Vec<_>>(),
            vec!["field4", "field5"],
        );
        assert_eq!(
            types.into_iter().map(Option::unwrap).collect::<Vec<_>>(),
            vec![
                InfluxFieldType::String as i32,
                InfluxFieldType::Boolean as i32,
            ],
        );
        assert_eq!(
            timestamps
                .into_iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>(),
            vec![100, 200],
        );

        assert_eq!(memory_pool.reserved(), 0);
        assert_eq!(metrics.output_rows().value(), 5);
    }

    #[tokio::test]
    async fn empty_result_without_match() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("time", TIME_DATA_TYPE(), false),
            Field::new("field1", InfluxFieldType::Float.into(), true),
            Field::new("field2", InfluxFieldType::Integer.into(), true),
            Field::new("field3", InfluxFieldType::UInteger.into(), true),
            Field::new("field4", InfluxFieldType::String.into(), true),
            Field::new("field5", InfluxFieldType::Boolean.into(), true),
        ]));

        let time = time_array([500, 400, 300, 200, 100]);
        let field1 = float_array([Some(1.0), None, None, Some(4.0), Some(5.0)]);
        let field2 = integer_array([None, Some(2), Some(3), Some(4), Some(5)]);
        let field3 = unsigned_array([None, None, Some(3_u32), None, Some(5_u32)]);
        let field4 = string_array([None, None, None, None, Some("5")]);
        let field5 = boolean_array([None, None, None, Some(true), Some(true)]);

        let input = futures::stream::iter(vec![Ok(RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(time),
                Arc::new(field1),
                Arc::new(field2),
                Arc::new(field3),
                Arc::new(field4),
                Arc::new(field5),
            ],
        )
        .unwrap())]);
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let (mut stream, _) = fields_pivot_stream(input, ["field6", "field7"], metrics.clone());
        let batch = pin!(stream).next().await.unwrap().unwrap();
        assert!(fields_pivot_schema().matches_arrow_schema(batch.schema().as_ref()));
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(metrics.output_rows().value(), 0);
    }

    fn fields_pivot_stream<S, T>(
        input: S,
        fields: T,
        metrics: BaselineMetrics,
    ) -> (FieldsPivotStream<S>, Arc<dyn MemoryPool>)
    where
        S: Stream<Item = Result<RecordBatch>> + Send + Unpin,
        T: IntoIterator,
        T::Item: Into<String>,
    {
        let memory_pool = Arc::new(UnboundedMemoryPool::default()) as _;
        let memory_consumer = MemoryConsumer::new("FieldsPivotStream test");
        let mut memory_reservation = memory_consumer.register(&memory_pool);

        let schema = Arc::new(fields_pivot_schema().as_ref().into());
        let fields = fields
            .into_iter()
            .map(|v| v.into())
            .inspect(|v| memory_reservation.grow(std::mem::size_of_val(v)))
            .collect();
        let stream = FieldsPivotStream::new(input, schema, fields, memory_reservation, metrics);
        (stream, memory_pool)
    }

    fn time_array<I>(vals: I) -> TimestampNanosecondArray
    where
        I: IntoIterator,
        I::Item: Into<i64>,
    {
        let mut builder = TimestampNanosecondBuilder::new().with_timezone_opt(TIME_DATA_TIMEZONE());
        for v in vals {
            builder.append_value(v.into());
        }
        builder.finish()
    }

    fn float_array<I, T>(vals: I) -> Float64Array
    where
        I: IntoIterator<Item = Option<T>>,
        T: Into<f64>,
    {
        let mut builder = Float64Builder::new();
        for v in vals {
            if let Some(v) = v {
                builder.append_value(v.into());
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    fn integer_array<I, T>(vals: I) -> Int64Array
    where
        I: IntoIterator<Item = Option<T>>,
        T: Into<i64>,
    {
        let mut builder = Int64Builder::new();
        for v in vals {
            if let Some(v) = v {
                builder.append_value(v.into());
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    fn unsigned_array<I, T>(vals: I) -> UInt64Array
    where
        I: IntoIterator<Item = Option<T>>,
        T: Into<u64>,
    {
        let mut builder = UInt64Builder::new();
        for v in vals {
            if let Some(v) = v {
                builder.append_value(v.into());
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    fn string_array<I, T>(vals: I) -> StringArray
    where
        I: IntoIterator<Item = Option<T>>,
        T: Into<String>,
    {
        let mut builder = StringBuilder::new();
        for v in vals {
            if let Some(v) = v {
                builder.append_value(v.into());
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    fn boolean_array<I, T>(vals: I) -> BooleanArray
    where
        I: IntoIterator<Item = Option<T>>,
        T: Into<bool>,
    {
        let mut builder = BooleanBuilder::new();
        for v in vals {
            if let Some(v) = v {
                builder.append_value(v.into());
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }
}
