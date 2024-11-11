use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StringDictionaryBuilder, UInt64Array, UnionArray,
};
use arrow::compute::filter;
use arrow::datatypes::{DataType, Int32Type, SchemaRef, UnionMode};
use datafusion::common::cast::as_string_array;
use datafusion::common::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::RecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use datafusion::physical_plan::PhysicalExpr;
use futures::ready;
use futures::stream::Stream;
use std::iter::repeat;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub(super) struct FieldExpr {
    expr: Arc<dyn PhysicalExpr>,
    time_expr: Arc<dyn PhysicalExpr>,
    key: Arc<str>,
}

impl FieldExpr {
    pub(super) fn new(
        expr: Arc<dyn PhysicalExpr>,
        time_expr: Arc<dyn PhysicalExpr>,
        key: Arc<str>,
    ) -> Self {
        Self {
            expr,
            time_expr,
            key,
        }
    }
}

pub(super) struct SeriesPivotStream<S> {
    input: S,
    schema: SchemaRef,

    measurement_expr: Arc<dyn PhysicalExpr>,
    tag_exprs: Vec<Arc<dyn PhysicalExpr>>,
    field_exprs: Vec<FieldExpr>,

    memory_reservation: MemoryReservation,
    metrics: BaselineMetrics,
    batch: Option<(RecordBatch, ArrayRef, Vec<ArrayRef>)>,
    field: usize,
}

impl<S> SeriesPivotStream<S> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        input: S,
        schema: SchemaRef,
        measurement_expr: Arc<dyn PhysicalExpr>,
        tag_exprs: Vec<Arc<dyn PhysicalExpr>>,
        field_exprs: Vec<FieldExpr>,
        memory_pool: &Arc<dyn MemoryPool>,
        metrics: BaselineMetrics,
    ) -> Self {
        let memory_consumer = MemoryConsumer::new("SeriesPivotStream");
        let memory_reservation = memory_consumer.register(memory_pool);
        Self {
            input,
            schema,
            measurement_expr,
            tag_exprs,
            field_exprs,
            memory_reservation,
            metrics,
            batch: None,
            field: 0,
        }
    }

    fn pivot(
        &self,
        key: &Arc<str>,
        expr: &Arc<dyn PhysicalExpr>,
        time_expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<RecordBatch> {
        let (batch, measurements, tags) = self.batch.as_ref().unwrap();
        let num_rows = batch.num_rows();

        let mut values = expr.evaluate(batch)?.into_array(num_rows)?;

        if values.null_count() == num_rows {
            // All values are null, skip this field.
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let mut measurements = Arc::clone(measurements);
        let mut tags = tags.iter().map(Arc::clone).collect::<Vec<_>>();
        let mut times = time_expr.evaluate(batch)?.into_array(num_rows)?;

        if values.null_count() > 0 {
            // If there are null values, we need to filter them out.
            let mask = values.nulls().unwrap().clone().into_inner();
            measurements = filter(&measurements, &BooleanArray::from(mask.clone()))?;
            tags = tags
                .iter()
                .map(|tag| Ok(filter(tag, &BooleanArray::from(mask.clone()))?))
                .collect::<Result<_>>()?;
            times = filter(&times, &BooleanArray::from(mask.clone()))?;
            values = filter(&values, &BooleanArray::from(mask))?;
        }

        let num_rows = values.len();
        let field_values = StringArray::from(vec![key.as_ref()]);
        let field_keys = repeat(0_i32).take(num_rows).collect::<Int32Array>();
        let fields = Arc::new(DictionaryArray::new(field_keys, Arc::new(field_values)));

        let mut columns = Vec::with_capacity(tags.len() + 4);
        columns.push(measurements);
        columns.extend(tags);
        columns.push(fields);
        columns.push(times);
        columns.push(self.as_union(&values)?);

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    fn measurements(&self, batch: &RecordBatch) -> Result<Arc<dyn Array>> {
        match self.measurement_expr.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let array = as_string_array(&array)?;
                let mut builder = StringDictionaryBuilder::<Int32Type>::new();
                for idx in 0..array.len() {
                    builder.append_value(array.value(idx));
                }
                Ok(Arc::new(builder.finish()))
            }
            ColumnarValue::Scalar(scalar) => {
                let keys = repeat(0_i32).take(batch.num_rows()).collect();
                let values = scalar.to_array()?;
                Ok(Arc::new(DictionaryArray::new(keys, values)))
            }
        }
    }

    fn tags(&self, batch: &RecordBatch) -> Result<Vec<Arc<dyn Array>>> {
        self.tag_exprs
            .iter()
            .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
            .collect()
    }

    fn as_union(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let num_rows = array.len();

        let type_id = match array.data_type() {
            DataType::Float64 => 0_i8,
            DataType::Int64 => 1,
            DataType::UInt64 => 2,
            DataType::Boolean => 3,
            DataType::Utf8 => 4,
            _ => unreachable!("unsupported data type {}", array.data_type()),
        };

        let mut children: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::new_null(num_rows)),
            Arc::new(Int64Array::new_null(num_rows)),
            Arc::new(UInt64Array::new_null(num_rows)),
            Arc::new(BooleanArray::new_null(num_rows)),
            Arc::new(StringArray::new_null(num_rows)),
        ];
        children[type_id as usize] = Arc::clone(array);

        let column = self.schema.fields().iter().last().unwrap();

        if let DataType::Union(union_fields, UnionMode::Sparse) = column.data_type() {
            Ok(Arc::new(UnionArray::try_new(
                union_fields.clone(),
                repeat(type_id).take(num_rows).collect(),
                None,
                children,
            )?))
        } else {
            unreachable!("values field is not a union")
        }
    }
}

impl<S> RecordBatchStream for SeriesPivotStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<S> Stream for SeriesPivotStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.field >= this.field_exprs.len() {
                if let Some((batch, measurements, tags)) = this.batch.take() {
                    this.memory_reservation.shrink(
                        batch.get_array_memory_size()
                            + measurements.get_array_memory_size()
                            + tags
                                .iter()
                                .map(|tag| tag.get_array_memory_size())
                                .sum::<usize>(),
                    );
                }
                this.field = 0;
            }
            if this.batch.is_none() {
                // Need to fetch a new batch from upstream.
                match ready!(Pin::new(&mut this.input).poll_next(cx)) {
                    Some(Ok(batch)) => {
                        let measurements = this.measurements(&batch)?;
                        let tags = this.tags(&batch)?;
                        this.memory_reservation.try_grow(
                            batch.get_array_memory_size()
                                + measurements.get_array_memory_size()
                                + tags
                                    .iter()
                                    .map(|tag| tag.get_array_memory_size())
                                    .sum::<usize>(),
                        )?;
                        this.batch = Some((batch, measurements, tags));
                        continue;
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    None => return Poll::Ready(None),
                }
            }

            // If we get this far we must have something to do.
            let timer = this.metrics.elapsed_compute().timer();
            let FieldExpr {
                expr,
                time_expr,
                key,
            } = &this.field_exprs[this.field];
            this.field += 1;
            let result = this.pivot(key, expr, time_expr);
            timer.done();
            if let Ok(batch) = &result {
                // Skip empty batches.
                if batch.num_rows() == 0 {
                    continue;
                }
                observability_deps::tracing::trace!(?batch, "SeriesPivotExec producing batch");
            }
            return Poll::Ready(Some(result.record_output(&this.metrics)));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::SeriesSchema;

    use super::*;
    use arrow_util::test_util::batches_to_lines;
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use datafusion::physical_plan::expressions::{Column, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use futures::stream::{once, TryStreamExt};
    use insta::assert_snapshot;
    use schema::TIME_DATA_TIMEZONE;

    #[tokio::test]
    async fn series_pivot_stream() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, TIME_DATA_TIMEZONE()),
                false,
            ),
            Field::new(
                "tag1",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "tag2",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("field1", DataType::Float64, true),
            Field::new("field2", DataType::Float64, true),
        ]));

        let times = TimestampNanosecondArray::from(vec![100_i64, 200, 300, 400, 500])
            .with_timezone_opt(TIME_DATA_TIMEZONE());
        let tag1_keys = Int32Array::from(vec![0, 1, 0, 1, 0]);
        let tag1_values = StringArray::from(vec!["A", "B"]);
        let tag1 = DictionaryArray::new(tag1_keys, Arc::new(tag1_values));
        let tag2_keys = Int32Array::from(vec![0, 0, 0, 1, 1]);
        let tag2_values = StringArray::from(vec!["X", "Y"]);
        let tag2 = DictionaryArray::new(tag2_keys, Arc::new(tag2_values));
        let field1 = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let field2 = Float64Array::from(vec![Some(10.0), Some(20.0), None, None, Some(50.0)]);

        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(times),
                Arc::new(tag1),
                Arc::new(tag2),
                Arc::new(field1),
                Arc::new(field2),
            ],
        )
        .unwrap();

        let measurement_expr =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("test_m".to_string()))));
        let tag_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("tag1", 1)),
            Arc::new(Column::new("tag2", 2)),
        ];
        let time_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("time", 0));

        let field_exprs: Vec<FieldExpr> = vec![
            FieldExpr {
                expr: Arc::new(Column::new("field1", 3)),
                time_expr: Arc::clone(&time_expr),
                key: "field1".into(),
            },
            FieldExpr {
                expr: Arc::new(Column::new("field2", 4)),
                time_expr,
                key: "field2".into(),
            },
        ];

        let output_schema = SeriesSchema::new_tags([
            Field::new(
                "tag1",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "tag2",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let metrics = ExecutionPlanMetricsSet::new();

        let stream = SeriesPivotStream::new(
            Box::pin(once(async move { Result::Ok(batch) })),
            output_schema.into(),
            measurement_expr,
            tag_exprs,
            field_exprs,
            &memory_pool,
            BaselineMetrics::new(&metrics, 0),
        );

        let results = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(results.len(), 2);

        assert_snapshot!(batches_to_lines(&results).join("\n"), @r"
        +--------------+------+------+--------+--------------------------------+--------------+
        | _measurement | tag1 | tag2 | _field | _time                          | _value       |
        +--------------+------+------+--------+--------------------------------+--------------+
        | test_m       | A    | X    | field1 | 1970-01-01T00:00:00.000000100Z | {float=1.0}  |
        | test_m       | B    | X    | field1 | 1970-01-01T00:00:00.000000200Z | {float=2.0}  |
        | test_m       | A    | X    | field1 | 1970-01-01T00:00:00.000000300Z | {float=3.0}  |
        | test_m       | B    | Y    | field1 | 1970-01-01T00:00:00.000000400Z | {float=4.0}  |
        | test_m       | A    | Y    | field1 | 1970-01-01T00:00:00.000000500Z | {float=5.0}  |
        | test_m       | A    | X    | field2 | 1970-01-01T00:00:00.000000100Z | {float=10.0} |
        | test_m       | B    | X    | field2 | 1970-01-01T00:00:00.000000200Z | {float=20.0} |
        | test_m       | A    | Y    | field2 | 1970-01-01T00:00:00.000000500Z | {float=50.0} |
        +--------------+------+------+--------+--------------------------------+--------------+
        ");

        assert_eq!(memory_pool.reserved(), 0);
    }
}
