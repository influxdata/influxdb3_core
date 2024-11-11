use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_util::watch::WatchedTask;
use datafusion_util::AdapterStream;
use observability_deps::tracing::debug;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

/// Physical operator that implements the SchemaPivot operation against
/// data types
pub(crate) struct SchemaPivotExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl SchemaPivotExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));
        Self {
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        use Partitioning::*;
        let output_partitioning = match input.output_partitioning() {
            RoundRobinBatch(num_partitions) => RoundRobinBatch(*num_partitions),
            // as this node transforms the output schema,  whatever partitioning
            // was present on the input is lost on the output
            Hash(_, num_partitions) => UnknownPartitioning(*num_partitions),
            UnknownPartitioning(num_partitions) => UnknownPartitioning(*num_partitions),
        };

        PlanProperties::new(eq_properties, output_partitioning, input.execution_mode())
    }
}

impl fmt::Debug for SchemaPivotExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaPivotExec")
    }
}

impl ExecutionPlan for SchemaPivotExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                Arc::clone(&self.schema),
            ))),
            _ => Err(DataFusionError::Internal(
                "SchemaPivotExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(partition, "Start SchemaPivotExec::execute");

        if self.properties().output_partitioning().partition_count() <= partition {
            return Err(DataFusionError::Internal(format!(
                "SchemaPivotExec invalid partition {partition}"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input_schema = self.input.schema();
        let input_stream = self.input.execute(partition, context)?;

        // the operation performed in a separate task which is
        // then sent via a channel to the output
        let (tx, rx) = mpsc::channel(1);

        let fut = schema_pivot(
            input_stream,
            input_schema,
            self.schema(),
            tx.clone(),
            baseline_metrics,
        );

        // A second task watches the output of the worker task and reports errors
        let handle = WatchedTask::new(fut, vec![tx], "schema_pivot");

        debug!(partition, "End SchemaPivotExec::execute");
        Ok(AdapterStream::adapt(self.schema(), rx, handle))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for SchemaPivotExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaPivotExec")
            }
        }
    }
}

// Algorithm: for each column we haven't seen a value for yet,
// check each input row;
//
// Performance Optimizations: Don't continue scaning columns
// if we have already seen a non-null value, and stop early we
// have seen values for all columns.
async fn schema_pivot(
    mut input_stream: SendableRecordBatchStream,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    baseline_metrics: BaselineMetrics,
) -> Result<(), DataFusionError> {
    let input_fields = input_schema.fields();
    let num_fields = input_fields.len();
    let mut field_indexes_with_seen_values = vec![false; num_fields];
    let mut num_fields_seen_with_values = 0;

    // use a loop so that we release the mutex once we have read each input_batch
    let mut keep_searching = true;
    while keep_searching {
        let input_batch = input_stream.next().await.transpose()?;
        let timer = baseline_metrics.elapsed_compute().timer();

        keep_searching = match input_batch {
            Some(input_batch) => {
                let num_rows = input_batch.num_rows();

                for (i, seen_value) in field_indexes_with_seen_values.iter_mut().enumerate() {
                    // only check fields we haven't seen values for
                    if !*seen_value {
                        let column = input_batch.column(i);

                        let field_has_values = !column.is_empty() && column.null_count() < num_rows;

                        if field_has_values {
                            *seen_value = true;
                            num_fields_seen_with_values += 1;
                        }
                    }
                }
                // need to keep searching if there are still some
                // fields without values
                num_fields_seen_with_values < num_fields
            }
            // no more input
            None => false,
        };
        timer.done();
    }

    // now, output a string for each column in the input schema
    // that we saw values for
    let column_names: StringArray = field_indexes_with_seen_values
        .iter()
        .enumerate()
        .filter_map(|(field_index, has_values)| {
            if *has_values {
                Some(input_fields[field_index].name())
            } else {
                None
            }
        })
        .map(Some)
        .collect();

    let batch = RecordBatch::try_new(output_schema, vec![Arc::new(column_names)])?
        .record_output(&baseline_metrics);

    // and send the result back
    tx.send(Ok(batch))
        .await
        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::schema_pivot_schema;
    use arrow::{
        array::{as_dictionary_array, as_string_array, Int64Array, StringArray},
        datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    };
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion_util::test_execute_partition;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn schema_pivot_exec_all_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[None, None],
                b: &[None, None],
            }],
            expected_output: &[],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {case:?}"
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_both_non_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[Some(1), None],
                b: &[None, Some("foo")],
            }],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {case:?}"
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_one_non_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[Some(1), None],
                b: &[None, None],
            }],
            expected_output: &["A"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {case:?}"
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_both_non_null_two_record_batches() {
        let case = SchemaTestCase {
            input_batches: &[
                TestBatch {
                    a: &[Some(1), None],
                    b: &[None, None],
                },
                TestBatch {
                    a: &[None, None],
                    b: &[None, Some("foo")],
                },
            ],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {case:?}"
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_one_non_null_in_second_record_batch() {
        let case = SchemaTestCase {
            input_batches: &[
                TestBatch {
                    a: &[None, None],
                    b: &[None, None],
                },
                TestBatch {
                    a: &[None, Some(1), None],
                    b: &[None, Some("foo"), None],
                },
            ],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {case:?}"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "SchemaPivotExec invalid partition 1")]
    async fn schema_pivot_exec_bad_partition() {
        // ensure passing in a bad partition generates a reasonable error

        let pivot = make_schema_pivot(SchemaTestCase::input_schema(), vec![]);

        test_execute_partition(pivot, 1).await;
    }

    /// Return a StringSet extracted from the record batch
    async fn reader_to_stringset(mut reader: SendableRecordBatchStream) -> BTreeSet<String> {
        let mut strings = BTreeSet::default();

        // process the record batches one by one
        while let Some(record_batch) = reader.next().await.transpose().expect("reading next batch")
        {
            assert_eq!(record_batch.num_columns(), 1);
            let arr = record_batch.column(0);
            match arr.data_type() {
                DataType::Utf8 => {
                    let arr = as_string_array(arr);
                    strings.extend(arr.iter().filter_map(|s| s.map(|s| s.to_string())));
                }
                DataType::Dictionary(key, value)
                    if key == &Box::new(DataType::Int32) && value == &Box::new(DataType::Utf8) =>
                {
                    let arr = as_dictionary_array::<Int32Type>(arr);
                    let values = as_string_array(arr.values());
                    strings.extend(
                        arr.keys_iter()
                            .filter_map(|idx| idx.map(|idx| values.value(idx).to_string())),
                    )
                }
                _ => panic!("Expected string"),
            }
        }
        strings
    }

    /// return a set for testing
    fn to_stringset(strs: &[&str]) -> BTreeSet<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    /// Create a schema pivot node with a single input
    fn make_schema_pivot(
        input_schema: SchemaRef,
        data: Vec<RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        let input = make_memory_exec(input_schema, data);
        let output_schema = Arc::new(schema_pivot_schema().as_ref().clone().into());
        Arc::new(SchemaPivotExec::new(input, output_schema))
    }

    /// Create an ExecutionPlan that produces `data` record batches.
    fn make_memory_exec(schema: SchemaRef, data: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let partitions = vec![data]; // single partition
        let projection = None;

        let memory_exec =
            MemoryExec::try_new(&partitions, schema, projection).expect("creating memory exec");

        Arc::new(memory_exec)
    }

    fn to_string_array(strs: &[Option<&str>]) -> Arc<StringArray> {
        let arr: StringArray = strs.iter().collect();
        Arc::new(arr)
    }

    // Input schema is (A INT, B STRING)
    #[derive(Debug)]
    struct TestBatch<'a> {
        a: &'a [Option<i64>],
        b: &'a [Option<&'a str>],
    }

    // Input schema is (A INT, B STRING)
    #[derive(Debug)]
    struct SchemaTestCase<'a> {
        // Input record batches, slices of slices (a,b)
        input_batches: &'a [TestBatch<'a>],
        expected_output: &'a [&'a str],
    }

    impl SchemaTestCase<'_> {
        fn input_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("A", DataType::Int64, true),
                Field::new("B", DataType::Utf8, true),
            ]))
        }

        /// return expected output, as StringSet
        fn expected_output(&self) -> BTreeSet<String> {
            to_stringset(self.expected_output)
        }

        /// run the input batches through a schema pivot and return the results
        /// as a BTreeSet
        async fn pivot(&self) -> BTreeSet<String> {
            let schema = Self::input_schema();

            // prepare input
            let input_batches = self
                .input_batches
                .iter()
                .map(|test_batch| {
                    let a_vec = test_batch.a.to_vec();
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(a_vec)),
                            to_string_array(test_batch.b),
                        ],
                    )
                    .expect("Creating new record batch")
                })
                .collect::<Vec<_>>();

            let pivot = make_schema_pivot(schema, input_batches);

            let results = test_execute_partition(pivot, 0).await;

            reader_to_stringset(results).await
        }
    }
}
