//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
pub mod gapfill;
mod metrics;
pub mod query_tracing;
mod schema_pivot;
pub mod sleep;
pub(crate) mod split;
use datafusion_util::config::register_iox_object_store;
pub use executor::DedicatedExecutor;
use metric::Registry;
use object_store::DynObjectStore;
use parquet_file::storage::StorageId;
mod cross_rt_stream;

use std::{collections::HashMap, fmt::Display, num::NonZeroUsize, sync::Arc};

use datafusion::{
    self,
    error::DataFusionError,
    execution::{
        disk_manager::DiskManagerConfig,
        memory_pool::MemoryPool,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    logical_expr::{
        expr_rewriter::normalize_col, Expr, Extension, LogicalPlan, LogicalPlanBuilder,
    },
};

pub use context::{
    IOxSessionConfig, IOxSessionContext, QueryConfig, QueryLanguage, SessionContextIOxExt,
};
use schema_pivot::SchemaPivotNode;

use crate::exec::metrics::DataFusionMemoryPoolMetricsBridge;

use self::split::StreamSplitNode;

const TESTING_MEM_POOL_SIZE: usize = 1024 * 1024 * 1024; // 1GB

/// Configuration for an Executor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Target parallelism for query execution
    pub target_query_partitions: NonZeroUsize,

    /// Object stores
    pub object_stores: HashMap<StorageId, Arc<DynObjectStore>>,

    /// Metric registry
    pub metric_registry: Arc<Registry>,

    /// Memory pool size in bytes.
    pub mem_pool_size: usize,
}

impl ExecutorConfig {
    pub fn testing() -> Self {
        Self {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: HashMap::default(),
            metric_registry: Arc::new(Registry::default()),
            mem_pool_size: TESTING_MEM_POOL_SIZE,
        }
    }
}

impl Display for ExecutorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "target_query_partitions={}, mem_pool_size={}",
            self.target_query_partitions, self.mem_pool_size
        )
    }
}

/// Handles executing DataFusion plans, and marshalling the results into rust
/// native structures.
#[derive(Debug)]
pub struct Executor {
    /// Executor
    executor: DedicatedExecutor,

    /// The default configuration options with which to create contexts
    config: ExecutorConfig,

    /// The DataFusion [RuntimeEnv] (including memory manager and disk
    /// manager) used for all executions
    runtime: Arc<RuntimeEnv>,
}

impl Display for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Executor({})", self.config)
    }
}

impl Executor {
    /// Get testing executor that runs a on single thread and a low memory bound
    /// to preserve resources.
    pub fn new_testing() -> Self {
        let config = ExecutorConfig::testing();
        let executor = DedicatedExecutor::new_testing();
        Self::new_with_config_and_executor(config, executor)
    }

    /// Low-level constructor.
    ///
    /// This is mostly useful if you wanna keep the executor (because they are quite expensive to create) but need a fresh IOx runtime.
    ///
    /// # Panic
    /// Panics if the number of threads in `executor` is different from `config`.
    pub fn new_with_config_and_executor(
        config: ExecutorConfig,
        executor: DedicatedExecutor,
    ) -> Self {
        let runtime_config = RuntimeConfig::new()
            .with_disk_manager(DiskManagerConfig::Disabled)
            .with_memory_limit(config.mem_pool_size, 1.0);

        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("creating runtime"));
        for (id, store) in &config.object_stores {
            register_iox_object_store(&runtime, id, Arc::clone(store));
        }

        // As there should only be a single memory pool for any executor,
        // verify that there was no existing instrument registered (for another pool)
        let mut created = false;
        let created_captured = &mut created;
        let bridge =
            DataFusionMemoryPoolMetricsBridge::new(&runtime.memory_pool, config.mem_pool_size);
        let bridge_ctor = move || {
            *created_captured = true;
            bridge
        };
        config
            .metric_registry
            .register_instrument("datafusion_pool", bridge_ctor);
        assert!(
            created,
            "More than one execution pool created: previously existing instrument"
        );

        Self {
            executor,
            config,
            runtime,
        }
    }

    /// Return a new ession config, suitable for executing a new query or system task.
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_session_config(&self) -> IOxSessionConfig {
        IOxSessionConfig::new(self.executor.clone(), Arc::clone(&self.runtime))
            .with_target_partitions(self.config.target_query_partitions)
    }

    /// Create a new execution context, suitable for executing a new query or system task
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_context(&self) -> IOxSessionContext {
        self.new_session_config().build()
    }

    /// Initializes shutdown.
    pub fn shutdown(&self) {
        self.executor.shutdown();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all created contexts.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    pub async fn join(&self) {
        self.executor.join().await;
    }

    /// Returns the memory pool associated with this `Executor`
    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.runtime.memory_pool)
    }

    /// Returns the runtime associated with this `Executor`
    pub fn runtime(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime)
    }

    /// Returns underlying config.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Returns the underlying [`DedicatedExecutor`].
    pub fn executor(&self) -> &DedicatedExecutor {
        &self.executor
    }
}

// No need to implement `Drop` because this is done by DedicatedExecutor already

/// Create a SchemaPivot node which takes an arbitrary input like
///  ColA | ColB | ColC
/// ------+------+------
///   1   | NULL | NULL
///   2   | 2    | NULL
///   3   | 2    | NULL
///
/// And pivots it to a table with a single string column for any
/// columns that had non null values.
///
///   non_null_column
///  -----------------
///   "ColA"
///   "ColB"
pub fn make_schema_pivot(input: LogicalPlan) -> LogicalPlan {
    let node = Arc::new(SchemaPivotNode::new(input));

    LogicalPlan::Extension(Extension { node })
}

/// Attach a SchemaPivot node to a builder. A SchemaPivot
/// node takes an arbitrary input like
///  ColA | ColB | ColC
/// ------+------+------
///   1   | NULL | NULL
///   2   | 2    | NULL
///   3   | 2    | NULL
///
/// And pivots it to a table with a single string column for any
/// columns that had non null values.
///
///   non_null_column
///  -----------------
///   "ColA"
///   "ColB"
pub fn build_schema_pivot(
    builder: LogicalPlanBuilder,
) -> Result<LogicalPlanBuilder, DataFusionError> {
    let input = builder.build()?;
    Ok(LogicalPlanBuilder::from(make_schema_pivot(input)))
}

/// Create a StreamSplit node which takes an input stream of record
/// batches and produces multiple output streams based on  a list of `N` predicates.
/// The output will have `N+1` streams, and each row is sent to the stream
/// corresponding to the first predicate that evaluates to true, or the last stream if none do.
///
/// For example, if the input looks like:
/// ```text
///  X | time
/// ---+-----
///  a | 1000
///  b | 4000
///  c | 2000
/// ```
///
/// A StreamSplit with split_exprs = [`time <= 1000`, `1000 < time <=2000`] will produce the
/// following three output streams (output DataFusion Partitions):
///
///
/// ```text
///  X | time
/// ---+-----
///  a | 1000
/// ```
///
/// ```text
///  X | time
/// ---+-----
///  b | 2000
/// ```
/// and
/// ```text
///  X | time
/// ---+-----
///  b | 4000
/// ```
pub fn make_stream_split(input: LogicalPlan, split_exprs: Vec<Expr>) -> LogicalPlan {
    // rewrite the input expression so that it is fully qualified with the input schema
    let split_exprs = split_exprs
        .into_iter()
        .map(|split_expr| normalize_col(split_expr, &input).expect("normalize is infallable"))
        .collect::<Vec<_>>();

    let node = Arc::new(StreamSplitNode::new(input, split_exprs));
    LogicalPlan::Extension(Extension { node })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, SchemaRef},
    };
    use datafusion::{
        datasource::{provider_as_source, MemTable},
        error::DataFusionError,
        logical_expr::LogicalPlanBuilder,
        physical_expr::{EquivalenceProperties, PhysicalSortExpr},
        physical_plan::{
            expressions::Column, sorts::sort::SortExec, DisplayAs, ExecutionMode, ExecutionPlan,
            PlanProperties, RecordBatchStream,
        },
    };
    use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
    use metric::{Observation, RawReporter};

    use tokio::sync::Barrier;

    use super::*;
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn make_schema_pivot_is_planned() {
        // Test that all the planning logic is wired up and that we
        // can make a plan using a SchemaPivot node
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("f1", to_string_array(&["foo", "bar"]), true),
            ("f2", to_string_array(&["baz", "bzz"]), true),
        ])
        .expect("created new record batch");

        let scan = make_plan(batch.schema(), vec![batch]);
        let pivot = make_schema_pivot(scan);

        let exec = Executor::new_testing();
        let ctx = exec.new_context();
        let physical_plan = ctx
            .create_physical_plan(&pivot)
            .await
            .expect("Created physical plan");
        let mut stream = ctx
            .execute_stream(physical_plan)
            .await
            .expect("Executed plan");

        let mut results = BTreeSet::<String>::default();
        while let Some(batch) = stream.try_next().await.expect("Got next batch") {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Cast to string array");
            results.extend(arr.into_iter().map(|x| String::from(x.expect("No nulls"))));
        }

        assert_eq!(results, to_set(&["f1", "f2"]));
    }

    #[tokio::test]
    async fn test_metrics_integration() {
        let exec = Executor::new_testing();

        // start w/o any reservation
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );

        // block some reservation
        let test_input = Arc::new(TestExec::default());
        let schema = test_input.schema();
        let plan = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema("c", &schema).unwrap()),
                options: Default::default(),
            }],
            Arc::clone(&test_input) as _,
        ));
        let ctx = exec.new_context();
        let handle = tokio::spawn(async move {
            ctx.collect(plan).await.unwrap();
        });
        test_input.wait().await;
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 896,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );
        test_input.wait_for_finish().await;

        // end w/o any reservation
        handle.await.unwrap();
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> BTreeSet<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    // creates a DataFusion plan that reads the RecordBatches into memory
    fn make_plan(schema: SchemaRef, data: Vec<RecordBatch>) -> LogicalPlan {
        let partitions = vec![data];

        let projection = None;

        // model one partition,
        let table = MemTable::try_new(schema, partitions).unwrap();
        let source = provider_as_source(Arc::new(table));

        LogicalPlanBuilder::scan("memtable", source, projection)
            .unwrap()
            .build()
            .unwrap()
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        // Barrier after a batch has been produced
        barrier: Arc<Barrier>,
        // Barrier right before the operator is complete
        barrier_finish: Arc<Barrier>,
        /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
        cache: PlanProperties,
    }

    impl Default for TestExec {
        fn default() -> Self {
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "c",
                DataType::Int64,
                true,
            )]));

            let cache = Self::compute_properties(Arc::clone(&schema));

            Self {
                schema,
                barrier: Arc::new(Barrier::new(2)),
                barrier_finish: Arc::new(Barrier::new(2)),
                cache,
            }
        }
    }

    impl TestExec {
        /// wait for the first output to be produced
        pub async fn wait(&self) {
            self.barrier.wait().await;
        }

        /// wait for output to be done
        pub async fn wait_for_finish(&self) {
            self.barrier_finish.wait().await;
        }

        /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);

            let output_partitioning =
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1);

            PlanProperties::new(eq_properties, output_partitioning, ExecutionMode::Bounded)
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "TestExec")
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            let barrier = Arc::clone(&self.barrier);
            let schema = Arc::clone(&self.schema);
            let barrier_finish = Arc::clone(&self.barrier_finish);
            let schema_finish = Arc::clone(&self.schema);
            let stream = futures::stream::iter([Ok(RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int64Array::from(vec![1i64; 100]))],
            )
            .unwrap())])
            .chain(futures::stream::once(async move {
                barrier.wait().await;
                Ok(RecordBatch::new_empty(schema))
            }))
            .chain(futures::stream::once(async move {
                barrier_finish.wait().await;
                Ok(RecordBatch::new_empty(schema_finish))
            }));
            let stream = BoxRecordBatchStream {
                schema: Arc::clone(&self.schema),
                inner: stream.boxed(),
            };
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
            Ok(datafusion::physical_plan::Statistics::new_unknown(
                &self.schema(),
            ))
        }
    }

    struct BoxRecordBatchStream {
        schema: SchemaRef,
        inner: BoxStream<'static, Result<RecordBatch, DataFusionError>>,
    }

    impl Stream for BoxRecordBatchStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = &mut *self;
            this.inner.poll_next_unpin(cx)
        }
    }

    impl RecordBatchStream for BoxRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct PoolMetrics {
        reserved: u64,
        limit: u64,
    }

    impl PoolMetrics {
        fn read(registry: &Registry) -> Self {
            let mut reporter = RawReporter::default();
            registry.report(&mut reporter);
            let metric = reporter.metric("datafusion_mem_pool_bytes").unwrap();

            let reserved = metric.observation(&[("state", "reserved")]).unwrap();
            let Observation::U64Gauge(reserved) = reserved else {
                panic!("wrong metric type")
            };
            let limit = metric.observation(&[("state", "limit")]).unwrap();
            let Observation::U64Gauge(limit) = limit else {
                panic!("wrong metric type")
            };

            Self {
                reserved: *reserved,
                limit: *limit,
            }
        }
    }
}
