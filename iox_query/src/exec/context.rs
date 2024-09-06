//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use super::{
    cross_rt_stream::CrossRtStream,
    gapfill::{plan_gap_fill, GapFill},
    sleep::SleepNode,
    split::StreamSplitNode,
};
use crate::{
    analyzer::register_iox_analyzers,
    config::IoxConfigExt,
    exec::{
        query_tracing::TracedStream,
        schema_pivot::{SchemaPivotExec, SchemaPivotNode},
        split::StreamSplitExec,
    },
    logical_optimizer::register_iox_logical_optimizers,
    physical_optimizer::register_iox_physical_optimizers,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::{
    catalog::CatalogProvider,
    common::ParamValues,
    config::ConfigExtension,
    execution::{
        context::{QueryPlanner, SessionState, TaskContext},
        runtime_env::RuntimeEnv,
        session_state::SessionStateBuilder,
    },
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    optimizer::{AnalyzerRule, OptimizerRule},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, displayable, stream::RecordBatchStreamAdapter,
        EmptyRecordBatchStream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::*,
};
use datafusion_util::config::{iox_session_config, DEFAULT_CATALOG};
use executor::DedicatedExecutor;
use futures::TryStreamExt;
use observability_deps::tracing::debug;
use query_functions::{register_iox_scalar_functions, selectors::register_selector_aggregates};
use std::{fmt, num::NonZeroUsize, str::FromStr, sync::Arc};
use trace::{
    ctx::SpanContext,
    span::{MetaValue, Span, SpanEvent, SpanExt, SpanRecorder},
};

// Reuse DataFusion error and Result types for this module
use crate::memory_pool::{Monitor, MonitoredMemoryPool};
pub use datafusion::error::{DataFusionError, Result};

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl QueryPlanner for IOxQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot
        // and StreamSplit nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone());
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(schema_pivot) = any.downcast_ref::<SchemaPivotNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(SchemaPivotExec::new(
                Arc::clone(&physical_inputs[0]),
                schema_pivot.schema().as_ref().clone().into(),
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(stream_split) = any.downcast_ref::<StreamSplitNode>() {
            assert_eq!(
                logical_inputs.len(),
                1,
                "Inconsistent number of logical inputs"
            );
            assert_eq!(
                physical_inputs.len(),
                1,
                "Inconsistent number of physical inputs"
            );

            let split_exprs = stream_split
                .split_exprs()
                .iter()
                .map(|e| planner.create_physical_expr(e, logical_inputs[0].schema(), session_state))
                .collect::<Result<Vec<_>>>()?;

            Some(Arc::new(StreamSplitExec::new(
                Arc::clone(&physical_inputs[0]),
                split_exprs,
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(gap_fill) = any.downcast_ref::<GapFill>() {
            let gap_fill_exec =
                plan_gap_fill(session_state, gap_fill, logical_inputs, physical_inputs)?;
            Some(Arc::new(gap_fill_exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(sleep) = any.downcast_ref::<SleepNode>() {
            let sleep = sleep.plan(planner, logical_inputs, physical_inputs, session_state)?;
            Some(Arc::new(sleep) as _)
        } else {
            None
        };
        Ok(plan)
    }
}

/// Configuration for an IOx execution context
///
/// Created from an Executor
#[derive(Clone)]
pub struct IOxSessionConfig {
    /// Executor to run on
    exec: DedicatedExecutor,

    /// DataFusion session configuration
    session_config: SessionConfig,

    /// Shared DataFusion runtime
    runtime: Arc<RuntimeEnv>,

    /// Default catalog
    default_catalog: Option<Arc<dyn CatalogProvider>>,

    /// Span context from which to create spans for this query
    span_ctx: Option<SpanContext>,

    /// Planners for extensions that are provided by the consumer.
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,

    /// Analyzer rules for extensions that are provided by the consumer.
    analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,

    /// Optimizer rules for extensions that are provided by the consumer.
    optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,

    /// Physical optimizer rules for extensions that are provided by the consumer.
    physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl fmt::Debug for IOxSessionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IOxSessionConfig ...")
    }
}

impl IOxSessionConfig {
    pub(super) fn new(exec: DedicatedExecutor, runtime: Arc<RuntimeEnv>) -> Self {
        let mut session_config = iox_session_config();
        session_config
            .options_mut()
            .extensions
            .insert(IoxConfigExt::default());

        Self {
            exec,
            session_config,
            runtime,
            default_catalog: None,
            span_ctx: None,
            extension_planners: vec![],
            analyzer_rules: vec![],
            optimizer_rules: vec![],
            physical_optimizer_rules: vec![],
        }
    }

    /// add a new extension to the session config
    pub fn with_extension(mut self, ext: impl ConfigExtension) -> Self {
        self.session_config.options_mut().extensions.insert(ext);
        self
    }

    /// Set execution concurrency
    pub fn with_target_partitions(mut self, target_partitions: NonZeroUsize) -> Self {
        self.session_config = self
            .session_config
            .with_target_partitions(target_partitions.get());
        self
    }

    /// Set the default catalog provider
    pub fn with_default_catalog(self, catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            default_catalog: Some(catalog),
            ..self
        }
    }

    /// Set the span context from which to create  distributed tracing spans for this query
    pub fn with_span_context(self, span_ctx: Option<SpanContext>) -> Self {
        Self { span_ctx, ..self }
    }

    /// Set DataFusion [config option].
    ///
    /// May be used to set [IOx-specific] option as well.
    ///
    ///
    /// [config option]: datafusion::common::config::ConfigOptions
    /// [IOx-specific]: crate::config::IoxConfigExt
    pub fn with_config_option(mut self, key: &str, value: &str) -> Self {
        // ignore invalid config
        if let Err(e) = self.session_config.options_mut().set(key, value) {
            debug!(
                key,
                value,
                %e,
                "invalid DataFusion config",
            );
        }
        self
    }

    /// Set query-specific configuration options.
    pub fn with_query_config(mut self, config: &QueryConfig) -> Self {
        let iox_config: &mut IoxConfigExt = self
            .session_config
            .options_mut()
            .extensions
            .get_mut()
            .unwrap();
        if let Some(partition_limit) = config.partition_limit {
            iox_config.set_partition_limit(partition_limit);
        };
        if let Some(parquet_file_limit) = config.parquet_file_limit {
            iox_config.set_parquet_file_limit(parquet_file_limit);
        };
        self
    }

    /// Add an [`ExtensionPlanner`] to the  context. `ExtensionPlanner`s
    /// take precedence in the order they are added, and take precedence
    /// over the `iox_query` and datafusion planners.
    pub fn with_extension_planner(
        mut self,
        extension_planner: Arc<dyn ExtensionPlanner + Send + Sync>,
    ) -> Self {
        self.extension_planners.push(extension_planner);
        self
    }

    /// Add an [`AnalyzerRule`] to the context.
    pub fn with_analyzer_rule(
        mut self,
        analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>,
    ) -> Self {
        self.analyzer_rules.push(analyzer_rule);
        self
    }

    /// Add an [`OptimizerRule`] to the context.
    pub fn with_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizer_rules.push(optimizer_rule);
        self
    }

    /// Add an [`PhysicalOptimizerRule`] to the context.
    pub fn with_physical_optimizer_rule(
        mut self,
        physical_optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizer_rules.push(physical_optimizer_rule);
        self
    }

    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn build(self) -> IOxSessionContext {
        let maybe_span = self.span_ctx.child_span("query_planning");
        let recorder = SpanRecorder::new(maybe_span);

        // attach span to DataFusion session
        let session_config = self
            .session_config
            .with_extension(Arc::new(recorder.span().cloned()));

        let memory_monitor = Arc::new(Default::default());
        let runtime = RuntimeEnv {
            memory_pool: Arc::new(MonitoredMemoryPool::new(
                Arc::clone(&self.runtime.memory_pool),
                Arc::clone(&memory_monitor),
            )),
            disk_manager: Arc::clone(&self.runtime.disk_manager),
            cache_manager: Arc::clone(&self.runtime.cache_manager),
            object_store_registry: Arc::clone(&self.runtime.object_store_registry),
        };
        let mut extension_planners = self.extension_planners;
        extension_planners.push(Arc::new(IOxExtensionPlanner {}));

        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime))
            .with_default_features()
            .with_query_planner(Arc::new(IOxQueryPlanner { extension_planners }));

        let state = register_iox_physical_optimizers(state);
        let state = register_iox_logical_optimizers(state);
        let mut state = register_iox_analyzers(state);

        for analyzer_rule in self.analyzer_rules {
            state = state.with_analyzer_rule(analyzer_rule);
        }
        for optimizer_rule in self.optimizer_rules {
            state = state.with_optimizer_rule(optimizer_rule);
        }
        for physical_optimizer_rule in self.physical_optimizer_rules {
            state = state.with_physical_optimizer_rule(physical_optimizer_rule);
        }

        let state = state.build();
        let inner = SessionContext::new_with_state(state);
        register_selector_aggregates(&inner);
        register_iox_scalar_functions(&inner);
        if let Some(default_catalog) = self.default_catalog {
            inner.register_catalog(DEFAULT_CATALOG, default_catalog);
        }

        IOxSessionContext::new(inner, self.exec, recorder, memory_monitor)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryLanguage {
    Sql,
    InfluxQL,
}

#[derive(Debug, Clone, Copy)]
pub struct NoSuchQueryLanguage;

impl std::fmt::Display for NoSuchQueryLanguage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "The provided string did not match any known query languages"
        )
    }
}

impl std::error::Error for NoSuchQueryLanguage {}

impl FromStr for QueryLanguage {
    type Err = NoSuchQueryLanguage;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("sql") {
            Ok(Self::Sql)
        } else if s.eq_ignore_ascii_case("influxql") {
            Ok(Self::InfluxQL)
        } else {
            Err(NoSuchQueryLanguage)
        }
    }
}

/// Configuration that may be applied differently for each query.
#[derive(Debug, Default, Clone, Copy)]
pub struct QueryConfig {
    /// Limit the number of partitions to scan in a single query. This may only reduce any limit
    /// set for the system.
    pub partition_limit: Option<usize>,

    /// Limit the number of parquet files to scan in a single query. This may only reduce any limit
    /// set for the system.
    pub parquet_file_limit: Option<usize>,
}

/// This is an execution context for planning in IOx.  It wraps a
/// DataFusion execution context with the information needed for planning.
///
/// Methods on this struct should be preferred to using the raw
/// DataFusion functions (such as `collect`) directly.
///
/// Eventually we envision this also managing additional resource
/// types such as Memory and providing visibility into what plans are
/// running
///
/// An IOxSessionContext is created directly from an Executor, or from
/// an IOxSessionConfig created by an Executor
pub struct IOxSessionContext {
    inner: SessionContext,

    /// Dedicated executor for query execution.
    ///
    /// DataFusion plans are "CPU" bound and thus can consume tokio
    /// executors threads for extended periods of time. We use a
    /// dedicated tokio runtime to run them so that other requests
    /// can be handled.
    exec: DedicatedExecutor,

    /// Span context from which to create spans for this query
    recorder: SpanRecorder,

    /// Monitor for the amount of memory allocated in this context.
    memory_monitor: Arc<Monitor>,
}

impl fmt::Debug for IOxSessionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxSessionContext")
            .field("inner", &"<DataFusion ExecutionContext>")
            .field("exec", &self.exec)
            .field("recorder", &self.recorder)
            .finish()
    }
}

impl IOxSessionContext {
    /// Constructor for testing.
    ///
    /// This is identical to [`Default::default`] but we do NOT implement [`Default`] to make the creation of untracked
    /// contexts more explicit.
    pub fn with_testing() -> Self {
        Self {
            inner: SessionContext::default(),
            exec: DedicatedExecutor::new_testing(),
            recorder: SpanRecorder::default(),
            memory_monitor: Arc::new(Default::default()),
        }
    }

    /// Private constructor
    pub(crate) fn new(
        inner: SessionContext,
        exec: DedicatedExecutor,
        recorder: SpanRecorder,
        memory_monitor: Arc<Monitor>,
    ) -> Self {
        Self {
            inner,
            exec,
            recorder,
            memory_monitor,
        }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Plan a SQL statement. This assumes that any tables referenced
    /// in the SQL have been registered with this context. Use
    /// `create_physical_plan` to actually execute the query.
    pub async fn sql_to_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let ctx = self.child_ctx("sql_to_logical_plan");
        debug!(text=%sql, "planning SQL query");
        let plan = ctx.inner.state().create_logical_plan(sql).await?;
        // ensure the plan does not contain unwanted statements
        let verifier = SQLOptions::new()
            .with_allow_ddl(false) // no CREATE ...
            .with_allow_dml(false) // no INSERT or COPY
            .with_allow_statements(false); // no SET VARIABLE, etc
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    /// Plan a SQL statement, providing a list of parameter values
    /// to supply to `$placeholder` variables. This assumes that
    /// any tables referenced in the SQL have been registered with
    /// this context. Use `create_physical_plan` to actually execute
    /// the query.
    pub async fn sql_to_logical_plan_with_params(
        &self,
        sql: &str,
        params: impl Into<ParamValues> + Send,
    ) -> Result<LogicalPlan> {
        self.sql_to_logical_plan(sql)
            .await?
            .with_param_values(params)
    }

    /// Create a logical plan that reads a single [`RecordBatch`]. Use
    /// `create_physical_plan` to actually execute the query.
    pub fn batch_to_logical_plan(&self, batch: RecordBatch) -> Result<LogicalPlan> {
        let ctx = self.child_ctx("batch_to_logical_plan");
        debug!(num_rows = batch.num_rows(), "planning RecordBatch query");
        ctx.inner.read_batch(batch)?.into_optimized_plan()
    }

    /// Plan a SQL statement and convert it to an execution plan. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn sql_to_physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.child_ctx("sql_to_physical_plan");

        let logical_plan = ctx.sql_to_logical_plan(sql).await?;

        ctx.create_physical_plan(&logical_plan).await
    }

    /// Plan a SQL statement and convert it to an execution plan, providing a list of
    /// parameter values to supply to `$placeholder` variables. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn sql_to_physical_plan_with_params(
        &self,
        sql: &str,
        params: impl Into<ParamValues> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.child_ctx("sql_to_physical_plan_with_params");

        let logical_plan = ctx.sql_to_logical_plan_with_params(sql, params).await?;

        ctx.create_physical_plan(&logical_plan).await
    }

    /// Prepare (optimize + plan) a pre-created [`LogicalPlan`] for execution
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = self.child_ctx("create_physical_plan");
        debug!(text=%logical_plan.display_indent_schema(), "create_physical_plan: initial plan");
        let physical_plan = ctx.inner.state().create_physical_plan(logical_plan).await?;

        ctx.recorder.event(SpanEvent::new("physical plan"));
        debug!(text=%displayable(physical_plan.as_ref()).indent(false), "create_physical_plan: plan to run");
        Ok(physical_plan)
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        debug!(
            "Running plan, physical:\n{}",
            displayable(physical_plan.as_ref()).indent(false)
        );
        let ctx = self.child_ctx("collect");
        let stream = ctx.execute_stream(physical_plan).await?;

        ctx.run(
            stream
                .err_into() // convert to DataFusionError
                .try_collect(),
        )
        .await
    }

    /// Executes the physical plan and produces a
    /// `SendableRecordBatchStream` to stream over the result that
    /// iterates over the results. The creation of the stream is
    /// performed in a separate thread pool.
    pub async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan
            .properties()
            .output_partitioning()
            .partition_count()
        {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(
                physical_plan.schema(),
            ))),
            1 => self.execute_stream_partitioned(physical_plan, 0).await,
            _ => {
                // Merge into a single partition
                self.execute_stream_partitioned(
                    Arc::new(CoalescePartitionsExec::new(physical_plan)),
                    0,
                )
                .await
            }
        }
    }

    /// Executes a single partition of a physical plan and produces a
    /// `SendableRecordBatchStream` to stream over the result that
    /// iterates over the results. The creation of the stream is
    /// performed in a separate thread pool.
    pub async fn execute_stream_partitioned(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let span = self
            .recorder
            .span()
            .map(|span| span.child("execute_stream_partitioned"));

        let task_context = Arc::new(TaskContext::from(self.inner()));

        let stream = self
            .run(async move {
                let stream = physical_plan.execute(partition, task_context)?;
                Ok(TracedStream::new(stream, span, physical_plan))
            })
            .await?;
        // Wrap the resulting stream into `CrossRtStream`. This is required because polling the DataFusion result stream
        // actually drives the (potentially CPU-bound) work. We need to make sure that this work stays within the
        // dedicated executor because otherwise this may block the top-level tokio/tonic runtime which may lead to
        // requests timeouts (either for new requests, metrics or even for HTTP2 pings on the active connection).
        let schema = stream.schema();
        let stream = CrossRtStream::new_with_df_error_stream(stream, self.exec.clone());
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    /// Runs the provided future using this execution context
    pub async fn run<Fut, T>(&self, fut: Fut) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        Self::run_inner(self.exec.clone(), fut).await
    }

    async fn run_inner<Fut, T>(exec: DedicatedExecutor, fut: Fut) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        exec.spawn(fut).await.unwrap_or_else(|e| {
            Err(DataFusionError::Context(
                "Join Error".to_string(),
                Box::new(DataFusionError::External(Box::new(e))),
            ))
        })
    }

    /// Returns a IOxSessionContext with a SpanRecorder that is a child of the current
    pub fn child_ctx(&self, name: &'static str) -> Self {
        Self::new(
            self.inner.clone(),
            self.exec.clone(),
            self.recorder.child(name),
            Arc::clone(&self.memory_monitor),
        )
    }

    /// Record an event on the span recorder
    pub fn record_event(&mut self, name: &'static str) {
        self.recorder.event(SpanEvent::new(name));
    }

    /// Record an event on the span recorder
    pub fn set_metadata(&mut self, name: &'static str, value: impl Into<MetaValue>) {
        self.recorder.set_metadata(name, value);
    }

    /// Returns the current [`Span`] if any
    pub fn span(&self) -> Option<&Span> {
        self.recorder.span()
    }

    /// Returns a new child span of the current context
    pub fn child_span(&self, name: &'static str) -> Option<Span> {
        self.recorder.child_span(name)
    }

    /// Retrieve the memory monitor for this context.
    pub(crate) fn memory_monitor(&self) -> &Arc<Monitor> {
        &self.memory_monitor
    }
}

/// Extension trait to pull IOx spans out of DataFusion contexts.
pub trait SessionContextIOxExt {
    /// Get child span of the current context.
    fn child_span(&self, name: &'static str) -> Option<Span>;

    /// Get span context
    fn span_ctx(&self) -> Option<SpanContext>;
}

impl SessionContextIOxExt for &dyn Session {
    fn child_span(&self, name: &'static str) -> Option<Span> {
        self.config()
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.child(name)))
    }

    fn span_ctx(&self) -> Option<SpanContext> {
        self.config()
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.ctx.clone()))
    }
}
