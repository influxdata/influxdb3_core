//! Ring buffer of queries that have been run with some brief information

use crate::exec::IOxSessionContext;
use crate::memory_pool::Monitor;
use crate::physical_optimizer::ParquetFileMetrics;
use data_types::NamespaceId;
use datafusion::physical_plan::{metrics::MetricValue, ExecutionPlan};
use iox_query_params::StatementParams;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{info, warn};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use trace::ctx::TraceId;
use uuid::Uuid;

/// Phase of a query entry.
///
/// ```text
///     +-------------------------------+---> fail
///     |                               |
/// received ---> planned ---> permit ---+
///     |           |           |       |
///     |           |           |       +---> success
///     |           |           |
///     |           |           |
///     +-----------+-----------+-----------> cancel
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueryPhase {
    /// Query was received but not processed.
    ///
    /// This is the initial state.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    ///
    /// # To Do
    /// - The query is not planned.
    /// - The concurrency-limiting semaphore has NOT yet issued a permit.
    /// - The query has not been executed.
    Received,

    /// Query was planned and is waiting for a semaphore permit.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    /// - The query was planned.
    ///
    /// # To Do
    /// - The concurrency-limiting semaphore has NOT yet issued a permit.
    /// - The query has not been executed.
    Planned,

    /// Query has the permit to be executed and is likely being executed.
    ///
    /// # Done
    /// - The query has been received (and potentially authenticated) by the server.
    /// - The query was planned.
    /// - The concurrency-limiting semaphore has issued a permit.
    ///
    /// # To Do
    /// - The query has not been executed.
    Permit,

    /// Query was cancelled (likely by the user or a downstream component).
    ///
    /// This is a terminal state.
    Cancel,

    /// Query was fully executed successfully.
    ///
    /// This is a terminal state.
    Success,

    /// Query failed due to an error, e.g. during planning or during execution.
    ///
    /// This is a terminal state.
    Fail,
}

impl QueryPhase {
    /// Name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Planned => "planned",
            Self::Permit => "permit",
            Self::Cancel => "cancel",
            Self::Success => "success",
            Self::Fail => "fail",
        }
    }
}

impl std::fmt::Debug for QueryPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::fmt::Display for QueryPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub struct IngesterMetrics {
    /// when the querier has enough information from all ingesters so that it can proceed with query planning
    pub latency_to_plan: Duration,

    /// measured from the initial request, when the querier has all the data from all ingesters
    pub latency_to_full_data: Duration,

    /// ingester response rows
    pub response_rows: u64,

    /// ingester partition count
    pub partition_count: u64,

    /// ingester record batch size in bytes
    pub response_size: u64,
}

impl std::fmt::Display for IngesterMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IngesterMetrics {{ latency_to_plan = {:?}, latency_to_full_data = {:?}, response_rows = {}, partition_count = {}, response_size = {} }}",
            self.latency_to_plan, self.latency_to_full_data, self.response_rows, self.partition_count, self.response_size
        )
    }
}

/// State of a [`QueryLogEntry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogEntryState {
    /// Unique ID.
    pub id: Uuid,

    /// Namespace ID.
    pub namespace_id: NamespaceId,

    /// Namespace name.
    pub namespace_name: Arc<str>,

    /// The type of query
    pub query_type: &'static str,

    /// The text of the query (SQL for sql queries, pbjson for storage rpc queries)
    pub query_text: QueryTextWrapper,

    /// key-value parameters associated with this query
    pub query_params: StatementParams,

    /// The trace ID if any
    pub trace_id: Option<TraceId>,

    /// Time at which the query was run
    pub issue_time: Time,

    /// Number of partitions processed by the query.
    pub partitions: Option<i64>,

    /// Number of parquet files processed by the query.
    pub parquet_files: Option<i64>,

    /// Duration it took to acquire a semaphore permit, relative to [`issue_time`](Self::issue_time).
    pub permit_duration: Option<Duration>,

    /// Duration it took to plan the query, relative to [`issue_time`](Self::issue_time) + [`permit_duration`](Self::permit_duration).
    pub plan_duration: Option<Duration>,

    /// Duration it took to execute the query, relative to [`issue_time`](Self::issue_time) +
    /// [`permit_duration`](Self::permit_duration) + [`plan_duration`](Self::plan_duration).
    pub execute_duration: Option<Duration>,

    /// Duration from [`issue_time`](Self::issue_time) til the query ended somehow.
    pub end2end_duration: Option<Duration>,

    /// CPU duration spend for computation.
    pub compute_duration: Option<Duration>,

    /// Peak memory allocated for processing the query.
    pub max_memory: Option<i64>,

    /// If the query completed successfully
    pub success: bool,

    /// If the query is currently running (in any state).
    pub running: bool,

    /// Phase.
    pub phase: QueryPhase,

    pub ingester_metrics: Option<IngesterMetrics>,
}

/// Information about a single query that was executed
#[derive(Debug)]
pub struct QueryLogEntry {
    /// State.
    state: Mutex<Arc<QueryLogEntryState>>,
}

impl QueryLogEntry {
    /// Get current state.
    pub fn state(&self) -> Arc<QueryLogEntryState> {
        Arc::clone(&self.state.lock())
    }

    fn set(&self, state: QueryLogEntryState) {
        *self.state.lock() = Arc::new(state);
    }

    /// Log entry.
    fn log(&self) {
        let state = self.state();
        let QueryLogEntryState {
            id,
            namespace_id,
            namespace_name,
            query_type,
            query_text,
            query_params,
            trace_id,
            issue_time,
            partitions,
            parquet_files,
            permit_duration,
            plan_duration,
            execute_duration,
            end2end_duration,
            compute_duration,
            max_memory,
            success,
            running,
            phase,
            ingester_metrics,
        } = state.as_ref();

        let ingester_metrics = ingester_metrics
            .map(|d| d.to_string())
            .unwrap_or("None".to_string());

        info!(
            when=phase.name(),
            id=%id,
            namespace_id=namespace_id.get(),
            namespace_name=namespace_name.as_ref(),
            query_type=query_type,
            query_text=%query_text,
            query_params=%query_params,
            trace_id=trace_id.map(|id| format!("{:x}", id.get())),
            issue_time=%issue_time,
            partitions,
            parquet_files,
            plan_duration_secs=plan_duration.map(|d| d.as_secs_f64()),
            permit_duration_secs=permit_duration.map(|d| d.as_secs_f64()),
            execute_duration_secs=execute_duration.map(|d| d.as_secs_f64()),
            end2end_duration_secs=end2end_duration.map(|d| d.as_secs_f64()),
            compute_duration_secs=compute_duration.map(|d| d.as_secs_f64()),
            max_memory=max_memory,
            ingester_metrics=%ingester_metrics,
            success=success,
            running=running,
            cancelled=(*phase == QueryPhase::Cancel),
            "query",
        )
    }
}

/// Snapshot of the entries the [`QueryLog`].
#[derive(Debug, Default)]
pub struct QueryLogEntries {
    /// Entries.
    pub entries: VecDeque<Arc<QueryLogEntry>>,

    /// Maximum number of entries
    pub max_size: usize,

    /// Number of evicted entries due to the "max size" constraint.
    pub evicted: usize,
}

/// Stores a fixed number `QueryExecutions` -- handles locking
/// internally so can be shared across multiple
pub struct QueryLog {
    log: Mutex<VecDeque<Arc<QueryLogEntry>>>,
    max_size: usize,
    evicted: AtomicUsize,
    time_provider: Arc<dyn TimeProvider>,
    id_gen: IDGen,
}

impl QueryLog {
    /// Create a new QueryLog that can hold at most `size` items.
    /// When the `size+1` item is added, item `0` is evicted.
    pub fn new(max_size: usize, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self::new_with_id_gen(max_size, time_provider, Box::new(Uuid::new_v4))
    }

    pub fn new_with_id_gen(
        max_size: usize,
        time_provider: Arc<dyn TimeProvider>,
        id_gen: IDGen,
    ) -> Self {
        Self {
            log: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            evicted: AtomicUsize::new(0),
            time_provider,
            id_gen,
        }
    }

    pub fn push(
        &self,
        namespace_id: NamespaceId,
        namespace_name: Arc<str>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        trace_id: Option<TraceId>,
    ) -> QueryCompletedToken<StateReceived> {
        let entry = Arc::new(QueryLogEntry {
            state: Mutex::new(Arc::new(QueryLogEntryState {
                id: (self.id_gen)(),
                namespace_id,
                namespace_name,
                query_type,
                query_text: QueryTextWrapper(query_text.into()),
                query_params,
                trace_id,
                issue_time: self.time_provider.now(),
                partitions: Default::default(),
                parquet_files: Default::default(),
                permit_duration: Default::default(),
                plan_duration: Default::default(),
                execute_duration: Default::default(),
                end2end_duration: Default::default(),
                compute_duration: Default::default(),
                max_memory: Default::default(),
                success: false,
                running: true,
                phase: QueryPhase::Received,
                ingester_metrics: None,
            })),
        });
        entry.log();

        let token = QueryCompletedToken {
            entry: Some(Arc::clone(&entry)),
            time_provider: Arc::clone(&self.time_provider),
            state: Default::default(),
        };

        if self.max_size == 0 {
            return token;
        }

        let mut log = self.log.lock();

        // enforce limit
        while log.len() > self.max_size {
            log.pop_front();
            self.evicted.fetch_add(1, Ordering::SeqCst);
        }

        log.push_back(Arc::clone(&entry));
        token
    }

    pub fn entries(&self) -> QueryLogEntries {
        let log = self.log.lock();
        QueryLogEntries {
            entries: log.clone(),
            max_size: self.max_size,
            evicted: self.evicted.load(Ordering::SeqCst),
        }
    }
}

impl Debug for QueryLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLog")
            .field("log", &self.log)
            .field("max_size", &self.max_size)
            .field("evicted", &self.evicted)
            .field("time_provider", &self.time_provider)
            .field("id_gen", &"<ID_GEN>")
            .finish()
    }
}

trait State {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>>;

    fn memory_monitor(&self) -> Option<&Arc<Monitor>>;
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Received`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StateReceived;

impl State for StateReceived {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        None
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        None
    }
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Planned`].
#[derive(Debug)]
pub struct StatePlanned {
    /// Physical execution plan.
    plan: Arc<dyn ExecutionPlan>,

    /// Memory usage monitor.
    memory_monitor: Arc<Monitor>,
}

impl State for StatePlanned {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        Some(&self.plan)
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        Some(&self.memory_monitor)
    }
}

/// State of [`QueryCompletedToken`], equivalent to [`QueryPhase::Permit`].
#[derive(Debug)]
pub struct StatePermit {
    /// Physical execution plan.
    plan: Arc<dyn ExecutionPlan>,

    /// Memory usage monitor.
    memory_monitor: Arc<Monitor>,
}

impl State for StatePermit {
    fn plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        Some(&self.plan)
    }

    fn memory_monitor(&self) -> Option<&Arc<Monitor>> {
        Some(&self.memory_monitor)
    }
}

/// A `QueryCompletedToken` is returned by `record_query` implementations of
/// a `QueryNamespace`. It is used to trigger side-effects (such as query timing)
/// on query completion.
#[derive(Debug)]
#[allow(private_bounds)]
pub struct QueryCompletedToken<S>
where
    S: State,
{
    /// Entry.
    ///
    /// This is optional so we can implement type state and [`Drop`] at the same time.
    entry: Option<Arc<QueryLogEntry>>,

    /// Time provider
    time_provider: Arc<dyn TimeProvider>,

    /// Current state.
    state: S,
}

#[allow(private_bounds)]
impl<S> QueryCompletedToken<S>
where
    S: State,
{
    /// Underlying entry.
    pub fn entry(&self) -> &Arc<QueryLogEntry> {
        self.entry.as_ref().expect("valid state")
    }

    fn collect_compute_time(&self, state: &mut QueryLogEntryState) {
        let Some(plan) = self.state.plan() else {
            return;
        };

        state.compute_duration = Some(collect_compute_duration(plan.as_ref()));
    }

    fn collect_memory_usage(&self, state: &mut QueryLogEntryState) {
        if let Some(memory_monitor) = self.state.memory_monitor() {
            state.max_memory = Some(memory_monitor.max() as i64);
        }
    }

    fn collect_ingester_metrics(&self, state: &mut QueryLogEntryState) {
        if let Some(plan) = self.state.plan() {
            state.ingester_metrics = Some(collect_ingester_metrics(plan.as_ref()));
        }
    }
}

impl QueryCompletedToken<StateReceived> {
    /// Record that this query got planned.
    pub fn planned(
        mut self,
        ctx: &IOxSessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> QueryCompletedToken<StatePlanned> {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();
        self.set_time(&mut state);
        state.phase = QueryPhase::Planned;
        let ParquetFileMetrics {
            partitions,
            parquet_files,
        } = ParquetFileMetrics::plan_metrics(plan.as_ref());
        state.partitions = Some(partitions as i64);
        state.parquet_files = Some(parquet_files as i64);
        entry.set(state);
        entry.log();

        QueryCompletedToken {
            entry: Some(entry),
            time_provider: Arc::clone(&self.time_provider),
            state: StatePlanned {
                plan,
                memory_monitor: Arc::clone(ctx.memory_monitor()),
            },
        }
    }

    /// Record that this query failed during planning.
    pub fn fail(self) {
        let entry = self.entry.as_ref().expect("valid state");
        let mut state = entry.state().as_ref().clone();
        self.set_time(&mut state);
        state.phase = QueryPhase::Fail;
        entry.set(state);
    }

    fn set_time(&self, state: &mut QueryLogEntryState) {
        let now = self.time_provider.now();
        let origin = state.issue_time;
        set_relative(origin, now, &mut state.plan_duration);
    }
}

impl QueryCompletedToken<StatePlanned> {
    /// Record that this query got a semaphore permit.
    pub fn permit(mut self) -> QueryCompletedToken<StatePermit> {
        let entry = self.entry.take().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        if let Some(plan_duration) = state.plan_duration {
            let now = self.time_provider.now();
            let origin = state.issue_time + plan_duration;
            set_relative(origin, now, &mut state.permit_duration);
        }
        state.phase = QueryPhase::Permit;
        entry.set(state);
        entry.log();

        QueryCompletedToken {
            entry: Some(entry),
            time_provider: Arc::clone(&self.time_provider),
            state: StatePermit {
                plan: Arc::clone(&self.state.plan),
                memory_monitor: Arc::clone(&self.state.memory_monitor),
            },
        }
    }
}

impl QueryCompletedToken<StatePermit> {
    /// Record that this query completed successfully
    pub fn success(self) {
        let entry = self.entry.as_ref().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        state.success = true;
        state.phase = QueryPhase::Success;

        self.finish(entry, state)
    }

    /// Record that the query finished execution with an error.
    pub fn fail(self) {
        let entry = self.entry.as_ref().expect("valid state");
        let mut state = entry.state().as_ref().clone();

        state.phase = QueryPhase::Fail;

        self.finish(entry, state)
    }

    fn finish(&self, entry: &Arc<QueryLogEntry>, mut state: QueryLogEntryState) {
        if let (Some(permit_duration), Some(plan_duration)) =
            (state.permit_duration, state.plan_duration)
        {
            let now = self.time_provider.now();
            let origin = state.issue_time + permit_duration + plan_duration;
            set_relative(origin, now, &mut state.execute_duration);
        }

        self.collect_compute_time(&mut state);
        self.collect_memory_usage(&mut state);
        self.collect_ingester_metrics(&mut state);
        entry.set(state);
    }
}

impl<S> Drop for QueryCompletedToken<S>
where
    S: State,
{
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            let mut state = entry.state().as_ref().clone();

            if state.phase != QueryPhase::Fail && state.execute_duration.is_none() {
                state.phase = QueryPhase::Cancel;

                if state.permit_duration.is_some() {
                    // started computation, collect partial stats
                    self.collect_compute_time(&mut state);
                    self.collect_ingester_metrics(&mut state);
                    self.collect_memory_usage(&mut state);
                }
            }

            let now = self.time_provider.now();
            set_relative(state.issue_time, now, &mut state.end2end_duration);
            state.running = false;

            entry.set(state);
            entry.log();
        }
    }
}

/// Boxed description of a query that knows how to render to a string
///
/// This avoids storing potentially large strings
pub type QueryText = Box<dyn std::fmt::Display + Send + Sync>;

/// Wrapper for QueryText that also implements [`Debug`].
#[derive(Clone)]
pub struct QueryTextWrapper(Arc<dyn std::fmt::Display + Send + Sync>);

impl QueryTextWrapper {
    #[cfg(test)]
    fn from_static(s: &'static str) -> Self {
        Self(Arc::new(s.to_owned()))
    }
}

impl Debug for QueryTextWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for QueryTextWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for QueryTextWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }
}

impl Eq for QueryTextWrapper {}

/// Method that generates [`Uuid`]s.
pub type IDGen = Box<dyn Fn() -> Uuid + Send + Sync>;

fn set_relative(origin: Time, now: Time, target: &mut Option<Duration>) {
    match now.checked_duration_since(origin) {
        Some(dur) => {
            *target = Some(dur);
        }
        None => {
            warn!("Clock went backwards, not query duration")
        }
    }
}

/// Collect compute duration from [`ExecutionPlan`].
fn collect_compute_duration(plan: &dyn ExecutionPlan) -> Duration {
    let mut total = Duration::ZERO;

    if let Some(metrics) = plan.metrics() {
        if let Some(nanos) = metrics.elapsed_compute() {
            total += Duration::from_nanos(nanos as u64);
        }
    }

    for child in plan.children() {
        total += collect_compute_duration(child.as_ref());
    }

    total
}

fn collect_ingester_metrics(plan: &dyn ExecutionPlan) -> IngesterMetrics {
    let mut latency_to_plan = Duration::ZERO;
    let mut latency_to_full_data = Duration::ZERO;
    let mut response_rows = 0;
    let mut partition_count = 0;
    let mut response_size = 0;

    if let Some(metrics) = plan.metrics() {
        for m in metrics.iter() {
            let value = m.value();

            match value.name() {
                "latency_to_plan" => {
                    if let MetricValue::Time { time, .. } = value {
                        latency_to_plan = Duration::from_nanos(time.value() as u64);
                    }
                }
                "latency_to_full_data" => {
                    if let MetricValue::Time { time, .. } = value {
                        latency_to_full_data = Duration::from_nanos(time.value() as u64);
                    }
                }
                "response_rows" => {
                    if let MetricValue::Count { count, .. } = value {
                        response_rows = count.value() as u64;
                    }
                }
                "partition_count" => {
                    if let MetricValue::Count { count, .. } = value {
                        partition_count = count.value() as u64;
                    }
                }
                "response_size" => {
                    if let MetricValue::Count { count, .. } = value {
                        response_size = count.value() as u64;
                    }
                }
                _ => {}
            }
        }
    }

    for child in plan.children() {
        let IngesterMetrics {
            latency_to_plan: lp,
            latency_to_full_data: ld,
            response_rows: rr,
            partition_count: pc,
            response_size: rs,
        } = collect_ingester_metrics(child.as_ref());
        latency_to_plan = lp;
        latency_to_full_data = ld;
        response_rows += rr;
        partition_count += pc;
        response_size += rs;
    }

    IngesterMetrics {
        latency_to_plan,
        latency_to_full_data,
        response_rows,
        partition_count,
        response_size,
    }
}

#[cfg(test)]
mod test_super {
    use datafusion::error::DataFusionError;
    use iox_query_params::params;
    use std::sync::atomic::AtomicU64;

    use datafusion::physical_plan::{
        metrics::{MetricValue, MetricsSet},
        DisplayAs, Metric,
    };
    use iox_time::MockProvider;
    use test_helpers::tracing::TracingCapture;

    use super::*;

    #[test]
    fn test_token_end2end_success() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        let expected = start_state;
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            phase: QueryPhase::Planned,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();

        let expected = QueryLogEntryState {
            permit_duration: Some(Duration::from_millis(10)),
            phase: QueryPhase::Permit,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(100));
        token.success();

        let expected = QueryLogEntryState {
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            success: true,
            running: false,
            phase: QueryPhase::Success,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "success"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics = IngesterMetrics { latency_to_plan = 0ns, latency_to_full_data = 0ns, response_rows = 0, partition_count = 0, response_size = 0 }; success = true; running = false; cancelled = false;"#,
            ],
        );
    }

    #[test]
    fn test_params_end2end_success() {
        let params: StatementParams = params!(
            "a" => true,
        );
        let capture = TracingCapture::new();
        let test = Test::with_log_entry(
            NamespaceId::new(1),
            Arc::from("ns"),
            "sql",
            "SELECT $a;",
            params.clone(),
            None,
        );
        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = test;

        let expected = start_state;
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            phase: QueryPhase::Planned,
            ingester_metrics: None,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();

        let expected = QueryLogEntryState {
            permit_duration: Some(Duration::from_millis(10)),
            phase: QueryPhase::Permit,
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        time_provider.inc(Duration::from_millis(100));
        token.success();

        let expected = QueryLogEntryState {
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            success: true,
            running: false,
            phase: QueryPhase::Success,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..expected
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "success"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT $a;; query_params = Params { "a" => TRUE, }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics = IngesterMetrics { latency_to_plan = 0ns, latency_to_full_data = 0ns, response_rows = 0, partition_count = 0, response_size = 0 }; success = true; running = false; cancelled = false;"#,
            ],
        );
    }

    #[test]
    fn test_token_planning_fail() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        token.fail();

        let expected = QueryLogEntryState {
            plan_duration: Some(Duration::from_millis(1)),
            end2end_duration: Some(Duration::from_millis(1)),
            running: false,
            phase: QueryPhase::Fail,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "fail"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; plan_duration_secs = 0.001; end2end_duration_secs = 0.001; ingester_metrics = None; success = false; running = false; cancelled = false;"#,
            ],
        );
    }

    #[test]
    fn test_token_execution_fail() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();
        time_provider.inc(Duration::from_millis(100));
        token.fail();

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            permit_duration: Some(Duration::from_millis(10)),
            execute_duration: Some(Duration::from_millis(100)),
            end2end_duration: Some(Duration::from_millis(111)),
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            running: false,
            phase: QueryPhase::Fail,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "fail"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; execute_duration_secs = 0.1; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics = IngesterMetrics { latency_to_plan = 0ns, latency_to_full_data = 0ns, response_rows = 0, partition_count = 0, response_size = 0 }; success = false; running = false; cancelled = false;"#,
            ],
        );
    }

    #[test]
    fn test_token_drop_before_planned() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        drop(token);

        let expected = QueryLogEntryState {
            end2end_duration: Some(Duration::from_millis(1)),
            running: false,
            phase: QueryPhase::Cancel,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; end2end_duration_secs = 0.001; ingester_metrics = None; success = false; running = false; cancelled = true;"#,
            ],
        );
    }

    #[test]
    fn test_token_drop_before_acquire() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        drop(token);

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            end2end_duration: Some(Duration::from_millis(11)),
            running: false,
            phase: QueryPhase::Cancel,
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; end2end_duration_secs = 0.011; ingester_metrics = None; success = false; running = false; cancelled = true;"#,
            ],
        );
    }

    #[test]
    fn test_token_drop_before_finish() {
        let capture = TracingCapture::new();

        let Test {
            time_provider,
            token,
            entry,
            start_state,
        } = Test::default();

        time_provider.inc(Duration::from_millis(1));
        let ctx = IOxSessionContext::with_testing();
        let token = token.planned(&ctx, plan());
        time_provider.inc(Duration::from_millis(10));
        let token = token.permit();
        time_provider.inc(Duration::from_millis(100));
        drop(token);

        let expected = QueryLogEntryState {
            partitions: Some(0),
            parquet_files: Some(0),
            plan_duration: Some(Duration::from_millis(1)),
            permit_duration: Some(Duration::from_millis(10)),
            end2end_duration: Some(Duration::from_millis(111)),
            // partial stats collected
            compute_duration: Some(Duration::from_millis(1_337)),
            max_memory: Some(0),
            running: false,
            phase: QueryPhase::Cancel,
            ingester_metrics: Some(IngesterMetrics::default()),
            ..start_state
        };
        assert_eq!(entry.state().as_ref(), &expected,);

        assert_logs(
            capture,
            [
                r#"level = INFO; message = query; when = "received"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "planned"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "permit"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; ingester_metrics = None; success = false; running = true; cancelled = false;"#,
                r#"level = INFO; message = query; when = "cancel"; id = 00000000-0000-0000-0000-000000000001; namespace_id = 1; namespace_name = "ns"; query_type = "sql"; query_text = SELECT 1; query_params = Params { }; issue_time = 1970-01-01T00:00:00.100+00:00; partitions = 0; parquet_files = 0; plan_duration_secs = 0.001; permit_duration_secs = 0.01; end2end_duration_secs = 0.111; compute_duration_secs = 1.337; max_memory = 0; ingester_metrics = IngesterMetrics { latency_to_plan = 0ns, latency_to_full_data = 0ns, response_rows = 0, partition_count = 0, response_size = 0 }; success = false; running = false; cancelled = true;"#,
            ],
        );
    }

    struct Test {
        time_provider: Arc<MockProvider>,
        token: QueryCompletedToken<StateReceived>,
        entry: Arc<QueryLogEntry>,
        start_state: QueryLogEntryState,
    }

    impl Test {
        fn with_log_entry(
            namespace_id: NamespaceId,
            namespace_name: Arc<str>,
            query_type: &'static str,
            query_text: &'static str,
            query_params: StatementParams,
            trace_id: Option<TraceId>,
        ) -> Self {
            let time_provider =
                Arc::new(MockProvider::new(Time::from_timestamp_millis(100).unwrap()));
            let id_counter = AtomicU64::new(1);
            let log = QueryLog::new_with_id_gen(
                1_000,
                Arc::clone(&time_provider) as _,
                Box::new(move || Uuid::from_u128(id_counter.fetch_add(1, Ordering::SeqCst) as _)),
            );

            let token = log.push(
                namespace_id,
                Arc::clone(&namespace_name),
                query_type,
                Box::new(query_text),
                query_params.clone(),
                trace_id,
            );

            let entry = Arc::clone(token.entry());

            let start_state = QueryLogEntryState {
                id: Uuid::from_u128(1),
                namespace_id,
                namespace_name,
                query_type,
                query_text: QueryTextWrapper::from_static(query_text),
                query_params,
                trace_id,
                issue_time: Time::from_timestamp_millis(100).unwrap(),
                partitions: None,
                parquet_files: None,
                permit_duration: None,
                plan_duration: None,
                execute_duration: None,
                end2end_duration: None,
                compute_duration: None,
                max_memory: None,
                success: false,
                running: true,
                phase: QueryPhase::Received,
                ingester_metrics: None,
            };

            Self {
                time_provider,
                token,
                entry,
                start_state,
            }
        }
    }

    impl Default for Test {
        fn default() -> Self {
            Self::with_log_entry(
                NamespaceId::new(1),
                Arc::from("ns"),
                "sql",
                "SELECT 1",
                Default::default(),
                None,
            )
        }
    }

    fn plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(TestExec)
    }

    #[derive(Debug)]
    struct TestExec;

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            _f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            unimplemented!()
        }

        fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
            unimplemented!()
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
            unimplemented!()
        }

        fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
            unimplemented!()
        }

        fn metrics(&self) -> Option<MetricsSet> {
            let mut metrics = MetricsSet::default();

            let t = datafusion::physical_plan::metrics::Time::default();
            t.add_duration(Duration::from_millis(1_337));
            metrics.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(t), None)));

            Some(metrics)
        }
    }

    #[track_caller]
    fn assert_logs<const N: usize>(capture: TracingCapture, expected: [&str; N]) {
        let logs = capture.to_string();
        let actual = logs
            .split('\n')
            .map(|s| s.trim())
            .filter(|s| s.starts_with("level = INFO;"))
            .collect::<Vec<_>>();
        assert!(
            actual == expected,
            "Actual:\n{}\n\nExpected:\n{}",
            actual.join("\n"),
            expected.join("\n")
        );
    }
}
