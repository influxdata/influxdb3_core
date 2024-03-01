use std::io::Write;

use crate::snapshot_comparison::Language;
use crate::{
    check_flight_error, run_influxql, run_influxql_with_params, run_sql, run_sql_with_params,
    snapshot_comparison, try_run_influxql, try_run_influxql_with_params, try_run_sql,
    try_run_sql_with_params, MiniCluster,
};
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use futures::future::BoxFuture;
use http::StatusCode;
use iox_query_params::StatementParam;
use observability_deps::tracing::info;
use serde_json::Value;
use std::collections::HashMap;
use std::{path::PathBuf, time::Duration};
use test_helpers::assert_contains;
use tokio::time;

const MAX_QUERY_RETRY_TIME_SEC: u64 = 20;

/// Test harness for end to end tests that are comprised of several steps
#[allow(missing_debug_implementations)]
pub struct StepTest<'a, S> {
    cluster: &'a mut MiniCluster,

    /// The test steps to perform
    steps: Box<dyn Iterator<Item = S> + Send + Sync + 'a>,
}

/// The test state that is passed to custom steps
#[derive(Debug)]
pub struct StepTestState<'a> {
    /// The mini cluster
    cluster: &'a mut MiniCluster,

    /// How many Parquet files the catalog service knows about for the mini cluster's namespace,
    /// for tracking when persistence has happened. If this is `None`, we haven't ever checked with
    /// the catalog service.
    num_parquet_files: Option<usize>,
}

impl<'a> StepTestState<'a> {
    /// Get a reference to the step test state's cluster.
    #[must_use]
    pub fn cluster(&self) -> &&'a mut MiniCluster {
        &self.cluster
    }

    /// Get a reference to the step test state's cluster.
    #[must_use]
    pub fn cluster_mut(&mut self) -> &mut &'a mut MiniCluster {
        &mut self.cluster
    }

    /// Store the number of Parquet files the catalog has for the mini cluster's namespace.
    /// Call this before a write to be able to tell when a write has been persisted by checking for
    /// a change in this count.
    pub async fn record_num_parquet_files(&mut self) {
        let num_parquet_files = self.get_num_parquet_files().await;

        info!(
            "Recorded count of Parquet files for namespace {}: {num_parquet_files}",
            self.cluster.namespace()
        );
        self.num_parquet_files = Some(num_parquet_files);
    }

    /// Wait for a change (up to a timeout) in the number of Parquet files the catalog has for the
    /// mini cluster's namespacee since the last time the number of Parquet files was recorded,
    /// which indicates persistence has taken place.
    pub async fn wait_for_num_parquet_file_change(&mut self, expected_increase: usize) {
        let retry_duration = Duration::from_secs(MAX_QUERY_RETRY_TIME_SEC);
        let num_parquet_files = self.num_parquet_files.expect(
            "No previous number of Parquet files recorded! \
                Use `Step::RecordNumParquetFiles` before `Step::WaitForPersisted`.",
        );
        let expected_count = num_parquet_files + expected_increase;

        tokio::time::timeout(retry_duration, async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                let current_count = self.get_num_parquet_files().await;
                if current_count >= expected_count {
                    info!(
                        "Success; Parquet file count is now {current_count} \
                        which is at least {expected_count}"
                    );
                    // Reset the saved value to require recording before waiting again
                    self.num_parquet_files = None;
                    return;
                }
                info!(
                    "Retrying; Parquet file count is still {current_count} \
                    which is less than {expected_count}"
                );

                interval.tick().await;
            }
        })
        .await
        .expect("did not get additional Parquet files in the catalog");
    }

    /// Ask the catalog service how many Parquet files it has for the mini cluster's namespace.
    async fn get_num_parquet_files(&self) -> usize {
        let connection = self.cluster.router().router_grpc_connection();
        let mut catalog_client = influxdb_iox_client::catalog::Client::new(connection);

        catalog_client
            .get_parquet_files_by_namespace(self.cluster.namespace())
            .await
            .map(|parquet_files| parquet_files.len())
            .unwrap_or_default()
    }

    /// waits for `MAX_QUERY_RETRY_TIME_SEC` for the database to
    /// report exactly `expected` for its partition keys
    async fn wait_for_partition_keys(
        &self,
        table_name: &str,
        namespace_name: &Option<String>,
        expected: &[&str],
    ) {
        let retry_duration = Duration::from_secs(MAX_QUERY_RETRY_TIME_SEC);
        let partition_keys = tokio::time::timeout(retry_duration, async {
            loop {
                let mut partition_keys = self
                    .cluster()
                    .partition_keys(table_name, namespace_name.clone())
                    .await;
                partition_keys.sort();
                info!("====Read partition keys: {partition_keys:?}");

                if partition_keys == *expected {
                    return partition_keys;
                }
            }
        })
        .await
        .expect("did not get expected partition keys before timeout");

        assert_eq!(partition_keys, *expected);
    }
}

/// Function used for custom [`Step`]s.
///
/// It is an async function that receives a mutable reference to [`StepTestState`].
///
/// Example of creating one (note the `boxed()` call):
/// ```
/// use futures::FutureExt;
/// use futures::future::BoxFuture;
/// use test_helpers_end_to_end::{FCustom, StepTestState};
///
/// let custom_function: FCustom = Box::new(|state: &mut StepTestState| {
///   async move {
///     // access the cluster:
///     let cluster = state.cluster();
///     // Your potentially async code here
///   }.boxed()
/// });
/// ```
pub type FCustom =
    Box<dyn for<'b> Fn(&'b mut StepTestState<'_>) -> BoxFuture<'b, ()> + Send + Sync>;

/// Function to do custom validation on metrics. Expected to panic on validation failure.
pub(crate) type MetricsValidationFn = Box<dyn Fn(&mut StepTestState<'_>, String) + Send + Sync>;

/// Possible test steps that a test can perform
#[allow(missing_debug_implementations)]
pub enum Step {
    /// Writes the specified line protocol to the `/api/v2/write`
    /// endpoint, assert the data was written successfully
    WriteLineProtocol(String),

    /// Writes the specified line protocol to the `/api/v2/write` endpoint; assert the request
    /// returned an error with the given code
    WriteLineProtocolExpectingError {
        line_protocol: String,
        expected_error_code: StatusCode,
        expected_error_message: String,
        expected_line_number: Option<usize>,
    },

    /// Writes the specified line protocol to the `/api/v2/write` endpoint
    /// using the specified authorization header, assert the data was
    /// written successfully.
    WriteLineProtocolWithAuthorization {
        line_protocol: String,
        authorization: String,
    },

    /// Ask the catalog service how many Parquet files it has for this cluster's namespace. Do this
    /// before a write where you're interested in when the write has been persisted to Parquet;
    /// then after the write use `WaitForPersisted` to observe the change in the number of Parquet
    /// files from the value this step recorded.
    RecordNumParquetFiles,

    /// Query the catalog service for how many parquet files it has for this
    /// cluster's namespace, asserting the value matches expected.
    AssertNumParquetFiles { expected: usize },

    /// Ask the ingester to persist immediately through the persist service gRPC API
    Persist,

    /// Wait for all previously written data to be persisted by observing an increase in the number
    /// of Parquet files in the catalog as specified for this cluster's namespace.
    WaitForPersisted { expected_increase: usize },

    /// Set the namespace retention interval to a retention period,
    /// specified in ns relative to `now()`.  `None` represents infinite retention
    /// (i.e. never drop data).
    SetRetention(Option<i64>),

    /// Run one compaction operation and wait for it to finish, expecting success.
    Compact,

    /// Run one compaction operation and wait for it to finish, expecting an error that matches
    /// the specified message.
    CompactExpectingError { expected_message: String },

    /// Run a SQL query using the FlightSQL interface and verify that the
    /// results match the expected results using the
    /// `assert_batches_eq!` macro
    Query {
        sql: String,
        expected: Vec<&'static str>,
    },

    /// Run SQL query using the FlightSQL interface, replacing `$placeholder` variables
    /// with the supplied parameters. Then verify that the
    /// results match the expected results using the
    /// `assert_batches_eq!` macro
    QueryWithParams {
        sql: String,
        params: HashMap<String, StatementParam>,
        expected: Vec<&'static str>,
    },

    /// Read the SQL queries in the specified file and verify that the results match the expected
    /// results in the corresponding expected file
    QueryAndCompare {
        input_path: PathBuf,
        setup_name: String,
        contents: String,
    },

    /// Run a SQL query that's expected to fail using the FlightSQL interface and verify that the
    /// request returns the expected error code and message
    QueryExpectingError {
        sql: String,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Run SQL query using the FlightSQL interface, replacing `$placeholder` variables
    /// with the supplied parameters. Then verify that the
    /// request returns the expected error code and message
    QueryWithParamsExpectingError {
        sql: String,
        params: HashMap<String, StatementParam>,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Run a SQL query using the FlightSQL interface authorized by the
    /// authorization header. Verify that the
    /// results match the expected results using the `assert_batches_eq!`
    /// macro
    QueryWithAuthorization {
        sql: String,
        authorization: String,
        expected: Vec<&'static str>,
    },

    /// Run a SQL query using the FlightSQL interface with the `iox-debug` header set.
    /// Verify that the
    /// results match the expected results using the `assert_batches_eq!`
    /// macro
    QueryWithDebug {
        sql: String,
        expected: Vec<&'static str>,
    },

    /// Run a SQL query using the FlightSQL interface, and then verifies
    /// the results using the provided validation function on the
    /// results.
    ///
    /// The validation function is expected to panic on validation
    /// failure.
    VerifiedQuery {
        sql: String,
        verify: Box<dyn Fn(Vec<RecordBatch>) + Send + Sync>,
    },

    /// Run an InfluxQL query using the FlightSQL interface and verify that the
    /// results match the expected results using the
    /// `assert_batches_eq!` macro
    InfluxQLQuery {
        query: String,
        expected: Vec<&'static str>,
    },

    /// Run an InfluxQL query using the FlightSQL interface, replacing `$placeholder` variables
    /// in the query text with values provided by the params HashMap. Then verify that the
    /// results match the expected results using the `assert_batches_eq!` macro
    InfluxQLQueryWithParams {
        query: String,
        params: HashMap<String, StatementParam>,
        expected: Vec<&'static str>,
    },

    /// Read the InfluxQL queries in the specified file and verify that the results match the
    /// expected results in the corresponding expected file
    InfluxQLQueryAndCompare {
        input_path: PathBuf,
        setup_name: String,
        contents: String,
    },

    /// Run an InfluxQL query that's expected to fail using the FlightSQL interface and verify that
    /// the request returns the expected error code and message
    InfluxQLExpectingError {
        query: String,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Run InfluxQL query using the FlightSQL interface, replacing `$placeholder` variables
    /// with the supplied parameters. Then verify that the
    /// request returns the expected error code and message
    InfluxQLWithParamsExpectingError {
        query: String,
        params: HashMap<String, StatementParam>,
        expected_error_code: tonic::Code,
        expected_message: String,
    },

    /// Run an InfluxQL query using the FlightSQL interface including an
    /// authorization header. Verify that the results match the expected
    /// results using the `assert_batches_eq!` macro.
    InfluxQLQueryWithAuthorization {
        query: String,
        authorization: String,
        expected: Vec<&'static str>,
    },

    /// Runs an http request against the ParquetCache server, in order to check state.
    ParquetCacheServerState {
        expected_state: String,
        expected_curr_version: usize,
        expected_next_version: usize,
    },

    /// Expects a timeout on the state request
    ///
    /// This occurs when InstanceState::Pending.
    ParquetCacheServerStateTimeout,

    /// Set the contents of the ParquetCache instance configmap
    ParquetCacheSetConfigmap {
        path: String,
        contents: serde_json::Value,
    },

    /// Wait until the warming is complete, and an instance is running.
    ParquetCacheSetServerStateWaitUntilWarmingComplete,

    /// Wait until specific version is current.
    ParquetCacheSetServerStateWaitUntilVersion { version: usize },

    /// Wait until the data cache write-back contains a certain number of files.
    ParquetCacheWaitUntilFileCountExists {
        count: usize,
        cache_store_path: String,
    },

    /// Confirm the existence, or lack thereof, of an object key within the ParquetCache.
    ParquetCacheExistenceCheck {
        location: String,
        expected: StatusCode,
    },

    /// Read and verify partition keys for a given table
    PartitionKeys {
        table_name: String,
        namespace_name: Option<String>,
        expected: Vec<&'static str>,
    },

    /// Attempt to gracefully shutdown all running ingester instances.
    ///
    /// This step blocks until all ingesters have gracefully stopped, or at
    /// least [`GRACEFUL_SERVER_STOP_TIMEOUT`] elapses before they are killed.
    ///
    /// [`GRACEFUL_SERVER_STOP_TIMEOUT`]:
    ///     crate::server_fixture::GRACEFUL_SERVER_STOP_TIMEOUT
    GracefulStopIngesters,

    /// Retrieve the metrics and verify the results using the provided
    /// validation function.
    ///
    /// The validation function is expected to panic on validation
    /// failure.
    VerifiedMetrics(MetricsValidationFn),

    /// A custom step that can be used to implement special cases that
    /// are only used once.
    Custom(FCustom),
}

impl AsRef<Self> for Step {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a, S> StepTest<'a, S>
where
    S: AsRef<Step> + Send,
{
    /// Create a new test that runs each `step`, in sequence, against
    /// `cluster` panic'ing if any step fails
    pub fn new<I>(cluster: &'a mut MiniCluster, steps: I) -> Self
    where
        I: IntoIterator<Item = S> + Send + Sync + 'a,
        <I as IntoIterator>::IntoIter: Send + Sync,
    {
        Self {
            cluster,
            steps: Box::new(steps.into_iter()),
        }
    }

    /// run the test.
    pub async fn run(self) {
        let Self { cluster, steps } = self;

        let mut state = StepTestState {
            cluster,
            num_parquet_files: Default::default(),
        };

        for (i, step) in steps.enumerate() {
            info!("**** Begin step {} *****", i);
            match step.as_ref() {
                Step::WriteLineProtocol(line_protocol) => {
                    info!(
                        "====Begin writing line protocol to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = state.cluster.write_to_router(line_protocol, None).await;
                    let status = response.status();
                    let body = hyper::body::to_bytes(response.into_body())
                        .await
                        .expect("reading response body");
                    assert!(
                        status == StatusCode::NO_CONTENT,
                        "Invalid response code while writing line protocol:\n\nLine Protocol:\n{}\n\nExpected Status: {}\nActual Status: {}\n\nBody:\n{:?}",
                        line_protocol,
                        StatusCode::NO_CONTENT,
                        status,
                        body,
                    );
                    info!("====Done writing line protocol");
                }
                Step::WriteLineProtocolExpectingError {
                    line_protocol,
                    expected_error_code,
                    expected_error_message,
                    expected_line_number,
                } => {
                    info!(
                        "====Begin writing line protocol expecting error to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = state.cluster.write_to_router(line_protocol, None).await;
                    assert_eq!(response.status(), *expected_error_code);

                    let body: serde_json::Value = serde_json::from_slice(
                        &hyper::body::to_bytes(response.into_body())
                            .await
                            .expect("should be able to read response body"),
                    )
                    .expect("response body should be valid json");

                    assert_matches::assert_matches!(
                        body["message"],
                        serde_json::Value::String(ref s) if s.contains(expected_error_message),
                        "error message did not match: expected '{}' to contain '{}'",
                        body["message"],
                        expected_error_message
                    );

                    match expected_line_number {
                        Some(line) => {
                            assert_matches::assert_matches!(
                                body["line"],
                                serde_json::Value::Number(ref n) if n == &serde_json::Number::from(*line),
                                "error line did not match: expected '{}' to be '{}'",
                                body["line"],
                                line
                            );
                        }
                        None => {
                            assert!(
                                !body.as_object().unwrap().contains_key("line"),
                                "error line should not be present"
                            );
                        }
                    };

                    info!("====Done writing line protocol expecting error");
                }
                Step::WriteLineProtocolWithAuthorization {
                    line_protocol,
                    authorization,
                } => {
                    info!(
                        "====Begin writing line protocol (authenticated) to v2 HTTP API:\n{}",
                        line_protocol
                    );
                    let response = state
                        .cluster
                        .write_to_router(line_protocol, Some(authorization))
                        .await;
                    assert_eq!(response.status(), StatusCode::NO_CONTENT);
                    info!("====Done writing line protocol");
                }
                // Get the current number of Parquet files in the cluster's namespace before
                // starting a new write so we can observe a change when waiting for persistence.
                Step::RecordNumParquetFiles => {
                    state.record_num_parquet_files().await;
                }
                Step::AssertNumParquetFiles { expected } => {
                    let have_files = state.get_num_parquet_files().await;
                    assert_eq!(have_files, *expected);
                }
                // Ask the ingesters to persist immediately through the persist service gRPC API
                Step::Persist => {
                    state.cluster().persist_ingesters().await;
                }
                Step::WaitForPersisted { expected_increase } => {
                    info!("====Begin waiting for a change in the number of Parquet files");
                    state
                        .wait_for_num_parquet_file_change(*expected_increase)
                        .await;
                    info!("====Done waiting for a change in the number of Parquet files");
                }
                Step::Compact => {
                    info!("====Begin running compaction");
                    state.cluster.run_compaction().unwrap();
                    info!("====Done running compaction");
                }
                Step::CompactExpectingError { expected_message } => {
                    info!("====Begin running compaction expected to error");
                    let err = state.cluster.run_compaction().unwrap_err();

                    assert_contains!(err, expected_message);

                    info!("====Done running");
                }

                Step::SetRetention(retention_period_ns) => {
                    info!("====Begin setting retention period to {retention_period_ns:?}");
                    let namespace = state.cluster().namespace();
                    let router_connection = state.cluster().router().router_grpc_connection();
                    let mut client = influxdb_iox_client::namespace::Client::new(router_connection);
                    client
                        .update_namespace_retention(namespace, *retention_period_ns)
                        .await
                        .expect("Error updating retention period");
                    info!("====Done setting retention period");
                }
                Step::Query { sql, expected } => {
                    info!("====Begin running SQL query: {}", sql);
                    // run query
                    let (mut batches, schema) = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                        None,
                        false,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::QueryWithParams {
                    sql,
                    params,
                    expected,
                } => {
                    info!("====Begin running SQL query: {}", sql);
                    info!("params: {:?}", params);
                    // run query
                    let (mut batches, schema) = run_sql_with_params(
                        sql,
                        state.cluster.namespace(),
                        params.clone(),
                        state.cluster.querier().querier_grpc_connection(),
                        None,
                        false,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::QueryAndCompare {
                    input_path,
                    setup_name,
                    contents,
                } => {
                    info!(
                        "====Begin running SQL queries in file {}",
                        input_path.display()
                    );
                    snapshot_comparison::run(
                        state.cluster,
                        input_path.into(),
                        setup_name.into(),
                        contents.into(),
                        Language::Sql,
                    )
                    .await
                    .unwrap();
                    info!("====Done running SQL queries");
                }
                Step::QueryExpectingError {
                    sql,
                    expected_error_code,
                    expected_message,
                } => {
                    info!("====Begin running SQL query expected to error: {}", sql);

                    let err = try_run_sql(
                        sql,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                        None,
                        false,
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, *expected_error_code, Some(expected_message));

                    info!("====Done running");
                }
                Step::QueryWithParamsExpectingError {
                    sql,
                    params,
                    expected_error_code,
                    expected_message,
                } => {
                    info!("====Begin running SQL query expected to error: {}", sql);

                    let err = try_run_sql_with_params(
                        sql,
                        state.cluster().namespace(),
                        params.clone(),
                        state.cluster().querier().querier_grpc_connection(),
                        None,
                        false,
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, *expected_error_code, Some(expected_message));

                    info!("====Done running");
                }
                Step::QueryWithAuthorization {
                    sql,
                    authorization,
                    expected,
                } => {
                    info!("====Begin running SQL query (authenticated): {}", sql);
                    // run query
                    let (mut batches, schema) = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                        Some(authorization.as_str()),
                        false,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::QueryWithDebug { sql, expected } => {
                    info!("====Begin running SQL query (w/ iox-debug): {}", sql);
                    // run query
                    let (mut batches, schema) = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                        None,
                        true,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::VerifiedQuery { sql, verify } => {
                    info!("====Begin running SQL verified query: {}", sql);
                    // run query
                    let (batches, _schema) = run_sql(
                        sql,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                        None,
                        true,
                    )
                    .await;
                    verify(batches);
                    info!("====Done running");
                }
                Step::InfluxQLQuery { query, expected } => {
                    info!("====Begin running InfluxQL query: {}", query);
                    // run query
                    let (mut batches, schema) = run_influxql(
                        query,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                        None,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::InfluxQLQueryWithParams {
                    query,
                    expected,
                    params,
                } => {
                    info!("====Begin running InfluxQL query: {}", query);
                    info!("params: {:?}", params);
                    // run query
                    let (mut batches, schema) = run_influxql_with_params(
                        query,
                        state.cluster.namespace(),
                        params.clone(),
                        state.cluster.querier().querier_grpc_connection(),
                        None,
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::InfluxQLQueryAndCompare {
                    input_path,
                    setup_name,
                    contents,
                } => {
                    info!(
                        "====Begin running InfluxQL queries in file {}",
                        input_path.display()
                    );
                    snapshot_comparison::run(
                        state.cluster,
                        input_path.into(),
                        setup_name.into(),
                        contents.into(),
                        Language::InfluxQL,
                    )
                    .await
                    .unwrap();
                    info!("====Done running InfluxQL queries");
                }
                Step::InfluxQLExpectingError {
                    query,
                    expected_error_code,
                    expected_message,
                } => {
                    info!(
                        "====Begin running InfluxQL query expected to error: {}",
                        query
                    );

                    let err = try_run_influxql(
                        query,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                        None,
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, *expected_error_code, Some(expected_message));

                    info!("====Done running");
                }
                Step::InfluxQLWithParamsExpectingError {
                    query,
                    params,
                    expected_error_code,
                    expected_message,
                } => {
                    info!(
                        "====Begin running InfluxQL query expected to error: {}",
                        query
                    );
                    info!("params: {:?}", params);
                    let err = try_run_influxql_with_params(
                        query,
                        state.cluster().namespace(),
                        params.clone(),
                        state.cluster().querier().querier_grpc_connection(),
                        None,
                    )
                    .await
                    .unwrap_err();

                    check_flight_error(err, *expected_error_code, Some(expected_message));

                    info!("====Done running");
                }
                Step::InfluxQLQueryWithAuthorization {
                    query,
                    authorization,
                    expected,
                } => {
                    info!("====Begin running InfluxQL query: {}", query);
                    // run query
                    let (mut batches, schema) = run_influxql(
                        query,
                        state.cluster.namespace(),
                        state.cluster.querier().querier_grpc_connection(),
                        Some(authorization),
                    )
                    .await;
                    batches.push(RecordBatch::new_empty(schema));
                    assert_batches_sorted_eq!(expected, &batches);
                    info!("====Done running");
                }
                Step::ParquetCacheServerState {
                    expected_state,
                    expected_curr_version,
                    expected_next_version,
                } => {
                    let resp = state
                        .cluster
                        .parquet_cache_state()
                        .await
                        .expect("should receive response from parquet_cache server");

                    let response = String::from_utf8(
                        hyper::body::to_bytes(resp.into_body())
                            .await
                            .expect("should read response body")
                            .into(),
                    )
                    .expect("response body is invalid utf8");
                    let response = serde_json::from_str::<Value>(&response)
                        .expect("response body is invalid json");

                    let state = response.get("state").expect("should have state");
                    assert_matches::assert_matches!(
                        state,
                        Value::String(s) if s == expected_state,
                        "expected state to be {}, but found {}",
                        expected_state, state
                    );

                    let curr_ver = response
                        .get("current_node_set_revision")
                        .expect("should have current_node_set_revision")
                        .to_string();
                    assert_eq!(
                        curr_ver,
                        expected_curr_version.to_string(),
                        "expected current_node_set_revision to be {}, but found {}",
                        expected_curr_version,
                        curr_ver
                    );

                    let next_ver = response
                        .get("next_node_set_revision")
                        .expect("should have next_node_set_revision")
                        .to_string();
                    assert_eq!(
                        next_ver,
                        expected_next_version.to_string(),
                        "expected next_node_set_revision to be {}, but found {}",
                        expected_next_version,
                        next_ver
                    );
                }
                Step::ParquetCacheServerStateTimeout => {
                    let resp = state.cluster.parquet_cache_state().await;
                    assert_matches::assert_matches!(
                        resp,
                        Err(e) if e.to_string().contains("timeout"),
                        "expected timeout (due to failed poll_ready), but found {:?}",
                        resp
                    );
                }
                Step::ParquetCacheSetConfigmap { path, contents } => {
                    let path = std::path::Path::new(path);
                    let tempdir_path = path.parent().expect("should have parent");
                    let mut new_configmap = tempfile::NamedTempFile::new_in(tempdir_path)
                        .expect("should create tempfile");
                    writeln!(new_configmap, "{}", contents)
                        .expect("should write keyspace definition to configfile");
                    std::fs::copy(new_configmap.path(), path)
                        .expect("should overwrite configmap file");
                }
                Step::ParquetCacheSetServerStateWaitUntilWarmingComplete => {
                    parquet_cache_server_wait_for_warming(&mut state).await;
                }
                Step::ParquetCacheSetServerStateWaitUntilVersion { version } => {
                    parquet_cache_server_wait_for_version(version, &mut state).await;
                }
                Step::ParquetCacheWaitUntilFileCountExists {
                    count,
                    cache_store_path,
                } => {
                    parquet_cache_server_wait_for_writeback(count, cache_store_path).await;
                }
                Step::ParquetCacheExistenceCheck { location, expected } => {
                    let resp = state
                        .cluster
                        .parquet_cache_fetch(location)
                        .await
                        .expect("should receive response from parquet_cache server");
                    assert_eq!(
                        resp.status(),
                        *expected,
                        "expected status code {}, but found {}",
                        expected,
                        resp.status()
                    );
                }
                Step::PartitionKeys {
                    table_name,
                    namespace_name,
                    expected,
                } => {
                    info!("====Persist ingesters to ensure catalog partition records exist");
                    state
                        .cluster()
                        .persist_ingesters_by_namespace(namespace_name.clone())
                        .await;

                    info!("====Begin reading partition keys for table: {}", table_name);
                    state
                        .wait_for_partition_keys(table_name, namespace_name, expected)
                        .await;
                    info!("====Done reading partition keys");
                }
                Step::GracefulStopIngesters => {
                    info!("====Gracefully stop all ingesters");

                    state.cluster_mut().gracefully_stop_ingesters().await;
                }
                Step::VerifiedMetrics(verify) => {
                    info!("====Begin validating metrics");

                    let cluster = state.cluster();
                    let http_base = cluster.router().router_http_base();
                    let url = format!("{http_base}/metrics");

                    let client = reqwest::Client::new();
                    let metrics = client.get(&url).send().await.unwrap().text().await.unwrap();

                    verify(&mut state, metrics);

                    info!("====Done validating metrics");
                }
                Step::Custom(f) => {
                    info!("====Begin custom step");
                    f(&mut state).await;
                    info!("====Done custom step");
                }
            }
        }
    }
}

async fn parquet_cache_server_wait_for_warming(state: &mut StepTestState<'_>) {
    let now = tokio::time::Instant::now();

    while now.elapsed().as_secs() < 10 {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        if let Ok(resp) = state.cluster.parquet_cache_state().await {
            let response = String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .expect("should read response body")
                    .into(),
            )
            .expect("response body is invalid utf8");
            let response =
                serde_json::from_str::<Value>(&response).expect("response body is invalid json");

            if let Value::String(state) = response.get("state").expect("should have state") {
                match state.as_str() {
                    "warming" => continue,
                    "running" => return,
                    other => panic!("unexpected state, found {}", other),
                }
            } else {
                unreachable!("should only receive a valid state response");
            }
        };
    }
    panic!("parquet cache server never finished warming");
}

async fn parquet_cache_server_wait_for_version(version: &usize, state: &mut StepTestState<'_>) {
    let now = tokio::time::Instant::now();

    while now.elapsed().as_secs() < 5 {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        if let Ok(resp) = state.cluster.parquet_cache_state().await {
            let response = String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .expect("should read response body")
                    .into(),
            )
            .expect("response body is invalid utf8");
            let response =
                serde_json::from_str::<Value>(&response).expect("response body is invalid json");

            if let Value::Number(current_version) = response
                .get("current_node_set_revision")
                .expect("should have current_node_set_revision")
            {
                if current_version.as_u64().unwrap() == *version as u64 {
                    return;
                } else {
                    continue;
                }
            } else {
                unreachable!("should only receive a valid current_node_set_revision response");
            }
        };
    }
    panic!("parquet cache server never got the version {}", version);
}

async fn parquet_cache_server_wait_for_writeback(count: &usize, cache_store_path: &String) {
    let retry_duration = time::Duration::from_secs(1);

    time::timeout(retry_duration, async move {
        let mut interval = time::interval(time::Duration::from_millis(10));
        loop {
            let mut file_count = 0;
            let mut files =
                std::fs::read_dir(cache_store_path).expect("local cache store path exists");
            while let Some(Ok(patition_key_dir)) = files.next() {
                let mut files = std::fs::read_dir(patition_key_dir.path().display().to_string())
                    .expect("partition dir should exist");
                while let Some(Ok(_object_location)) = files.next() {
                    file_count += 1;
                }
            }

            if &file_count == count {
                return;
            }
            interval.tick().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "parquet cache server never got write-backs for {} number of objects",
            count
        )
    });
}
