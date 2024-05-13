//! Client helpers for writing end to end ng tests
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use assert_cmd::Command;
use data_types::{NamespaceId, TableId};
use dml::{DmlMeta, DmlWrite};
use futures::TryStreamExt;
use http::{Method, Response};
use hyper::{Body, Client, Request};
use influxdb_iox_client::{
    connection::Connection,
    ingester::generated_types::{write_service_client::WriteServiceClient, WriteRequest},
};
use iox_query_params::StatementParam;
use mutable_batch_lp::lines_to_batches;
use mutable_batch_pb::encode::encode_write;
use predicates::prelude::*;
use std::{fmt::Display, time::Duration};
use tonic::IntoRequest;

use crate::TestConfig;

/// Writes the line protocol to the write_base/api/v2/write endpoint (typically on the router)
pub async fn write_to_router(
    line_protocol: impl Into<String> + Send,
    org: impl AsRef<str> + Send,
    bucket: impl AsRef<str> + Send,
    write_base: impl AsRef<str> + Send,
    authorization: Option<&str>,
) -> Response<Body> {
    let client = Client::new();
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}",
        write_base.as_ref(),
        org.as_ref(),
        bucket.as_ref()
    );

    let mut builder = Request::builder().uri(url).method("POST");
    if let Some(authorization) = authorization {
        builder = builder.header(hyper::header::AUTHORIZATION, authorization);
    };
    let request = builder
        .body(Body::from(line_protocol.into()))
        .expect("failed to construct HTTP request");

    client
        .request(request)
        .await
        .expect("http error sending write")
}

/// Writes the line protocol to the WriteService endpoint (typically on the ingester)
pub async fn write_to_ingester(
    line_protocol: impl Into<String> + Send,
    namespace_id: NamespaceId,
    table_id: TableId,
    ingester_connection: Connection,
) {
    let line_protocol = line_protocol.into();
    let writes = lines_to_batches(&line_protocol, 0).unwrap();
    let writes = writes
        .into_iter()
        .map(|(_name, data)| (table_id, data))
        .collect();

    let mut client = WriteServiceClient::new(ingester_connection.into_grpc_connection());

    let op = DmlWrite::new(
        namespace_id,
        writes,
        "1970-01-01".into(),
        DmlMeta::unsequenced(None),
    );

    client
        .write(
            tonic::Request::new(WriteRequest {
                payload: Some(encode_write(namespace_id.get(), &op)),
            })
            .into_request(),
        )
        .await
        .unwrap();
}

/// Run a compaction job once on a given partition, and error if the job panics
/// (e.g. if the catalog or parquet file is incorrect, or uncontrolled OOMing).
///
/// If the partition has no work to do, or successfully compacts the given partition,
/// then the compactor run is successful.
pub async fn run_compactor_once(config: TestConfig, data_dir: &str, partition_id: i64) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("compactor")
        .env(
            "INFLUXDB_IOX_CATALOG_DSN",
            config
                .dsn()
                .as_ref()
                .unwrap_or(&format!("sqlite://{data_dir}/catalog.sqlite")),
        )
        .env(
            "INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME",
            config.catalog_schema_name(),
        )
        .envs(config.env())
        .env(
            "INFLUXDB_IOX_BIND_ADDR",
            config.addrs().compactor_http_api().bind_addr().to_string(),
        )
        .env(
            "INFLUXDB_IOX_GRPC_BIND_ADDR",
            config.addrs().compactor_grpc_api().bind_addr().to_string(),
        )
        .env(
            "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
            config
                .addrs()
                .compactor_gossip_api()
                .bind_addr()
                .to_string(),
        )
        .env(
            "INFLUXDB_IOX_GOSSIP_SEED_LIST",
            config
                .addrs()
                .all_gossip_apis()
                .into_iter()
                .map(|a| a.bind_addr().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg("--object-store=file")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--force-compact-partition")
        .arg(format!("{partition_id}"))
        .arg("--compaction-process-once")
        .assert()
        .stdout(
            predicate::str::contains("cannot find column names for sort key")
                .not()
                .and(predicate::str::contains("clean compactor shutdown: JoinError::Panic").not()),
        )
        .success();
}

/// Performs http request to the specified endpoint.
pub async fn try_request_http(
    http_base: impl AsRef<str> + Send,
    path_and_query: impl AsRef<str> + Send,
    method: Method,
    body: impl Into<Body> + Send,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send>> {
    let client = Client::new();
    let request = Request::builder()
        .uri(format!("{}{}", http_base.as_ref(), path_and_query.as_ref(),))
        .method(method)
        .body(body.into())
        .map_err(|e| Box::new(e) as Box<_>)?;

    match tokio::time::timeout(Duration::from_secs(5), client.request(request)).await {
        Ok(res) => res.map_err(|e| Box::new(e) as Box<_>),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout",
        ))),
    }
}

/// Runs a SQL query using the flight API on the specified connection.
pub async fn try_run_sql(
    sql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    try_run_sql_with_params(
        sql_query,
        namespace,
        [],
        querier_connection,
        authorization,
        with_debug,
    )
    .await
}

/// Runs a SQL query using the flight API on the specified connection.
pub async fn try_run_sql_with_params(
    sql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);
    if with_debug {
        client.add_header("iox-debug", "true").unwrap();
    }
    if let Some(authorization) = authorization {
        client.add_header("authorization", authorization).unwrap();
    }

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    let mut stream = client
        .query(namespace)
        .sql(sql_query.into())
        .with_params(params)
        .run()
        .await?;

    let batches = (&mut stream).try_collect().await?;

    // read schema AFTER collection, otherwise the stream does not have the schema data yet
    let schema = stream
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema)?;

    Ok((batches, schema))
}

/// Runs a InfluxQL query using the flight API on the specified connection.
pub async fn try_run_influxql(
    influxql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    try_run_influxql_with_params(
        influxql_query,
        namespace,
        [],
        querier_connection,
        authorization,
    )
    .await
}

pub async fn try_run_influxql_with_params(
    influxql_query: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> Result<(Vec<RecordBatch>, SchemaRef), influxdb_iox_client::flight::Error> {
    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);
    if let Some(authorization) = authorization {
        client.add_header("authorization", authorization).unwrap();
    }

    // Test the client handshake implementation
    // Normally this would be done one per connection, not per query
    client.handshake().await?;

    let mut stream = client
        .query(namespace)
        .influxql(influxql_query.into())
        .with_params(params)
        .run()
        .await?;

    let batches = (&mut stream).try_collect().await?;

    // read schema AFTER collection, otherwise the stream does not have the schema data yet
    let schema = stream
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema)?;

    Ok((batches, schema))
}

/// Runs a SQL query using the flight API on the specified connection.
///
/// Use [`try_run_sql`] if you want to check the error manually.
pub async fn run_sql(
    sql: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_sql(
        sql,
        namespace,
        querier_connection,
        authorization,
        with_debug,
    )
    .await
    .expect("Error executing sql query")
}

/// Runs a SQL query using the flight API on the specified connection.
///
/// Use [`try_run_sql`] if you want to check the error manually.
pub async fn run_sql_with_params(
    sql: impl Into<String> + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
    with_debug: bool,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_sql_with_params(
        sql,
        namespace,
        params,
        querier_connection,
        authorization,
        with_debug,
    )
    .await
    .expect("Error executing sql query")
}

/// Runs an InfluxQL query using the flight API on the specified connection.
///
/// Use [`try_run_influxql`] if you want to check the error manually.
pub async fn run_influxql(
    influxql: impl Into<String> + Clone + Display + Send,
    namespace: impl Into<String> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_influxql(
        influxql.clone(),
        namespace,
        querier_connection,
        authorization,
    )
    .await
    .unwrap_or_else(|_| panic!("Error executing InfluxQL query: {influxql}"))
}

/// Runs an InfluxQL query using the flight API on the specified connection.
///
/// Use [`try_run_influxql`] if you want to check the error manually.
pub async fn run_influxql_with_params(
    influxql: impl Into<String> + Clone + Display + Send,
    namespace: impl Into<String> + Send,
    params: impl IntoIterator<Item = (String, StatementParam)> + Send,
    querier_connection: Connection,
    authorization: Option<&str>,
) -> (Vec<RecordBatch>, SchemaRef) {
    try_run_influxql_with_params(
        influxql.clone(),
        namespace,
        params,
        querier_connection,
        authorization,
    )
    .await
    .unwrap_or_else(|_| panic!("Error executing InfluxQL query: {influxql}"))
}
