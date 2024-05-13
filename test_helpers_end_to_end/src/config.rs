use crate::{addrs::BindAddresses, ServerType, UdpCapture};
use http::{header::HeaderName, HeaderValue};
use observability_deps::tracing::info;
use rand::Rng;
use std::{collections::HashMap, num::NonZeroUsize, path::Path, sync::Arc};
use tempfile::TempDir;

/// Options for creating test servers (`influxdb_iox` processes)
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// environment variables to pass to server process. HashMap to avoid duplication
    env: HashMap<String, String>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,

    /// Server type
    server_type: ServerType,

    /// Catalog DSN value. Required unless you're running all-in-one in ephemeral mode.
    dsn: Option<String>,

    /// Catalog schema name
    catalog_schema_name: String,

    /// Object store directory, if needed.
    object_store_dir: Option<Arc<TempDir>>,

    /// WAL directory, if needed.
    wal_dir: Option<Arc<TempDir>>,

    /// Catalog directory, if needed
    catalog_dir: Option<Arc<TempDir>>,

    /// Which ports this server should use
    addrs: Arc<BindAddresses>,

    /// Wait for server to be ready during creation.
    wait_for_ready: bool,
}

impl TestConfig {
    /// Create a new TestConfig. Tests should use one of the specific
    /// configuration setup below, such as [new_router](Self::new_router).
    pub fn new(
        server_type: ServerType,
        dsn: Option<String>,
        catalog_schema_name: impl Into<String>,
    ) -> Self {
        let catalog_schema_name = catalog_schema_name.into();

        let (dsn, catalog_dir) = specialize_dsn_if_needed(dsn, &catalog_schema_name);

        Self {
            env: HashMap::new(),
            client_headers: vec![],
            server_type,
            dsn,
            catalog_schema_name,
            object_store_dir: None,
            wal_dir: None,
            catalog_dir,
            addrs: Arc::new(BindAddresses::default()),
            wait_for_ready: true,
        }
    }

    /// Creates a new TestConfig of `server_type` with the same catalog as `other`
    fn new_with_existing_catalog(server_type: ServerType, other: &Self) -> Self {
        Self::new(
            server_type,
            other.dsn.clone(),
            other.catalog_schema_name.clone(),
        )
        // also copy a reference to the temp dir, if any, so it isn't
        // deleted too soon
        .with_catalog_dir(other.catalog_dir.as_ref().cloned())
    }

    /// Create new catalog node w/o peers
    fn new_catalog(dsn: Option<String>, catalog_schema_name: String) -> Self {
        Self::new(ServerType::Catalog, dsn, catalog_schema_name)
            .with_env("INFLUXDB_IOX_CATALOG_CACHE_WARMUP_DELAY", "100ms")
            // disable bypass detection by default
            .with_bypass_detection(false)
    }

    /// Enable/disable catalog cache bypass detection
    pub fn with_bypass_detection(self, enable: bool) -> Self {
        assert_eq!(self.server_type(), ServerType::Catalog);

        self.with_env(
            "INFLUXDB_IOX_CATALOG_CACHE_BYPASS_DETECTION_ENABLED",
            enable.to_string(),
        )
        .with_env(
            "INFLUXDB_IOX_CATALOG_CACHE_BYPASS_DETECTION_CHECK_INTERVAL",
            "100ms",
        )
        .with_env(
            "INFLUXDB_IOX_CATALOG_CACHE_BYPASS_DETECTION_RECOVERY_DURATION",
            "1s",
        )
        .with_env(
            "INFLUXDB_IOX_CATALOG_CACHE_BYPASS_DETECTION_CLEAR_INTERVAL",
            "500ms",
        )
    }

    /// Create a triplet of catalog cache nodes.
    ///
    /// Also returns the catalog DSN
    pub fn catalog_nodes(dsn: impl Into<String>) -> ([Self; 3], String) {
        let catalog_schema_name = random_catalog_schema_name();
        Self::catalog_nodes_with_catalog_schema_name(dsn, catalog_schema_name)
    }

    pub fn catalog_nodes_with_catalog_schema_name(
        dsn: impl Into<String>,
        catalog_schema_name: String,
    ) -> ([Self; 3], String) {
        let dsn = Some(dsn.into());

        let n0 = Self::new_catalog(dsn.clone(), catalog_schema_name.clone());
        let n1 = Self::new_catalog(dsn.clone(), catalog_schema_name.clone());
        let n2 = Self::new_catalog(dsn.clone(), catalog_schema_name.clone());

        let n0 = n0.with_catalog_peers([
            n1.addrs().catalog_http_api().client_base(),
            n2.addrs().catalog_http_api().client_base(),
        ]);
        let n1 = n1.with_catalog_peers([
            n0.addrs().catalog_http_api().client_base(),
            n2.addrs().catalog_http_api().client_base(),
        ]);
        let n2 = n2.with_catalog_peers([
            n0.addrs().catalog_http_api().client_base(),
            n1.addrs().catalog_http_api().client_base(),
        ]);

        let catalog_service_url = format!(
            "{};{};{}",
            n0.addrs().catalog_grpc_api().client_base(),
            n1.addrs().catalog_grpc_api().client_base(),
            n2.addrs().catalog_grpc_api().client_base()
        );

        ([n0, n1, n2], catalog_service_url)
    }

    /// Create a minimal router configuration that doesn't connect to an ingester. If you need a
    /// router that connects to an ingester, call `new_ingester` first and then pass the resulting
    /// `TestConfig` to `new_router`.
    pub fn router_only(dsn: impl Into<String>) -> Self {
        let dsn = Some(dsn.into());
        Self::new(ServerType::Router, dsn, random_catalog_schema_name())
            .with_new_object_store()
            .with_no_router_upstream_wait()
    }

    /// Create a minimal router configuration sharing configuration with the ingester config
    pub fn new_router(ingester_config: &Self) -> Self {
        assert_eq!(ingester_config.server_type(), ServerType::Ingester);

        Self::new_with_existing_catalog(ServerType::Router, ingester_config)
            .with_existing_object_store(ingester_config)
            .with_ingester_addresses(&[ingester_config.ingester_base()])
    }

    /// Create a minimal ingester configuration, using the dsn configuration specified. Set the
    /// persistence options such that it will persist as quickly as possible.
    pub fn new_ingester(dsn: impl Into<String>) -> Self {
        let dsn = Some(dsn.into());
        Self::new(ServerType::Ingester, dsn, random_catalog_schema_name())
            .with_new_object_store()
            .with_new_wal()
            .with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "1")
    }

    /// Create minima ingester config, from another configuration which specifies
    /// the existing catalog and object store.
    pub fn new_ingester_from_config(other: &Self) -> Self {
        Self::new_with_existing_catalog(ServerType::Ingester, other)
            .with_existing_object_store(other)
            .with_new_wal()
            .with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "1")
    }

    /// Create a minimal ingester configuration, using the dsn configuration specified. Set the
    /// persistence options such that it will likely never persist, to be able to test when data
    /// only exists in the ingester's memory.
    pub fn new_ingester_never_persist(dsn: impl Into<String>) -> Self {
        let dsn = Some(dsn.into());
        Self::new(ServerType::Ingester, dsn, random_catalog_schema_name())
            .with_new_object_store()
            .with_new_wal()
            // I didn't run my tests for a day, because that would be too long
            .with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "86400")
    }

    /// Create a minimal parquet cache configuration, using the dsn configuration specified.
    /// Set the provided env for configmap path, local_store dir path, and hostname in order to
    /// test different scenarios.
    pub fn parquet_cache_server_only(
        dsn: impl Into<String>,
        configmap: impl Into<String>,
        local_store: impl Into<String>,
        hostname: impl Into<String>,
    ) -> Self {
        let dsn = Some(dsn.into());
        Self::new(
            ServerType::ParquetCache,
            dsn.clone(),
            random_catalog_schema_name(),
        )
        .with_new_object_store()
        .with_env("INFLUXDB_IOX_PARQUET_CACHE_KEYSPACE_CONFIG_PATH", configmap)
        .with_env("INFLUXDB_IOX_PARQUET_CACHE_LOCAL_DIR", local_store)
        .with_env("HOSTNAME", hostname)
    }

    /// Create another ingester with the same dsn, catalog schema name, and object store, but with
    /// its own WAL directory and own addresses.
    pub fn another_ingester(ingester_config: &Self) -> Self {
        Self {
            env: ingester_config.env.clone(),
            client_headers: ingester_config.client_headers.clone(),
            server_type: ServerType::Ingester,
            dsn: ingester_config.dsn.clone(),
            catalog_schema_name: ingester_config.catalog_schema_name.clone(),
            object_store_dir: None,
            wal_dir: None,
            catalog_dir: ingester_config.catalog_dir.as_ref().cloned(),
            addrs: Arc::new(BindAddresses::default()),
            wait_for_ready: ingester_config.wait_for_ready,
        }
        .with_existing_object_store(ingester_config)
        .with_new_wal()
    }

    /// Create a minimal querier configuration from the specified ingester configuration, using
    /// the same dsn and object store, and pointing at the specified ingester.
    pub fn new_querier(ingester_config: &Self) -> Self {
        assert_eq!(ingester_config.server_type(), ServerType::Ingester);

        Self::new_querier_without_ingester(ingester_config)
            .with_ingester_addresses(&[ingester_config.ingester_base()])
    }

    /// Create a minimal compactor configuration, using the dsn configuration from other
    pub fn new_compactor(other: &Self) -> Self {
        Self::new_with_existing_catalog(ServerType::Compactor, other)
            .with_existing_object_store(other)
    }

    /// Create a minimal querier configuration from the specified ingester configuration, using
    /// the same dsn and object store, but without specifying the ingester addresses
    pub fn new_querier_without_ingester(ingester_config: &Self) -> Self {
        Self::new_with_existing_catalog(ServerType::Querier, ingester_config)
            .with_existing_object_store(ingester_config)
            .with_querier_defaults()
    }

    /// Set querier defaults.
    pub fn with_querier_defaults(self) -> Self {
        self
            // Hard code query threads so query plans do not vary based on environment
            .with_env("INFLUXDB_IOX_NUM_QUERY_THREADS", "4")
            .with_env(
                "INFLUXDB_IOX_DATAFUSION_CONFIG",
                "iox.influxql_metadata_cutoff:1990-01-01T00:00:00Z",
            )
            // fixtures have central catalog cache (or it doesn't matter in the first place)
            .with_env("INFLUXDB_IOX_V2_OPTIMIZE_FOR_CATALOG_CACHE", "true")
    }

    /// Create a minimal all in one configuration
    pub fn new_all_in_one(dsn: Option<String>) -> Self {
        Self::new(ServerType::AllInOne, dsn, random_catalog_schema_name()).with_new_object_store()
    }

    /// Create a minimal all in one configuration with the specified
    /// data directory (`--data_dir = <data_dir>`)
    ///
    /// the data_dir has a file based object store and sqlite catalog
    pub fn new_all_in_one_with_data_dir(data_dir: &Path) -> Self {
        let dsn = None; // use default sqlite catalog in data_dir

        let data_dir_str = data_dir.as_os_str().to_str().unwrap();
        Self::new(ServerType::AllInOne, dsn, random_catalog_schema_name())
            .with_env("INFLUXDB_IOX_DB_DIR", data_dir_str)
    }

    /// Set the number of failed ingester queries before the querier considers
    /// the ingester to be dead.
    pub fn with_querier_circuit_breaker_threshold(self, count: usize) -> Self {
        assert!(count > 0);
        self.with_env(
            "INFLUXDB_IOX_INGESTER_CIRCUIT_BREAKER_THRESHOLD",
            count.to_string(),
        )
    }

    /// Configure tracing capture
    pub fn with_tracing(self, udp_capture: &UdpCapture) -> Self {
        self.with_env("TRACES_EXPORTER", "jaeger")
            .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
            .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
            .with_env(
                "TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
                "custom-trace-header",
            )
            .with_client_header("custom-trace-header", "4:3:2:1")
            .with_env("INFLUXDB_IOX_COMPACTION_PARTITION_TRACE", "all")
    }

    /// Configure a custom debug name for tracing
    pub fn with_tracing_debug_name(self, custom_debug_name: &str) -> Self {
        // setup a custom debug name (to ensure it gets plumbed through)
        self.with_env("TRACES_EXPORTER_JAEGER_DEBUG_NAME", custom_debug_name)
            .with_client_header(custom_debug_name, "some-debug-id")
    }

    pub fn with_ingester_addresses(
        self,
        ingester_addresses: &[impl std::borrow::Borrow<str>],
    ) -> Self {
        self.with_env(
            "INFLUXDB_IOX_INGESTER_ADDRESSES",
            ingester_addresses.join(","),
        )
    }

    pub fn with_rpc_write_replicas(self, rpc_write_replicas: NonZeroUsize) -> Self {
        self.with_env(
            "INFLUXDB_IOX_RPC_WRITE_REPLICAS",
            rpc_write_replicas.get().to_string(),
        )
    }

    pub fn with_no_router_upstream_wait(self) -> Self {
        self.with_env("INFLUXDB_IOX_NO_WAIT_RPC_UPSTREAMS", "true")
    }

    pub fn with_ingester_never_persist(self) -> Self {
        self.with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "86400")
    }

    /// Configure the single tenancy mode, including the authorization server.
    pub fn with_single_tenancy(self, addr: impl Into<String>) -> Self {
        self.with_env("INFLUXDB_IOX_AUTHZ_ADDR", addr)
            .with_env("INFLUXDB_IOX_SINGLE_TENANCY", "true")
    }

    /// Enable partial writes.
    pub fn with_partial_writes(self) -> Self {
        self.with_env("INFLUXDB_IOX_PARTIAL_WRITES_ENABLED", "true")
    }

    // Get the catalog DSN URL if set.
    pub fn dsn(&self) -> &Option<String> {
        &self.dsn
    }

    // Get the catalog postgres schema name
    pub fn catalog_schema_name(&self) -> &str {
        &self.catalog_schema_name
    }

    /// Retrieve the directory used to write WAL files to, if set
    pub fn wal_dir(&self) -> &Option<Arc<TempDir>> {
        &self.wal_dir
    }

    /// Retrieve the directory used for object store, if set
    pub fn object_store_dir(&self) -> &Option<Arc<TempDir>> {
        &self.object_store_dir
    }

    // copy a reference to the catalog temp dir, if any
    fn with_catalog_dir(mut self, catalog_dir: Option<Arc<TempDir>>) -> Self {
        self.catalog_dir = catalog_dir;
        self
    }

    /// add a name=value environment variable when starting the server
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    /// Remove an environment variable from the config
    fn without_env(mut self, name: impl Into<String>) -> Self {
        self.env.remove(&name.into());
        self
    }

    /// copy the specified environment variables from other; Panic's if they do not exist.
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn copy_env(self, name: impl Into<String>, other: &Self) -> Self {
        let name = name.into();
        let value = match other.env.get(&name) {
            Some(v) => v.clone(),
            None => panic!(
                "Cannot copy {} from existing config. Available values are: {:#?}",
                name, other.env
            ),
        };

        self.with_env(name, value)
    }

    /// add a name=value http header to all client requests made to the server
    fn with_client_header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.client_headers.push((
            name.as_ref().parse().expect("valid header name"),
            value.as_ref().parse().expect("valid header value"),
        ));
        self
    }

    /// Configures a new WAL
    fn with_new_wal(mut self) -> Self {
        let tmpdir = TempDir::new().expect("cannot create tmp dir");

        let wal_string = tmpdir.path().display().to_string();
        self.wal_dir = Some(Arc::new(tmpdir));
        self.with_env("INFLUXDB_IOX_WAL_DIRECTORY", wal_string)
    }

    /// Configures a new object store
    fn with_new_object_store(mut self) -> Self {
        let tmpdir = TempDir::new().expect("cannot create tmp dir");

        let object_store_string = tmpdir.path().display().to_string();
        self.object_store_dir = Some(Arc::new(tmpdir));
        self.with_env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .with_env("INFLUXDB_IOX_DB_DIR", object_store_string)
    }

    pub fn with_file_object_store(mut self, tmpdir: Arc<TempDir>) -> Self {
        let object_store_string = tmpdir.path().display().to_string();
        self.object_store_dir = Some(tmpdir);
        self.with_env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .with_env("INFLUXDB_IOX_DB_DIR", object_store_string)
    }

    /// Configures this TestConfig to use the same object store as other
    pub fn with_existing_object_store(mut self, other: &Self) -> Self {
        // copy a reference to the temp dir, if any
        self.object_store_dir.clone_from(&other.object_store_dir);
        self.copy_env("INFLUXDB_IOX_OBJECT_STORE", other)
            .copy_env("INFLUXDB_IOX_DB_DIR", other)
    }

    /// Removes object store configuration
    pub fn with_memory_object_store(self) -> Self {
        self.with_env("INFLUXDB_IOX_OBJECT_STORE", "memory")
            .without_env("INFLUXDB_IOX_DB_DIR")
    }

    /// Configure maximum per-table query bytes for the querier.
    pub fn with_querier_mem_pool_bytes(self, bytes: usize) -> Self {
        self.with_env("INFLUXDB_IOX_EXEC_MEM_POOL_BYTES", bytes.to_string())
    }

    /// Configure sharding splits for the compactor.
    pub fn with_compactor_shards(self, n_shards: usize, shard_id: usize) -> Self {
        self.with_env("INFLUXDB_IOX_COMPACTION_SHARD_COUNT", n_shards.to_string())
            .with_env("INFLUXDB_IOX_COMPACTION_SHARD_ID", shard_id.to_string())
    }

    /// Limit the number of concurrent queries.
    pub fn with_max_concurrent_queries(self, n: usize) -> Self {
        self.with_env("INFLUXDB_IOX_MAX_CONCURRENT_QUERIES", n.to_string())
    }

    /// Set up a metadata signing key for bulk ingest.
    pub fn with_bulk_ingest_metadata_signing_key(self, metadata_signing_key_file: &str) -> Self {
        self.with_env(
            "INFLUXDB_IOX_BULK_INGEST_METADATA_SIGNING_KEY_FILE",
            metadata_signing_key_file,
        )
    }

    /// Use a mock presigned URL generator rather than whatever object store may have been
    /// configured. Allows for testing bulk ingest without needing S3.
    pub fn with_mock_presigned_url_signer(self, url: &str) -> Self {
        self.with_env(
            "INFLUXDB_IOX_BULK_INGEST_USE_MOCK_PRESIGNED_URL_SIGNER",
            url,
        )
    }

    /// Register catalog peers.
    pub fn with_catalog_peers<I, S>(self, peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: std::fmt::Display,
    {
        let peers = peers.into_iter().map(|s| s.to_string()).collect::<Vec<_>>();
        self.with_env("INFLUXDB_IOX_CATALOG_CACHE_PEERS", peers.join(","))
    }

    /// Set [`wait_for_ready`](Self::wait_for_ready).
    pub fn with_wait_for_ready(self, wait_for_ready: bool) -> Self {
        Self {
            wait_for_ready,
            ..self
        }
    }

    /// Get the test config's server type.
    #[must_use]
    pub fn server_type(&self) -> ServerType {
        self.server_type
    }

    /// Get a reference to the test config's env.
    pub fn env(&self) -> impl Iterator<Item = (&str, &str)> {
        self.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Get a reference to the test config's client headers.
    #[must_use]
    pub fn client_headers(&self) -> &[(HeaderName, HeaderValue)] {
        self.client_headers.as_ref()
    }

    /// Get a reference to the test config's addrs.
    #[must_use]
    pub fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }

    /// return the base ingester gRPC address, such as
    /// `http://localhost:8082/`
    pub fn ingester_base(&self) -> Arc<str> {
        self.addrs().ingester_grpc_api().client_base()
    }

    /// Return a HTTP base that is usable for health and metrics.
    ///
    /// This depends on the [server type](Self::server_type).
    #[must_use]
    pub fn http_base(&self) -> Arc<str> {
        let addr = match self.server_type {
            ServerType::AllInOne => self.addrs.router_http_api(),
            ServerType::Ingester => self.addrs.ingester_http_api(),
            ServerType::Router => self.addrs.router_http_api(),
            ServerType::Querier => self.addrs.querier_http_api(),
            ServerType::Compactor => self.addrs.compactor_http_api(),
            ServerType::Catalog => self.addrs.catalog_http_api(),
            ServerType::ParquetCache => self.addrs.parquet_cache_http_api(),
        };
        addr.client_base()
    }

    /// Wait for server to be ready during creation.
    pub fn wait_for_ready(&self) -> bool {
        self.wait_for_ready
    }
}

fn random_catalog_schema_name() -> String {
    let mut rng = rand::thread_rng();

    (&mut rng)
        .sample_iter(rand::distributions::Alphanumeric)
        .filter(|c| c.is_ascii_alphabetic())
        .take(20)
        .map(char::from)
        .collect::<String>()
}

/// Rewrites the special "sqlite" catalog DSN to a new
/// temporary sqlite filename in a new temporary directory such as
///
/// sqlite:///tmp/XygUWHUwBhSdIUNXblXo.sqlite
///
///
/// This is needed to isolate different test runs from each other
/// (there is no "schema" within a sqlite database, it is the name of
/// the file).
///
/// returns (dsn, catalog_dir)
fn specialize_dsn_if_needed(
    dsn: Option<String>,
    catalog_schema_name: &str,
) -> (Option<String>, Option<Arc<TempDir>>) {
    if dsn.as_deref() == Some("sqlite") {
        let tmpdir = TempDir::new().expect("cannot create tmp dir for catalog");
        let catalog_dir = Arc::new(tmpdir);
        let dsn = format!(
            "sqlite://{}/{catalog_schema_name}.sqlite",
            catalog_dir.path().display()
        );
        info!(%dsn, "rewrote 'sqlite' to temporary file");
        (Some(dsn), Some(catalog_dir))
    } else {
        (dsn, None)
    }
}
