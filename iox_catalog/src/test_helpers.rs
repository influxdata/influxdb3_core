//! Catalog helper functions for creation of catalog objects

use std::{any::Any, collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use catalog_cache::{
    api::{quorum::QuorumCatalogCache, server::test_util::TestCacheServer},
    local::CatalogCache,
};
use data_types::{
    partition_template::TablePartitionTemplateOverride, ColumnId, ColumnSet, CompactionLevel,
    MaxL0CreatedAt, Namespace, NamespaceName, ObjectStoreId, ParquetFileParams, Partition, Table,
    TableSchema, Timestamp,
};
use iox_time::{SystemProvider, TimeProvider};
use parking_lot::Mutex;

use crate::{
    cache::{CachingCatalog, CachingCatalogParams},
    interface::{Catalog, Error, RepoCollection},
    metrics::GetTimeMetric,
    postgres::{parse_dsn, PostgresCatalog, PostgresConnectionOptions},
};

/// When the details of the namespace don't matter; the test just needs *a* catalog namespace
/// with a particular name.
///
/// Use [`NamespaceRepo::create`] directly if:
///
/// - The values of the parameters to `create` need to be different than what's here
/// - The values of the parameters to `create` are relevant to the behavior under test
/// - You expect namespace creation to fail in the test
///
/// [`NamespaceRepo::create`]: crate::interface::NamespaceRepo::create
pub async fn arbitrary_namespace<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
) -> Namespace {
    let namespace_name = NamespaceName::new(name).unwrap();
    repos
        .namespaces()
        .create(&namespace_name, None, None, None)
        .await
        .unwrap()
}

/// When the details of the namespace don't matter; the test just needs *a* catalog namespace
/// with a particular name and a specific retention policy.
///
/// Use [`NamespaceRepo::create`] directly if:
///
/// - The values of the parameters to `create` need to be different than what's here
/// - The values of the parameters to `create` are relevant to the behavior under test
/// - You expect namespace creation to fail in the test
///
/// [`NamespaceRepo::create`]: crate::interface::NamespaceRepo::create
pub async fn arbitrary_namespace_with_retention_policy<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
    retention_period_ns: i64,
) -> Namespace {
    let namespace_name = NamespaceName::new(name).unwrap();
    repos
        .namespaces()
        .create(&namespace_name, None, Some(retention_period_ns), None)
        .await
        .unwrap()
}

/// When the details of the table don't matter; the test just needs *a* catalog table
/// with a particular name in a particular namespace.
///
/// Use [`TableRepo::create`] directly if:
///
/// - The values of the parameters to `create_or_get` need to be different than what's here
/// - The values of the parameters to `create_or_get` are relevant to the behavior under test
/// - You expect table creation to fail in the test
///
/// [`TableRepo::create`]: crate::interface::TableRepo::create
pub async fn arbitrary_table<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
    namespace: &Namespace,
) -> Table {
    repos
        .tables()
        .create(
            name,
            TablePartitionTemplateOverride::try_from_existing(None, &namespace.partition_template)
                .unwrap(),
            namespace.id,
        )
        .await
        .unwrap()
}

/// Load or create an arbitrary table schema in the same way that a write implicitly creates a
/// table, that is, with a time column.
pub async fn arbitrary_table_schema_load_or_create<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
    namespace: &Namespace,
) -> TableSchema {
    crate::util::table_load_or_create(repos, namespace.id, &namespace.partition_template, name)
        .await
        .unwrap()
}

/// When the details of a Parquet file record don't matter, the test just needs *a* Parquet
/// file record in a particular namespace+table+partition.
pub fn arbitrary_parquet_file_params(
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
) -> ParquetFileParams {
    ParquetFileParams {
        namespace_id: namespace.id,
        table_id: table.id,
        partition_id: partition.id,
        partition_hash_id: partition.hash_id().cloned(),
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(1),
        max_time: Timestamp::new(10),
        file_size_bytes: 1337,
        row_count: 0,
        compaction_level: CompactionLevel::Initial,
        column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        max_l0_created_at: MaxL0CreatedAt::NotCompacted,
        source: None,
    }
}

/// Create a parquet file in the given catalog, namespace, table, and partition, panicking on
/// failure
pub async fn create_parquet_file<C: Catalog>(
    catalog: &C,
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
) {
    let params = arbitrary_parquet_file_params(namespace, table, partition);
    let create = vec![params];
    _ = catalog
        .repositories()
        .parquet_files()
        .create_upgrade_delete(partition.id, &[], &[], &create, CompactionLevel::Initial)
        .await
        .unwrap()
}

/// A convenience type to avoid making some return types too complicated
pub type CatalogAndCache = (TestCatalog<CachingCatalog>, Arc<QuorumCatalogCache>);

/// calls [`catalog_from_backing`] with a temporary [`PostgresCatalog`]
pub async fn catalog() -> CatalogAndCache {
    let (backing, db) = run_backing_postgres_catalog(Arc::default()).await;
    let (cat, cache) = catalog_from_backing(backing);
    cat.hold_onto(db);
    (cat, cache)
}

/// Call [`catalog_from_backing_and_times`] with the provided `backing`, [`SystemProvider`], and
/// [`Duration::ZERO`]
pub fn catalog_from_backing<C: Catalog + 'static>(backing: C) -> CatalogAndCache {
    catalog_from_backing_and_times(
        backing,
        Arc::new(SystemProvider::new()) as _,
        Duration::ZERO,
        None,
    )
}

/// Build a basic Catalog and Cache for use with tests, using the provided `backing` [`Catalog`]
/// as the backing data store, with the provided times
pub fn catalog_from_backing_and_times<C: Catalog + 'static>(
    backing: C,
    time_provider: Arc<dyn TimeProvider>,
    batch_delay: Duration,
    etag_min_payload_size: Option<usize>,
) -> CatalogAndCache {
    let metrics = backing.metrics();
    let peer0 = TestCacheServer::bind_ephemeral(&metrics);
    let peer1 = TestCacheServer::bind_ephemeral(&metrics);

    let cache = Arc::new(QuorumCatalogCache::new(
        Arc::new(CatalogCache::default()),
        Arc::new([peer0.client(), peer1.client()]),
    ));

    let params = CachingCatalogParams {
        cache: Arc::clone(&cache),
        backing: Arc::new(backing),
        metrics,
        time_provider,
        quorum_fanout: 10,
        partition_linger: batch_delay,
        table_linger: batch_delay,
        admin_ui_storage_api_enabled: false,
    };

    let caching_catalog = match etag_min_payload_size {
        Some(size) => CachingCatalog::new_inner(params, size),
        None => CachingCatalog::new(params),
    };

    let test_catalog = TestCatalog::new(caching_catalog);
    test_catalog.hold_onto(peer0);
    test_catalog.hold_onto(peer1);

    (test_catalog, cache)
}

async fn run_backing_postgres_catalog(
    metrics: Arc<metric::Registry>,
) -> (PostgresCatalog, pgtemp::PgTempDB) {
    let db = pgtemp::PgTempDBBuilder::new()
        .with_config_param("fsync", "on") // pgtemp sets fsync=off, but the last arg wins
        .with_config_param("synchronous_commit", "on") // pgtemp sets synchronous_commit=off
        .with_config_param("full_page_writes", "on") // pgtemp sets full_page_writes=off
        .with_config_param("autovacuum", "on") // pgtemp sets autovacuum=off
        .start();
    let dsn = parse_dsn(&db.connection_uri()).unwrap();

    let pg_conn_options = PostgresConnectionOptions {
        dsn,
        ..Default::default()
    };

    let postgres_catalog = PostgresCatalog::connect(pg_conn_options, metrics)
        .await
        .expect("failed to connect to catalog");

    postgres_catalog
        .setup()
        .await
        .expect("failed to setup catalog");

    (postgres_catalog, db)
}

/// [`Catalog`] wrapper that is helpful for testing.
#[derive(Debug)]
pub struct TestCatalog<T> {
    hold_onto: Mutex<Vec<Box<dyn Any + Send>>>,
    get_time_metric: GetTimeMetric,
    pub(crate) inner: T,
}

impl<T: Catalog> TestCatalog<T> {
    const NAME: &'static str = "test";

    /// Create new test catalog.
    pub(crate) fn new(inner: T) -> Self {
        Self {
            hold_onto: Mutex::new(vec![]),
            get_time_metric: GetTimeMetric::new(&inner.metrics(), Self::NAME),
            inner,
        }
    }

    /// Hold onto given value til dropped.
    pub(crate) fn hold_onto<H>(&self, o: H)
    where
        H: Send + 'static,
    {
        self.hold_onto.lock().push(Box::new(o) as _)
    }
}

#[async_trait]
impl<T: Catalog> Catalog for TestCatalog<T> {
    async fn setup(&self) -> Result<(), Error> {
        self.inner.setup().await
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        self.inner.repositories()
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        self.inner.metrics()
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        self.inner.time_provider()
    }

    async fn get_time(&self) -> Result<iox_time::Time, Error> {
        let start = tokio::time::Instant::now();
        let res = Ok(self.time_provider().now());
        self.get_time_metric.record(start.elapsed(), &res);
        res
    }

    async fn active_applications(&self) -> Result<HashSet<String>, Error> {
        self.inner.active_applications().await
    }

    fn name(&self) -> &'static str {
        Self::NAME
    }
}
