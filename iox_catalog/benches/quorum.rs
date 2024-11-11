// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![allow(unused_crate_dependencies)]

use std::{
    any::Any,
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_concurrency::future::Join;
use tokio::sync::Barrier;

use async_trait::async_trait;
use catalog_cache::{
    api::{quorum::QuorumCatalogCache, server::test_util::TestCacheServer},
    local::CatalogCache,
};
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion, Throughput,
};
use data_types::{CompactionLevel, Namespace, Partition, PartitionKey, Table};
use iox_catalog::{
    cache::{CachingCatalog, CachingCatalogParams},
    interface::{Catalog, Error, RepoCollection, Result},
    postgres::{parse_dsn, PostgresCatalog, PostgresConnectionOptions},
    test_helpers::{arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table},
};
use iox_time::{SystemProvider, TimeProvider};
use parking_lot::Mutex;

struct BenchmarkParams {
    concurrent_writers: usize,
    preloaded_files: usize,
}

fn quorum_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum_caching_catalog");
    for preloaded_files in [0, 100, 500, 1000] {
        for concurrent_writers in [1, 2, 4, 8, 16, 32, 64, 128] {
            run_bench(
                &mut group,
                BenchmarkParams {
                    concurrent_writers,
                    preloaded_files,
                },
            );
        }
    }
    group.finish();
}

fn run_bench(group: &mut BenchmarkGroup<'_, WallTime>, params: BenchmarkParams) {
    let BenchmarkParams {
        concurrent_writers,
        preloaded_files,
    } = params;

    group.throughput(Throughput::Elements(1));
    group.bench_function(
        format!("create_upgrade_delete/{preloaded_files}_preloaded_files/{concurrent_writers}_concurrent_writers"),
        |b| {
            b.to_async(runtime()).iter_custom(|iters| async move {
                let catalog = build_catalog().await.0;
                let mut repos = catalog.repositories();

                let ns = arbitrary_namespace(&mut *repos, "ns").await;
                let table = arbitrary_table(&mut *repos, "t", &ns).await;
                let partition = repos
                    .partitions()
                    .create_or_get(PartitionKey::from("p"), table.id)
                    .await
                    .unwrap();

                // preload files
                let preload_files = (0..preloaded_files)
                    .map(|_| create_parquet_file(
                        Arc::clone(&catalog),
                        &ns,
                        &table,
                        &partition,
                    ))
                    .collect::<Vec<_>>();

                preload_files.join().await;


                let barrier = Arc::new(Barrier::new(concurrent_writers));
                let mut handles = vec![];
                // create N futures that wait for the last thread to spawn
                for _ in 0..concurrent_writers {
                    handles.push(measure_parquet_file_creation_duration(
                        Arc::clone(&catalog),
                        Arc::clone(&barrier),
                        &ns,
                        &table,
                        &partition,
                        iters,
                    ));
                }

                let times = handles.join().await;

                // return the Duration from the last future
                times.last().unwrap().to_owned()
            });
        },
    );
}

async fn measure_parquet_file_creation_duration(
    catalog: Arc<impl Catalog>,
    barrier: Arc<Barrier>,
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
    iters: u64,
) -> Duration {
    barrier.wait().await;
    let start = Instant::now();
    for _ in 0..iters {
        create_parquet_file(Arc::clone(&catalog), namespace, table, partition).await;
    }
    start.elapsed()
}

async fn create_parquet_file(
    catalog: Arc<impl Catalog>,
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
) {
    let params = arbitrary_parquet_file_params(namespace, table, partition);
    let create = vec![params];
    let _ = catalog
        .repositories()
        .parquet_files()
        .create_upgrade_delete(partition.id, &[], &[], &create, CompactionLevel::Initial)
        .await;
}

async fn build_catalog() -> (Arc<TestCatalog<CachingCatalog>>, Arc<QuorumCatalogCache>) {
    build_catalog_with_params(Arc::new(SystemProvider::new()), Duration::ZERO).await
}

async fn build_catalog_with_params(
    time_provider: Arc<dyn TimeProvider>,
    batch_delay: Duration,
) -> (Arc<TestCatalog<CachingCatalog>>, Arc<QuorumCatalogCache>) {
    let metrics = Arc::new(metric::Registry::default());
    let (backing, db) = run_backing_postgres_catalog(Arc::clone(&metrics)).await;

    let peer0 = TestCacheServer::bind_ephemeral(&metrics);
    let peer1 = TestCacheServer::bind_ephemeral(&metrics);

    let cache = Arc::new(QuorumCatalogCache::new(
        Arc::new(CatalogCache::default()),
        Arc::new([peer0.client(), peer1.client()]),
    ));

    let caching_catalog = CachingCatalog::new(CachingCatalogParams {
        cache: Arc::clone(&cache),
        backing: Arc::new(backing),
        metrics,
        time_provider,
        quorum_fanout: 10,
        partition_linger: batch_delay,
        table_linger: batch_delay,
        admin_ui_storage_api_enabled: false,
    });

    let test_catalog = Arc::new(TestCatalog::new(caching_catalog));
    test_catalog.hold_onto(peer0);
    test_catalog.hold_onto(peer1);
    test_catalog.hold_onto(db);

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

fn runtime() -> tokio::runtime::Runtime {
    // TODO: experiment with different numbers of worker threads ?
    // Since we use scoped threads in our benchmark, this does not dictate the number of concurrent
    // writers, but it does dictate the parallelism of the catalog & backing quorum cache
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

// duplicating this type because cfg(test) is not enabled for benchmarks
#[derive(Debug)]
struct TestCatalog<T> {
    hold_onto: Mutex<Vec<Box<dyn Any + Send>>>,
    inner: T,
}

impl<T: Catalog> TestCatalog<T> {
    /// Create new test catalog.
    fn new(inner: T) -> Self {
        Self {
            hold_onto: Mutex::new(vec![]),
            inner,
        }
    }

    /// Hold onto given value til dropped.
    fn hold_onto<H>(&self, o: H)
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

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        self.inner.time_provider()
    }

    async fn get_time(&self) -> Result<iox_time::Time, Error> {
        Ok(self.inner.time_provider().now())
    }

    async fn active_applications(&self) -> Result<HashSet<String>, Error> {
        self.inner.active_applications().await
    }

    fn name(&self) -> &'static str {
        "test"
    }
}

criterion_group!(benches, quorum_benchmarks);
criterion_main!(benches);
