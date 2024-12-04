// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures_concurrency::future::Join;
use tokio::sync::Barrier;

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion, Throughput,
};
use data_types::{Namespace, Partition, PartitionKey, Table};
use iox_catalog::{
    interface::Catalog,
    test_helpers::{arbitrary_namespace, arbitrary_table, catalog, create_parquet_file},
};

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
        |b| b.to_async(runtime()).iter_custom(|iters| async move {
            let catalog = catalog().await.0;
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
                    &catalog,
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
                    &catalog,
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
        })
    );
}

async fn measure_parquet_file_creation_duration<C: Catalog>(
    catalog: &C,
    barrier: Arc<Barrier>,
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
    iters: u64,
) -> Duration {
    barrier.wait().await;
    let start = Instant::now();
    for _ in 0..iters {
        create_parquet_file(catalog, namespace, table, partition).await;
    }
    start.elapsed()
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

criterion_group!(benches, quorum_benchmarks);
criterion_main!(benches);
