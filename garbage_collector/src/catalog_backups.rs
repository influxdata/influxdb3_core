use catalog_backup_file_list::{
    next_hourly_file_list_as_of, DesiredFileListTime, PathsInCatalogBackups,
};
use data_types::{NamespaceId, NamespaceName, TableId};
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use iox_time::{Time, TimeProvider};
use metric::{DurationHistogram, DurationHistogramOptions, DURATION_MAX};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap,
    io::{self},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    process::Command,
    select,
    time::{interval_at, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

/// Structure that holds metrics for the garbage collector catalog backups
#[derive(Debug)]
struct CatalogBackupMetrics {
    // Track how long successfully creating catalog backup data snapshots takes
    catalog_backup_success_duration: DurationHistogram,

    // Track how long failed catalog backup data snapshot creation takes
    catalog_backup_error_duration: DurationHistogram,
}

impl CatalogBackupMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let runtime_duration_seconds = metric_registry
            .register_metric_with_options::<DurationHistogram, _>(
                "gc_catalog_backup_runtime_duration",
                "GC catalog backup loop run-times, bucketed by operation success or failure.",
                || {
                    DurationHistogramOptions::new(vec![
                        Duration::from_secs(1),
                        Duration::from_secs(10),
                        Duration::from_secs(60),
                        Duration::from_secs(300),
                        Duration::from_secs(600),
                        Duration::from_secs(6000),
                        DURATION_MAX,
                    ])
                },
            );
        let catalog_backup_success_duration =
            runtime_duration_seconds.recorder(&[("result", "success")]);
        let catalog_backup_error_duration =
            runtime_duration_seconds.recorder(&[("result", "error")]);

        Self {
            catalog_backup_success_duration,
            catalog_backup_error_duration,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn perform(
    enabled: bool,
    metric_registry: Arc<metric::Registry>,
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    dump_dsn: Option<String>,
    start_time: Time,
    interval_duration: Duration,
    paths_in_catalog_backups: Arc<PathsInCatalogBackups>,
) {
    if !enabled {
        info!(%start_time, "Catalog backup data snapshot creation is not enabled");
        return;
    }

    let metrics = CatalogBackupMetrics::new(metric_registry);

    let time_provider = catalog.time_provider();

    // Do our best to start when the catalog thinks `start_time` is. If any of these calculations
    // fail or produce times in the past, start now.
    let catalog_now = catalog.time_provider().now();
    let duration_until_start = start_time
        .checked_duration_since(catalog_now)
        .unwrap_or(Duration::from_secs(0));
    let now = Instant::now();
    let start_instant = now.checked_add(duration_until_start).unwrap_or(now);

    let mut interval = interval_at(start_instant.into(), interval_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await; // first tick completes at `start_instant

    info!(%start_time, ?duration_until_start, ?start_instant, "catalog backup data snapshot creation is enabled");

    loop {
        let start = Instant::now();

        info!("catalog backup beginning");

        let is_behind = match try_creating_catalog_backup(
            &catalog,
            &time_provider,
            &object_store,
            &dump_dsn,
            &paths_in_catalog_backups,
        )
        .await
        {
            Ok((Some(num_files), as_of, is_behind)) => {
                let elapsed = start.elapsed();
                metrics.catalog_backup_success_duration.record(elapsed);
                let as_of = as_of.unwrap();

                info!(
                    num_files,
                    ?elapsed,
                    ?as_of,
                    ?is_behind,
                    "catalog backup completed"
                );
                is_behind
            }
            Ok((None, _, is_behind)) => {
                let elapsed = start.elapsed();
                info!(?elapsed, "no catalog backup needed at this time");
                is_behind
            }
            Err(e) => {
                let elapsed = start.elapsed();
                metrics.catalog_backup_error_duration.record(elapsed);

                warn!(?elapsed, "catalog backup error, continuing: {e}");
                false
            }
        };

        if !is_behind {
            select! {
                _ = shutdown.cancelled() => {
                    break
                },
                _ = interval.tick() => (),
            }
        }
    }
}

#[derive(Debug, Snafu)]
enum CatalogBackupCreationError {
    #[snafu(display("Error listing active Parquet files from catalog: {source}"))]
    ListingActiveParquetFiles {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error getting namespace ID to name map from catalog: {source}"))]
    NamespaceNameMap {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error getting table ID to name map from catalog: {source}"))]
    TableNameMap {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error saving catalog backup list to object storage: {source}"))]
    SavingToObjectStorage {
        source: catalog_backup_file_list::SaveError,
    },

    #[snafu(display("Error determining need for a snapshot: {source}"))]
    DeterminingSnapshotNeed {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error performing IO operation: {source}"))]
    Io { source: io::Error },

    #[snafu(display("Error running pg_dump: {status}"))]
    PgDump { status: ExitStatus },
}

async fn try_creating_catalog_backup(
    catalog: &Arc<dyn Catalog>,
    time_provider: &Arc<dyn TimeProvider>,
    object_store: &Arc<DynObjectStore>,
    dump_dsn: &Option<String>,
    paths_in_catalog_backups: &Arc<PathsInCatalogBackups>,
) -> Result<(Option<usize>, Option<DesiredFileListTime>, bool), CatalogBackupCreationError> {
    match next_hourly_file_list_as_of(time_provider, object_store).await {
        Ok((Some(as_of), is_behind)) => {
            info!(?as_of, "creating catalog backup");

            if let Some(dsn) = dump_dsn {
                let _dump = run_pg_dump_with_dsn(dsn).await;
            }

            let parquet_files = catalog
                .repositories()
                .parquet_files()
                .active_as_of(as_of.time().into())
                .await
                .context(ListingActiveParquetFilesSnafu)?;

            let num_files = parquet_files.len();
            info!(%num_files, ?as_of, "catalog backup identified valid files");

            let catalog_backup_file_list =
                catalog_backup_file_list::FileList::new(as_of, parquet_files);

            let namespace_names_by_id = get_namespace_name_map(catalog)
                .await
                .context(NamespaceNameMapSnafu)?;

            let table_names_by_id = get_table_name_map(catalog)
                .await
                .context(TableNameMapSnafu)?;

            info!(?as_of, "catalog backup saving file list");
            catalog_backup_file_list
                .save(object_store, &namespace_names_by_id, &table_names_by_id)
                .await
                .context(SavingToObjectStorageSnafu)?;

            // Union this bloom filter with the previous sets.
            //
            // The Parquet files included in this snapshot won't be sent for deletion by the
            // `objectstore::checker` racing with this thread prior to this bloom filter being
            // made visible to the checker, because even if an active Parquet file included in
            // this snapshot is immediately marked in the catalog as non-active (`to_delete`
            // set), the `objectstore::checker` is guaranteed to not take any action on any
            // Parquet file until it is at least 3h old (enforced by the `CutoffDuration` type
            // used for `objectstore_cutoff`).
            //
            // Invariant: the bloom filter for this snapshot must be generated and union-ed
            // into this shared state within 3h (the value of
            // `clap_blocks::garbage_collector::MINIMUM_CUTOFF_PERIOD` used in the construction
            // of `CutoffDuration`s) to avoid racing with the `objectstore::checker`.
            info!(?as_of, "catalog backup merging bloom filter");
            paths_in_catalog_backups.union(catalog_backup_file_list.bloom_filter());

            Ok((Some(num_files), Some(as_of), is_behind))
        }
        Ok((None, is_behind)) => Ok((None, None, is_behind)),
        Err(source) => Err(CatalogBackupCreationError::DeterminingSnapshotNeed { source }),
    }
}

async fn get_namespace_name_map(
    catalog: &Arc<dyn Catalog>,
) -> Result<HashMap<NamespaceId, NamespaceName<'_>>, iox_catalog::interface::Error> {
    let namespaces = catalog
        .repositories()
        .namespaces()
        .list(SoftDeletedRows::AllRows)
        .await?;

    Ok(namespaces
        .into_iter()
        .map(|namespace| {
            (
                namespace.id,
                namespace
                    .name
                    .try_into()
                    .expect("namespace names from the catalog should be valid"),
            )
        })
        .collect())
}

async fn get_table_name_map(
    catalog: &Arc<dyn Catalog>,
) -> Result<HashMap<TableId, String>, iox_catalog::interface::Error> {
    let tables = catalog.repositories().tables().list().await?;

    Ok(tables
        .into_iter()
        .map(|table| (table.id, table.name))
        .collect())
}

async fn run_pg_dump_with_dsn(dsn: &str) -> Result<Vec<u8>, CatalogBackupCreationError> {
    let start = Instant::now();
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!("/usr/bin/pg_dump -d {} > /dev/null", dsn))
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| CatalogBackupCreationError::Io { source: e })?
        .wait_with_output()
        .await
        .map_err(|e| CatalogBackupCreationError::Io { source: e })?;

    info!(
        "pg_dump commpleted in {:?}, status: {}, size: {}",
        start.elapsed(),
        output.status,
        output.stdout.len()
    );

    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(CatalogBackupCreationError::PgDump {
            status: output.status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_backup_file_list::FindFileList;
    use futures::TryStreamExt;
    use iox_catalog::{interface::ParquetFileRepoExt, mem::MemCatalog, test_helpers::*};
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::memory::InMemory;
    use std::io::Read;

    #[tokio::test]
    async fn do_nothing_if_not_enabled() {
        let enabled = false;

        // Say the garbage collector happens to start up 2 seconds before the hour, and there have
        // never been any catalog backups taken.
        let time_provider = Arc::new(MockProvider::new(
            Time::from_rfc3339("2024-08-12T13:59:58+00:00").unwrap(),
        ));

        let shutdown = CancellationToken::new();
        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::default());
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider) as _,
        ));
        let paths_in_catalog_backups = Arc::new(
            PathsInCatalogBackups::try_new(&object_store, enabled)
                .await
                .unwrap(),
        );

        let task_shutdown = shutdown.clone();
        let task_catalog = Arc::clone(&catalog);
        let task_object_store = Arc::clone(&object_store);
        let task_paths_in_catalog_backups = Arc::clone(&paths_in_catalog_backups);

        // The task should start immediately if given a start time in the past
        let start_time = time_provider.now();
        // So that this test doesn't take an hour to run, have the catalog backup task check to see
        // if it should create a backup once per second. Usually, this would be once an hour.
        let interval_duration = Duration::from_secs(1);
        let join_handle = tokio::spawn(async move {
            perform(
                enabled,
                metric_registry,
                task_shutdown,
                task_catalog,
                task_object_store,
                None,
                start_time,
                interval_duration,
                task_paths_in_catalog_backups,
            )
            .await;
        });

        // Sleep for 2 seconds to give the task a chance to run, if the `enabled` flag check isn't
        // functioning or has been removed.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // The task should have exited without needing to be told to shut down
        join_handle.await.unwrap();

        // There should not be anything in object storage
        let all_object_store_paths: Vec<_> = object_store
            .list(None)
            .map_ok(|object_meta| object_meta.location)
            .try_collect()
            .await
            .unwrap();
        assert!(
            all_object_store_paths.is_empty(),
            "Expected empty, got {all_object_store_paths:?}"
        );
    }

    #[tokio::test]
    async fn creates_first_two_snapshots() {
        let enabled = true;

        // Say the garbage collector happens to start up 2 seconds before the hour, and there have
        // never been any catalog backups taken.
        let time_provider = Arc::new(MockProvider::new(
            Time::from_rfc3339("2024-08-12T13:59:58+00:00").unwrap(),
        ));

        let shutdown = CancellationToken::new();
        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::default());
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider) as _,
        ));
        let paths_in_catalog_backups = Arc::new(
            PathsInCatalogBackups::try_new(&object_store, enabled)
                .await
                .unwrap(),
        );

        let task_shutdown = shutdown.clone();
        let task_catalog = Arc::clone(&catalog);
        let task_object_store = Arc::clone(&object_store);
        let task_paths_in_catalog_backups = Arc::clone(&paths_in_catalog_backups);

        // The task should start immediately if given a start time in the past
        let start_time = time_provider.now();
        // So that this test doesn't take an hour to run, have the catalog backup task check to see
        // if it should create a backup once per second. Usually, this would be once an hour.
        let interval_duration = Duration::from_secs(1);
        let join_handle = tokio::spawn(async move {
            perform(
                enabled,
                metric_registry,
                task_shutdown,
                task_catalog,
                task_object_store,
                None,
                start_time,
                interval_duration,
                task_paths_in_catalog_backups,
            )
            .await;
        });

        // Sleep for 2 seconds to give the task a chance to run with the time set to 2 seconds
        // before the hour, which should take a snapshot for the previously passed hour because
        // there were no existing snapshots. This snapshot will be empty.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add some Parquet files to the catalog
        let mut repos = catalog.repositories();
        let namespace = arbitrary_namespace(&mut *repos, "test_namespace").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
        let partition = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();
        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Then change the mocked time to be 3 seconds after the hour,
        time_provider.inc(Duration::from_secs(5));
        // give the task a chance to run a few more times, only one of which should take a second
        // snapshot for the hour just passed.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Stop the task.
        shutdown.cancel();
        join_handle.await.unwrap();

        // The most recent snapshot should be for 14:00
        let most_recent = object_store.most_recent().await.unwrap().unwrap();
        assert_eq!(most_recent.to_string(), "2024-08-12T14:00:00+00:00");

        // There should be two total snapshots available
        let all_times = object_store.all_times().await.unwrap();
        let all_times_strings: Vec<_> =
            all_times.into_iter().map(|time| time.to_string()).collect();
        assert_eq!(
            all_times_strings,
            vec!["2024-08-12T13:00:00+00:00", "2024-08-12T14:00:00+00:00",]
        );

        let mut all_object_store_paths: Vec<_> = object_store
            .list(None)
            .map_ok(|object_meta| object_meta.location)
            .try_collect()
            .await
            .unwrap();
        all_object_store_paths.sort();
        assert_eq!(all_object_store_paths.len(), 3);

        // The first snapshot had no Parquet files in it, so it only has a bloom filter file
        assert!(all_object_store_paths[0]
            .as_ref()
            .starts_with("catalog_backup_file_lists/2024-08-12T13:00:00+00:00"));
        assert!(all_object_store_paths[0]
            .as_ref()
            .ends_with("/bloom.bin.gz"));

        // The second snapshot has a bloom filter file and one table text file
        assert!(all_object_store_paths[1]
            .as_ref()
            .starts_with("catalog_backup_file_lists/2024-08-12T14:00:00+00:00"));
        assert!(all_object_store_paths[1]
            .as_ref()
            .ends_with("/bloom.bin.gz"));
        assert!(all_object_store_paths[2]
            .as_ref()
            .starts_with("catalog_backup_file_lists/2024-08-12T14:00:00+00:00"));
        assert!(all_object_store_paths[2]
            .as_ref()
            .ends_with("/test_namespace-ID1/test_table-ID1.txt.gz"));

        // The table text file should have the one Parquet file's path in it
        let text_file_bytes = get_decompressed(&object_store, &all_object_store_paths[2]).await;
        let text_file_content = String::from_utf8(text_file_bytes).unwrap();
        let expected_text_file_content = format!(
            "{}/{}/{}/{}.parquet",
            namespace.id,
            table.id,
            partition.transition_partition_id(),
            parquet_file.object_store_id
        );
        assert_eq!(text_file_content, expected_text_file_content);

        // The bloom filter should recognize the Parquet file path is present in a backup
        assert!(
            paths_in_catalog_backups.contains(&object_store::path::Path::from(
                expected_text_file_content.as_str()
            ))
        );
    }

    async fn get_decompressed(
        object_store: &Arc<DynObjectStore>,
        path: &object_store::path::Path,
    ) -> Vec<u8> {
        let compressed_bytes = object_store
            .get(path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();

        let mut decoder = flate2::read::GzDecoder::new(&*compressed_bytes);
        let mut file_content = Vec::new();
        decoder
            .read_to_end(&mut file_content)
            .expect("should have been able to decompress file contents");
        file_content
    }

    mod name_map_lookups {
        use super::*;
        use iox_time::SystemProvider;

        #[tokio::test]
        async fn namespace_id_name_maps() {
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
                Default::default(),
                Arc::new(SystemProvider::new()),
            ));
            let mut repos = catalog.repositories();

            // Add a namespace to the catalog and mark it as deleted, to ensure we can get names
            // for soft-deleted namespaces
            arbitrary_namespace(&mut *repos, "test_namespace").await;
            let soft_deleted_id = repos
                .namespaces()
                .soft_delete("test_namespace")
                .await
                .unwrap();
            // Add an active namespace
            let another = arbitrary_namespace(&mut *repos, "not_deleted_namespace").await;

            let namespace_name_map = get_namespace_name_map(&catalog).await.unwrap();
            assert_eq!(namespace_name_map.len(), 2);

            assert_eq!(
                namespace_name_map.get(&soft_deleted_id).unwrap().as_str(),
                "test_namespace"
            );
            assert_eq!(
                namespace_name_map.get(&another.id).unwrap().as_str(),
                "not_deleted_namespace"
            );
        }

        #[tokio::test]
        async fn table_id_name_maps() {
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
                Default::default(),
                Arc::new(SystemProvider::new()),
            ));
            let mut repos = catalog.repositories();

            let namespace1 = arbitrary_namespace(&mut *repos, "namespace1").await;
            let namespace2 = arbitrary_namespace(&mut *repos, "namespace2").await;

            // Create 2 tables in namespace 1 with diferent names
            let table1_1 = arbitrary_table(&mut *repos, "test_table1", &namespace1).await;
            let table1_2 = arbitrary_table(&mut *repos, "test_table2", &namespace1).await;

            // Create 1 table in namespace 2 with the same name as one of namespace 1's tables
            let table2_1 = arbitrary_table(&mut *repos, "test_table1", &namespace2).await;

            let table_name_map = get_table_name_map(&catalog).await.unwrap();
            assert_eq!(table_name_map.len(), 3);

            assert_eq!(
                table_name_map.get(&table1_1.id).unwrap().as_str(),
                "test_table1"
            );
            assert_eq!(
                table_name_map.get(&table1_2.id).unwrap().as_str(),
                "test_table2"
            );
            assert_eq!(
                table_name_map.get(&table2_1.id).unwrap().as_str(),
                "test_table1"
            );
        }
    }
}
