//! Tool to clean up old object store files that don't appear in the catalog.

#![warn(missing_docs)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use chrono as _;
use clap as _;
#[cfg(test)]
use test_helpers as _;
use workspace_hack as _;

use crate::{
    objectstore::{checker as os_checker, deleter as os_deleter, lister as os_lister},
    parquetfile::deleter as pf_deleter,
    parquetfile::deleter::OlderThanCalculator,
};

use std::{fmt::Debug, sync::Arc, time::Duration};

use catalog_backup_file_list::PathsInCatalogBackups;
use clap_blocks::garbage_collector::GarbageCollectorConfig;
use humantime::format_duration;
use iox_catalog::interface::{Catalog, CatalogTimeProvider};
use iox_time::{SystemProvider, Time};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::prelude::*;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

/// Logic for managing catalog backup file lists
mod catalog_backups;
/// Logic for listing, checking and deleting files in object storage
mod objectstore;
/// Logic for deleting parquet files from the catalog
mod parquetfile;
/// Logic for flagging parquet files for deletion based on retention settings
mod retention;
// Helpers for unit tests
#[cfg(test)]
mod test_utils;

const BUFFER_SIZE: usize = 1000;
const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

/// Run the tasks that clean up old object store files that don't appear in the catalog.
pub async fn main(config: Config, metric_registry: Arc<metric::Registry>) -> Result<()> {
    GarbageCollector::start(config, metric_registry)
        .await?
        .join()
        .await
}

/// The tasks that clean up old object store files that don't appear in the catalog.
pub struct GarbageCollector {
    shutdown: CancellationToken,
    catalog_backup: tokio::task::JoinHandle<()>,
    paths_in_catalog_backups: Arc<PathsInCatalogBackups>,

    os_lister: tokio::task::JoinHandle<Result<(), os_lister::Error>>,
    os_checker: tokio::task::JoinHandle<Result<(), os_checker::Error>>,
    os_deleter: tokio::task::JoinHandle<()>,
    pf_deleter: tokio::task::JoinHandle<()>,
    partition_retention: tokio::task::JoinHandle<()>,
    parquet_retention: tokio::task::JoinHandle<()>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish_non_exhaustive()
    }
}

impl GarbageCollector {
    /// Construct the garbage collector and start it
    pub async fn start(config: Config, metric_registry: Arc<metric::Registry>) -> Result<Self> {
        let Config {
            object_store,
            sub_config,
            catalog,
            replica_catalog,
            dump_dsn,
        } = config;

        // Do not start the garbage collector if none of it's subtasks are enabled.
        let gc_disabled = !(sub_config.enable_object_store_deletion
            || sub_config.enable_parquet_file_deletion
            || sub_config.enable_partition_row_deletion);

        if gc_disabled {
            return Err(Error::GarbageCollectorDisabled);
        }

        info!(
            objectstore_cutoff = %format_duration(sub_config.objectstore_cutoff.duration()),
            bulk_ingest_objectstore_cutoff =
                %format_duration(sub_config.bulk_ingest_objectstore_cutoff.duration()),
            parquetfile_cutoff = %format_duration(sub_config.parquetfile_cutoff.duration()),
            parquetfile_sleep_interval = %format_duration(sub_config.parquetfile_sleep_interval()),
            objectstore_sleep_interval_minutes = %sub_config.objectstore_sleep_interval_minutes,
            retention_sleep_interval_minutes = %sub_config.retention_sleep_interval_minutes,
            enable_partition_row_deletion = %sub_config.enable_partition_row_deletion,
            "GarbageCollector starting"
        );

        // Shutdown handler channel to notify children
        let shutdown = CancellationToken::new();

        // Initialize the bloom filter from previously stored catalog backups to be used for
        // deciding whether Parquet files in object storage should be kept because they're
        // contained in some catalog backup. If the second parameter is false, this instance's
        // `contains` method will always return `true`, thus keeping all Parquet files without
        // considering the catalog backups.
        let paths_in_catalog_backups = Arc::new(
            PathsInCatalogBackups::try_new(
                &object_store,
                sub_config.delete_using_catalog_backup_data_snapshot_files,
            )
            .await
            .context(BloomFilterRestoreSnafu)?,
        );

        // Initialize the catalog backup thread, which saves lists of Parquet files active at
        // particular times to serve as snapshots of data state.
        let catalog_to_query_for_backup = replica_catalog.unwrap_or_else(|| Arc::clone(&catalog));
        // Try to start at aboun 5 minutes after the hour no matter when the garbage collector
        // service happens to start up, so that the snapshot will be taken soon after the hour but
        // the queries run as of the hour will return complete and consistent results.
        let start_time =
            next_five_minutes_after_the_hour(catalog_to_query_for_backup.time_provider().now());
        let catalog_backup = tokio::spawn(catalog_backups::perform(
            // If this is false, the function will do nothing
            sub_config.create_catalog_backup_data_snapshot_files,
            Arc::clone(&metric_registry),
            shutdown.clone(),
            catalog_to_query_for_backup,
            Arc::clone(&object_store),
            dump_dsn,
            start_time,
            ONE_HOUR,
            Arc::clone(&paths_in_catalog_backups),
        ));

        // Initialise the object store garbage collector, which works as three communicating threads:
        // - lister lists objects in the object store and sends them on a channel. the lister will
        //   run until it has enumerated all matching files, then sleep for the configured
        //   interval.
        // - checker receives from that channel and checks the catalog to see if they exist, if not
        //   it sends them on another channel
        // - deleter receives object store entries that have been checked and therefore should be
        //   deleted.
        let (tx1, rx1) = mpsc::channel(BUFFER_SIZE);
        let (tx2, rx2) = mpsc::channel(BUFFER_SIZE);

        let sdt = shutdown.clone();
        let osa = Arc::clone(&object_store);

        let lister_metric_registry = Arc::clone(&metric_registry);
        let os_lister = if sub_config.enable_object_store_deletion {
            tokio::spawn(async move {
                select! {
                    ret = os_lister::perform(
                        lister_metric_registry,
                        osa,
                        tx1,
                        sub_config.objectstore_sleep_interval_minutes,
                        sub_config.objectstore_sleep_interval_batch_milliseconds,
                    ) => {
                        ret
                    },
                    _ = sdt.cancelled() => {
                        Ok(())
                    },
                }
            })
        } else {
            warn!("object store lister disabled");
            tokio::spawn(async { Ok(()) })
        };

        let cat = Arc::clone(&catalog);
        let sdt = shutdown.clone();

        // TODO: exclusively use `CatalogTimeProvider`; see https://github.com/influxdata/influxdb_iox/issues/11963
        let time_provider = Arc::new(CatalogTimeProvider::new(Arc::clone(&catalog)));

        let checker_metric_registry = Arc::clone(&metric_registry);
        let os_checker = if sub_config.enable_object_store_deletion {
            tokio::spawn({
                let time_provider = Arc::clone(&time_provider);
                let paths_in_catalog_backups = Arc::clone(&paths_in_catalog_backups);
                async move {
                    select! {
                        ret = os_checker::perform(
                            checker_metric_registry,
                            cat,
                            time_provider,
                            sub_config.objectstore_cutoff,
                            sub_config.bulk_ingest_objectstore_cutoff,
                            sub_config.keep_hourly_catalog_backup_file_lists,
                            sub_config.keep_daily_catalog_backup_file_lists,
                            paths_in_catalog_backups,
                            rx1,
                            tx2,
                        ) => {
                            ret
                        },
                        _ = sdt.cancelled() => {
                            Ok(())
                        },
                    }
                }
            })
        } else {
            warn!("object store checker disabled");
            tokio::spawn(async { Ok(()) })
        };

        let os_deleter = if sub_config.enable_object_store_deletion {
            tokio::spawn({
                os_deleter::perform(
                    Arc::clone(&metric_registry),
                    shutdown.clone(),
                    Arc::clone(&object_store),
                    rx2,
                    Arc::clone(&paths_in_catalog_backups),
                )
            })
        } else {
            warn!("object store deletion disabled");
            tokio::spawn(async {})
        };

        let cutoff_from_now = pf_deleter::PlainOldCutoffFromNow {
            // This can use the garbage collector's `SystemProvider` because in this case, this is
            // an approximate time ago that doesn't need to be an exact match with anything in the
            // catalog.
            time_provider: Arc::new(SystemProvider::new()),
            cutoff: sub_config.parquetfile_cutoff,
        };
        let older_than_calculator: Arc<dyn OlderThanCalculator> =
            if sub_config.create_catalog_backup_data_snapshot_files {
                Arc::new(pf_deleter::CheckLastCatalogFileList {
                    file_list_finder: object_store,
                    cutoff_from_now,
                })
            } else {
                Arc::new(cutoff_from_now)
            };
        let pf_deleter = if sub_config.enable_parquet_file_deletion {
            tokio::spawn({
                pf_deleter::perform(
                    Arc::clone(&metric_registry),
                    shutdown.clone(),
                    Arc::clone(&catalog),
                    older_than_calculator,
                    sub_config.parquetfile_sleep_interval(),
                )
            })
        } else {
            warn!("parquet deletion disabled");
            tokio::spawn(async {})
        };

        // Initialise the retention code, which is just one thread that calls
        // flag_for_delete_by_retention() on the catalog then sleeps.
        let parquet_retention = if sub_config.enable_parquet_file_deletion {
            tokio::spawn(retention::parquet::perform(
                Arc::clone(&metric_registry),
                shutdown.clone(),
                Arc::clone(&catalog),
                sub_config.retention_sleep_interval_minutes,
            ))
        } else {
            warn!("parquet retention disabled");
            tokio::spawn(async {})
        };

        let partition_retention = if sub_config.enable_partition_row_deletion {
            tokio::spawn(retention::partition::perform(
                Arc::clone(&metric_registry),
                shutdown.clone(),
                catalog,
                sub_config.retention_sleep_interval_minutes,
            ))
        } else {
            warn!("partition row deletion disabled");
            tokio::spawn(async {})
        };

        Ok(Self {
            shutdown,
            catalog_backup,
            paths_in_catalog_backups,
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            partition_retention,
            parquet_retention,
        })
    }

    /// A handle to gracefully shutdown the garbage collector when invoked
    pub fn shutdown_handle(&self) -> impl Fn() {
        let shutdown = self.shutdown.clone();
        move || {
            shutdown.cancel();
        }
    }

    /// Wait for the garbage collector to finish work
    pub async fn join(self) -> Result<()> {
        let Self {
            catalog_backup,
            paths_in_catalog_backups: _,
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            partition_retention,
            parquet_retention,
            shutdown: _,
        } = self;

        let (
            catalog_backup,
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            partition_retention,
            parquet_retention,
        ) = futures::join!(
            catalog_backup,
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            partition_retention,
            parquet_retention,
        );

        parquet_retention.context(ParquetFileDeleterPanicSnafu)?;
        partition_retention.context(PartitionRetentionFlaggerPanicSnafu)?;
        pf_deleter.context(ParquetFileDeleterPanicSnafu)?;
        os_deleter.context(ObjectStoreDeleterPanicSnafu)?;
        os_checker.context(ObjectStoreCheckerPanicSnafu)??;
        os_lister.context(ObjectStoreListerPanicSnafu)??;
        catalog_backup.context(CatalogBackupPanicSnafu)?;

        Ok(())
    }

    /// Access to the bloom filter for testing purposes
    pub fn paths_in_catalog_backups(&self) -> &PathsInCatalogBackups {
        &self.paths_in_catalog_backups
    }
}

/// Configuration to run the object store garbage collector
#[derive(Clone)]
pub struct Config {
    /// The object store to garbage collect
    pub object_store: Arc<DynObjectStore>,

    /// The catalog to check if an object is garbage
    pub catalog: Arc<dyn Catalog>,

    /// The read replica catalog to query for active Parquet files to put in catalog file backup
    /// lists. If not specified, use `catalog`.
    pub replica_catalog: Option<Arc<dyn Catalog>>,

    /// DSN for the [catalog|replica] for generating pg_dump
    pub dump_dsn: Option<String>,

    /// The garbage collector specific configuration
    pub sub_config: GarbageCollectorConfig,
}

impl Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config (GarbageCollector")
            .field("sub_config", &self.sub_config)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error restoring bloom filters: {source}"))]
    BloomFilterRestore {
        source: catalog_backup_file_list::BloomFilterRestoreError,
    },

    #[snafu(display("The catalog backup task failed: {source}"))]
    CatalogBackupPanic { source: tokio::task::JoinError },

    #[snafu(display("The object store lister task failed"))]
    #[snafu(context(false))]
    ObjectStoreLister { source: os_lister::Error },
    #[snafu(display("The object store lister task panicked"))]
    ObjectStoreListerPanic { source: tokio::task::JoinError },

    #[snafu(display("The object store checker task failed"))]
    #[snafu(context(false))]
    ObjectStoreChecker { source: os_checker::Error },
    #[snafu(display("The object store checker task panicked"))]
    ObjectStoreCheckerPanic { source: tokio::task::JoinError },

    #[snafu(display("The object store deleter task panicked"))]
    ObjectStoreDeleterPanic { source: tokio::task::JoinError },

    #[snafu(display("The parquet file deleter task panicked"))]
    ParquetFileDeleterPanic { source: tokio::task::JoinError },

    #[snafu(display("The partition retention task panicked"))]
    PartitionRetentionFlaggerPanic { source: tokio::task::JoinError },

    #[snafu(display("The parquet file retention flagger task panicked"))]
    ParquetFileRetentionFlaggerPanic { source: tokio::task::JoinError },

    #[snafu(display("All garbage collector subtasks are disabled"))]
    GarbageCollectorDisabled,
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

fn next_five_minutes_after_the_hour(now: Time) -> Time {
    const FIVE_MINUTES: Duration = Duration::from_secs(5 * 60);

    // Get the most recent top of the hour in the past (or now if there's a rounding error)
    let most_recently_passed_hour = now.truncate_to_hour().unwrap_or(now);
    // Advance 5 minutes (or now if something has gone terribly wrong)
    let five_after = most_recently_passed_hour
        .checked_add(FIVE_MINUTES)
        .unwrap_or(now);
    // If that's in the past, we need the next hour
    if five_after < now {
        five_after.checked_add(ONE_HOUR).unwrap_or(five_after)
    } else {
        five_after
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap_blocks::{
        catalog_dsn::CatalogDsnConfig,
        object_store::{make_object_store, ObjectStoreConfig},
    };
    use filetime::FileTime;
    use futures::TryStreamExt;
    use iox_catalog::mem::MemCatalog;
    use iox_time::{MockProvider, SystemProvider};
    use object_store::memory::InMemory;
    use std::{fs, iter, path::PathBuf, time::Duration};
    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn computing_five_min_after_the_hour() {
        // Input "now" times
        let one_min_before_one_am = Time::from_rfc3339("2024-08-12T00:59:00+00:00").unwrap();
        let exactly_one_am = Time::from_rfc3339("2024-08-12T01:00:00+00:00").unwrap();
        let one_min_after_one_am = Time::from_rfc3339("2024-08-12T01:01:00+00:00").unwrap();
        let one_second_after_five_after_one_am =
            Time::from_rfc3339("2024-08-12T01:05:01+00:00").unwrap();

        // Expected start times
        let five_after_one = Time::from_rfc3339("2024-08-12T01:05:00+00:00").unwrap();
        let five_after_two = Time::from_rfc3339("2024-08-12T02:05:00+00:00").unwrap();

        assert_eq!(
            next_five_minutes_after_the_hour(one_min_before_one_am),
            five_after_one
        );
        assert_eq!(
            next_five_minutes_after_the_hour(exactly_one_am),
            five_after_one
        );
        assert_eq!(
            next_five_minutes_after_the_hour(one_min_after_one_am),
            five_after_one
        );
        assert_eq!(
            next_five_minutes_after_the_hour(one_second_after_five_after_one_am),
            five_after_two
        );
    }

    #[tokio::test]
    async fn does_not_create_catalog_backups_if_not_enabled() {
        // Say the garbage collector happens to start up 1 second before five minutes past the
        // hour, and there have never been any catalog backups taken.
        let time_provider = Arc::new(MockProvider::new(
            Time::from_rfc3339("2024-08-12T14:04:59+00:00").unwrap(),
        ));
        let sub_config = GarbageCollectorConfig::parse_from(iter::once("dummy-program-name"));
        let metric_registry = Arc::new(metric::Registry::new());
        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider) as _,
        ));

        let config = Config {
            object_store: Arc::clone(&object_store),
            catalog,
            replica_catalog: None,
            sub_config,
            dump_dsn: None,
        };
        tokio::spawn(async {
            main(config, metric_registry).await.unwrap();
        });
        sleep(Duration::from_secs(2)).await;
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
    async fn does_create_catalog_backups_if_enabled() {
        // Say the garbage collector happens to start up 1 second before five minutes past the
        // hour, and there have never been any catalog backups taken.
        let time_provider = Arc::new(MockProvider::new(
            Time::from_rfc3339("2024-08-12T14:04:59+00:00").unwrap(),
        ));
        let sub_config = GarbageCollectorConfig::parse_from([
            "dummy-program-name",
            "--create-catalog-backup-data-snapshot-files",
            "--enable-partition-row-deletion",
            "--enable-parquet-file-deletion",
            "--enable-object-store-deletion",
        ]);
        let metric_registry = Arc::new(metric::Registry::new());
        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider) as _,
        ));

        let config = Config {
            object_store: Arc::clone(&object_store),
            catalog,
            replica_catalog: None,
            sub_config,
            dump_dsn: None,
        };
        tokio::spawn(async {
            main(config, metric_registry).await.unwrap();
        });
        sleep(Duration::from_secs(2)).await;
        // There should be one backup file in object storage
        let all_object_store_paths: Vec<_> = object_store
            .list(None)
            .map_ok(|object_meta| object_meta.location)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            all_object_store_paths.len(),
            1,
            "Expected one object store path, got {all_object_store_paths:?}"
        );
    }

    #[tokio::test]
    async fn deletes_untracked_files_older_than_the_cutoff() {
        let setup = OldFileSetup::new();
        let metric_registry = Arc::new(metric::Registry::new());

        let config = build_config(
            setup.data_dir_arg(),
            [
                "--objectstore-sleep-interval-minutes=0",
                "--enable-partition-row-deletion",
                "--enable-parquet-file-deletion",
                "--enable-object-store-deletion",
            ],
        )
        .await;
        tokio::spawn(async {
            main(config, metric_registry).await.unwrap();
        });

        // file-based objectstore only has one file, it can't take long
        sleep(Duration::from_millis(500)).await;

        assert!(
            !setup.file_path.exists(),
            "The path {} should have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    #[tokio::test]
    async fn preserves_untracked_files_newer_than_the_cutoff() {
        let setup = OldFileSetup::new();
        let metric_registry = Arc::new(metric::Registry::new());

        #[rustfmt::skip]
        let config = build_config(setup.data_dir_arg(), [
            "--objectstore-cutoff", "10y",
        ]).await;
        tokio::spawn(async {
            main(config, metric_registry).await.unwrap();
        });

        // file-based objectstore only has one file, it can't take long
        sleep(Duration::from_millis(500)).await;

        assert!(
            setup.file_path.exists(),
            "The path {} should not have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    async fn build_config(data_dir: &str, args: impl IntoIterator<Item = &str> + Send) -> Config {
        let sub_config =
            GarbageCollectorConfig::parse_from(iter::once("dummy-program-name").chain(args));
        let object_store = object_store(data_dir);
        let catalog = catalog().await;

        Config {
            object_store,
            catalog,
            replica_catalog: None,
            sub_config,
            dump_dsn: None,
        }
    }

    fn object_store(data_dir: &str) -> Arc<DynObjectStore> {
        #[rustfmt::skip]
        let cfg = ObjectStoreConfig::parse_from([
            "dummy-program-name",
            "--object-store", "file",
            "--data-dir", data_dir,
        ]);
        make_object_store(&cfg).unwrap()
    }

    async fn catalog() -> Arc<dyn Catalog> {
        #[rustfmt::skip]
        let cfg = CatalogDsnConfig::parse_from([
            "dummy-program-name",
            "--catalog-dsn", "memory",
        ]);

        let metrics = metric::Registry::default().into();
        let time_provider = Arc::new(SystemProvider::new());

        cfg.get_catalog(
            "garbage_collector",
            metrics,
            time_provider,
            "uber-trace-id".to_owned(),
        )
        .await
        .unwrap()
    }

    struct OldFileSetup {
        data_dir: TempDir,
        file_path: PathBuf,
    }

    impl OldFileSetup {
        const APRIL_9_2018: FileTime = FileTime::from_unix_time(1523308536, 0);

        fn new() -> Self {
            let data_dir = TempDir::new().unwrap();

            let file_path = data_dir.path().join("some-old-file");
            fs::write(&file_path, "dummy content").unwrap();
            filetime::set_file_mtime(&file_path, Self::APRIL_9_2018).unwrap();

            Self {
                data_dir,
                file_path,
            }
        }

        fn data_dir_arg(&self) -> &str {
            self.data_dir.path().to_str().unwrap()
        }
    }
}
