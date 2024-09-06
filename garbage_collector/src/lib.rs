//! Tool to clean up old object store files that don't appear in the catalog.

#![warn(missing_docs)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
use clap as _;
use workspace_hack as _;

use crate::{
    objectstore::{checker as os_checker, deleter as os_deleter, lister as os_lister},
    parquetfile::deleter as pf_deleter,
};

use clap_blocks::garbage_collector::GarbageCollectorConfig;
use humantime::format_duration;
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{fmt::Debug, sync::Arc};
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

/// Run the tasks that clean up old object store files that don't appear in the catalog.
pub async fn main(config: Config, metric_registry: Arc<metric::Registry>) -> Result<()> {
    GarbageCollector::start(config, metric_registry)?
        .join()
        .await
}

/// The tasks that clean up old object store files that don't appear in the catalog.
pub struct GarbageCollector {
    shutdown: CancellationToken,
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
    pub fn start(config: Config, metric_registry: Arc<metric::Registry>) -> Result<Self> {
        let Config {
            object_store,
            sub_config,
            catalog,
            replica_catalog: _, // Will be used for catalog backup file lists; see below
        } = config;

        info!(
            objectstore_cutoff = %format_duration(sub_config.objectstore_cutoff),
            bulk_ingest_objectstore_cutoff = %format_duration(sub_config.bulk_ingest_objectstore_cutoff),
            parquetfile_cutoff = %format_duration(sub_config.parquetfile_cutoff),
            parquetfile_sleep_interval = %format_duration(sub_config.parquetfile_sleep_interval()),
            objectstore_sleep_interval_minutes = %sub_config.objectstore_sleep_interval_minutes,
            retention_sleep_interval_minutes = %sub_config.retention_sleep_interval_minutes,
            "GarbageCollector starting"
        );

        // Shutdown handler channel to notify children
        let shutdown = CancellationToken::new();

        // Soon there will be a thread that manages catalog backup file lists on a schedule. It
        // will be called something like this:
        //
        // let catalog_backup_file_lists = tokio::spawn(catalog_backup_file_lists::perform(
        //     shutdown.clone(),
        //     replica_catalog.unwrap_or_else(Arc::clone(&catalog)),
        //     Arc::clone(&object_store),
        // ));

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
        let os_lister = tokio::spawn(async move {
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
        });

        let cat = Arc::clone(&catalog);
        let sdt = shutdown.clone();
        let cutoff = chrono::Duration::from_std(sub_config.objectstore_cutoff).map_err(|e| {
            Error::CutoffError {
                message: e.to_string(),
            }
        })?;

        let bulk_ingest_cutoff =
            chrono::Duration::from_std(sub_config.bulk_ingest_objectstore_cutoff).map_err(|e| {
                Error::CutoffError {
                    message: e.to_string(),
                }
            })?;

        let checker_metric_registry = Arc::clone(&metric_registry);
        let os_checker = tokio::spawn(async move {
            select! {
                ret = os_checker::perform(
                    checker_metric_registry,
                    cat,
                    cutoff,
                    bulk_ingest_cutoff,
                    rx1,
                    tx2,
                ) => {
                    ret
                },
                _ = sdt.cancelled() => {
                    Ok(())
                },
            }
        });

        let os_deleter = tokio::spawn({
            os_deleter::perform(
                Arc::clone(&metric_registry),
                shutdown.clone(),
                object_store,
                rx2,
            )
        });

        let pf_deleter = tokio::spawn(pf_deleter::perform(
            Arc::clone(&metric_registry),
            shutdown.clone(),
            Arc::clone(&catalog),
            sub_config.parquetfile_cutoff,
            sub_config.parquetfile_sleep_interval(),
        ));

        // Initialise the retention code, which is just one thread that calls
        // flag_for_delete_by_retention() on the catalog then sleeps.
        let parquet_retention = tokio::spawn(retention::parquet::perform(
            Arc::clone(&metric_registry),
            shutdown.clone(),
            Arc::clone(&catalog),
            sub_config.retention_sleep_interval_minutes,
        ));

        let partition_retention = tokio::spawn(retention::partition::perform(
            Arc::clone(&metric_registry),
            shutdown.clone(),
            catalog,
            sub_config.retention_sleep_interval_minutes,
        ));

        Ok(Self {
            shutdown,
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
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            partition_retention,
            parquet_retention,
            shutdown: _,
        } = self;

        let (os_lister, os_checker, os_deleter, pf_deleter, partition_retention, parquet_retention) = futures::join!(
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

        Ok(())
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
    #[snafu(display("Error converting parsed duration: {message}"))]
    CutoffError { message: String },

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
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap_blocks::{
        catalog_dsn::CatalogDsnConfig,
        object_store::{make_object_store, ObjectStoreConfig},
    };
    use filetime::FileTime;
    use iox_time::SystemProvider;
    use std::{fs, iter, path::PathBuf, time::Duration};
    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn deletes_untracked_files_older_than_the_cutoff() {
        let setup = OldFileSetup::new();
        let metric_registry = Arc::new(metric::Registry::new());

        let config = build_config(
            setup.data_dir_arg(),
            ["--objectstore-sleep-interval-minutes=0"],
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
