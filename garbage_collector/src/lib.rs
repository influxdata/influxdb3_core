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

/// Logic for listing, checking and deleting files in object storage
mod objectstore;
/// Logic for deleting parquet files from the catalog
mod parquetfile;
/// Logic for flagging parquet files for deletion based on retention settings
mod retention;

const BUFFER_SIZE: usize = 1000;

/// Run the tasks that clean up old object store files that don't appear in the catalog.
pub async fn main(config: Config) -> Result<()> {
    GarbageCollector::start(config)?.join().await
}

/// The tasks that clean up old object store files that don't appear in the catalog.
pub struct GarbageCollector {
    shutdown: CancellationToken,
    os_lister: tokio::task::JoinHandle<Result<(), os_lister::Error>>,
    os_checker: tokio::task::JoinHandle<Result<(), os_checker::Error>>,
    os_deleter: tokio::task::JoinHandle<Result<(), os_deleter::Error>>,
    pf_deleter: tokio::task::JoinHandle<Result<(), pf_deleter::Error>>,
    partition_retention: Option<tokio::task::JoinHandle<Result<(), retention::partition::Error>>>,
    parquet_retention: tokio::task::JoinHandle<Result<(), retention::parquet::Error>>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish_non_exhaustive()
    }
}

impl GarbageCollector {
    /// Construct the garbage collector and start it
    pub fn start(config: Config) -> Result<Self> {
        let Config {
            object_store,
            sub_config,
            catalog,
        } = config;

        let dry_run = sub_config.dry_run;
        info!(
            objectstore_cutoff = %format_duration(sub_config.objectstore_cutoff),
            bulk_ingest_objectstore_cutoff = %format_duration(sub_config.bulk_ingest_objectstore_cutoff),
            parquetfile_cutoff = %format_duration(sub_config.parquetfile_cutoff),
            parquetfile_sleep_interval = %format_duration(sub_config.parquetfile_sleep_interval()),
            objectstore_sleep_interval_minutes = %sub_config.objectstore_sleep_interval_minutes,
            retention_sleep_interval_minutes = %sub_config.retention_sleep_interval_minutes,
            partition_retention = %sub_config.partition_retention,
            "GarbageCollector starting"
        );

        // Shutdown handler channel to notify children
        let shutdown = CancellationToken::new();

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

        let os_lister = tokio::spawn(async move {
            select! {
                ret = os_lister::perform(
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

        let os_checker = tokio::spawn(async move {
            select! {
                ret = os_checker::perform(
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

        let os_deleter = tokio::spawn(os_deleter::perform(
            shutdown.clone(),
            object_store,
            dry_run,
            rx2,
        ));

        // Initialise the parquet file deleter, which is just one thread that calls delete_old()
        // on the catalog then sleeps.
        let pf_deleter = tokio::spawn(pf_deleter::perform(
            shutdown.clone(),
            Arc::clone(&catalog),
            sub_config.parquetfile_cutoff,
            sub_config.parquetfile_sleep_interval(),
        ));

        // Initialise the retention code, which is just one thread that calls
        // flag_for_delete_by_retention() on the catalog then sleeps.
        let parquet_retention = tokio::spawn(retention::parquet::perform(
            shutdown.clone(),
            Arc::clone(&catalog),
            sub_config.retention_sleep_interval_minutes,
            sub_config.dry_run,
        ));

        let partition_retention = sub_config.partition_retention.then(|| {
            tokio::spawn(retention::partition::perform(
                shutdown.clone(),
                catalog,
                sub_config.retention_sleep_interval_minutes,
            ))
        });

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

        let (os_lister, os_checker, os_deleter, pf_deleter, parquet_retention) = futures::join!(
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            parquet_retention
        );

        if let Some(p) = partition_retention {
            p.await.context(PartitionRetentionFlaggerPanicSnafu)??;
        }

        parquet_retention.context(ParquetFileDeleterPanicSnafu)??;
        pf_deleter.context(ParquetFileDeleterPanicSnafu)??;
        os_deleter.context(ObjectStoreDeleterPanicSnafu)??;
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

    #[snafu(display("The object store deleter task failed"))]
    #[snafu(context(false))]
    ObjectStoreDeleter { source: os_deleter::Error },
    #[snafu(display("The object store deleter task panicked"))]
    ObjectStoreDeleterPanic { source: tokio::task::JoinError },

    #[snafu(display("The parquet file deleter task failed"))]
    #[snafu(context(false))]
    ParquetFileDeleter { source: pf_deleter::Error },
    #[snafu(display("The parquet file deleter task panicked"))]
    ParquetFileDeleterPanic { source: tokio::task::JoinError },

    #[snafu(display("The parquet file retention flagger task failed"))]
    #[snafu(context(false))]
    ParquetFileRetentionFlagger { source: retention::parquet::Error },

    #[snafu(display("The partition retention task failed"))]
    #[snafu(context(false))]
    PartitionRetention { source: retention::partition::Error },

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

        let config = build_config(
            setup.data_dir_arg(),
            ["--objectstore-sleep-interval-minutes=0"],
        )
        .await;
        tokio::spawn(async {
            main(config).await.unwrap();
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

        #[rustfmt::skip]
        let config = build_config(setup.data_dir_arg(), [
            "--objectstore-cutoff", "10y",
        ]).await;
        tokio::spawn(async {
            main(config).await.unwrap();
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
