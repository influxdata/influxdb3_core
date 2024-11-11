use async_trait::async_trait;
use catalog_backup_file_list::FindFileList;
use clap_blocks::garbage_collector::CutoffDuration;
use data_types::Timestamp;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use metric::{DurationHistogram, Metric, U64Histogram, U64HistogramOptions};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::Snafu;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

/// Structure that holds metrics for the garbage collector deleter
#[derive(Debug)]
struct DeleterMetrics {
    // Track how long successful parquet delete attempts take
    parquet_delete_success_duration: DurationHistogram,

    // Track how long failed parquet delete attempts take
    parquet_delete_error_duration: DurationHistogram,

    // Track how many parquet files were successfully deleted
    parquet_delete_count: U64Histogram,
}

impl DeleterMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let parquet_delete_count_metric: Metric<U64Histogram> = metric_registry
            .register_metric_with_options(
                "gc_deleter_deleted_parquets",
                "GC deleter deleted parquet counts",
                || U64HistogramOptions::new(vec![1, 10, 100, 1_000, 10_000, 100_000]),
            );
        let parquet_delete_count = parquet_delete_count_metric.recorder(&[("deleted", "parquet")]);

        let runtime_duration_seconds = metric_registry.register_metric::<DurationHistogram>(
            "gc_deleter_parquet_runtime_duration",
            "GC parquet deleter loop run-times, bucketed by operation success or failure.",
        );
        let parquet_delete_success_duration =
            runtime_duration_seconds.recorder(&[("result", "success")]);
        let parquet_delete_error_duration =
            runtime_duration_seconds.recorder(&[("result", "error")]);

        Self {
            parquet_delete_count,
            parquet_delete_success_duration,
            parquet_delete_error_duration,
        }
    }
}

pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    older_than_calculator: Arc<dyn OlderThanCalculator>,
    sleep_interval: Duration,
) {
    let metrics = DeleterMetrics::new(metric_registry);

    loop {
        let start = Instant::now();

        match older_than_calculator.older_than().await {
            Ok(older_than) => {
                delete_from_catalog(&metrics, Arc::clone(&catalog), older_than, start).await;
            }
            Err(e) => {
                warn!("error getting older_than time; continuing: {e}");
            }
        }

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(sleep_interval) => (),
        }
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum OlderThanCalculatorError {
    #[snafu(display("No catalog backup list files found"))]
    NoneFound,

    #[snafu(display("Error attempting to find file lists: {source}"))]
    Find {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

#[async_trait]
pub(crate) trait OlderThanCalculator: Send + Sync + 'static {
    async fn older_than(&self) -> Result<Timestamp, OlderThanCalculatorError>;
}

/// Use when the `older_than` value passed to the catalog `delete_old_ids_only` method should
/// always be approximately the configured Parquet file cutoff value ago from now.
pub(crate) struct PlainOldCutoffFromNow {
    /// The garbage collector's `SystemTimeProvider` is fine; this doesn't need to use the catalog's
    /// `NOW` value because if this value isn't coming from a catalog backup file list, it doesn't
    /// need to be exactly in sync.
    pub(crate) time_provider: Arc<dyn TimeProvider>,
    pub(crate) cutoff: CutoffDuration,
}

#[async_trait]
impl OlderThanCalculator for PlainOldCutoffFromNow {
    async fn older_than(&self) -> Result<Timestamp, OlderThanCalculatorError> {
        let now = self.time_provider.now();
        Ok(Timestamp::from(now - self.cutoff.duration()))
    }
}

/// Use when the `older_than` value passed to the catalog `delete_old_ids_only` method should be
/// whichever is older: the time at which the last successful catalog backup file list snapshot
/// was created, or approximately the configured Parquet file cutoff value ago from now.
///
/// This ensures that if catalog backups stop being created, we aren't cleaning up catalog records
/// that should be in a catalog backup.
pub(crate) struct CheckLastCatalogFileList {
    pub(crate) file_list_finder: Arc<DynObjectStore>,
    pub(crate) cutoff_from_now: PlainOldCutoffFromNow,
}

#[async_trait]
impl OlderThanCalculator for CheckLastCatalogFileList {
    async fn older_than(&self) -> Result<Timestamp, OlderThanCalculatorError> {
        match self.file_list_finder.most_recent().await {
            Ok(Some(most_recent)) => {
                let cutoff_ago: Timestamp = self
                    .cutoff_from_now
                    .older_than()
                    .await
                    .expect("PlainOldCutoffFromNow::older_than is infallible");
                let most_recent_timestamp: Timestamp = most_recent.time().into();
                Ok(std::cmp::min(most_recent_timestamp, cutoff_ago))
            }
            Ok(None) => Err(OlderThanCalculatorError::NoneFound),
            Err(source) => Err(OlderThanCalculatorError::Find { source }),
        }
    }
}

/// Call the catalog method that will delete Parquet file records older than `cutoff` ago.
/// If successful, log and add to metrics the number of deleted files. If unsuccessful, log and add
/// to metrics the error information.
async fn delete_from_catalog(
    metrics: &DeleterMetrics,
    catalog: Arc<dyn Catalog>,
    older_than: Timestamp,
    start: Instant,
) {
    match catalog
        .repositories()
        .parquet_files()
        .delete_old_ids_only(older_than) // read/write
        .await
    {
        Ok(deleted) => {
            let elapsed = start.elapsed();
            info!(delete_count = %deleted.len(), ?elapsed, "iox_catalog::delete_old()");
            metrics.parquet_delete_count.record(deleted.len() as u64);
            metrics.parquet_delete_success_duration.record(elapsed);
        }
        Err(e) => {
            metrics.parquet_delete_count.record(0);
            metrics
                .parquet_delete_error_duration
                .record(start.elapsed());
            warn!("error deleting old parquet files from the catalog, continuing: {e}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_catalog_and_file_with_max_time;
    use iox_time::SystemProvider;
    use metric::{assert_histogram, Attributes, Metric};

    #[tokio::test]
    async fn delete_old_file() {
        let shutdown = CancellationToken::new();
        let metric_registry = Arc::new(metric::Registry::new());
        let (catalog, _file) = create_catalog_and_file_with_max_time(Timestamp::new(1)).await;
        let time_provider = Arc::new(SystemProvider::new());

        // Set cutoff to anything older than 5 hours
        let cutoff_calculator = Arc::new(PlainOldCutoffFromNow {
            time_provider: Arc::clone(&time_provider) as _,
            cutoff: CutoffDuration::try_new("5h").unwrap(),
        });
        let sleep_interval = Duration::from_secs(5);

        // Mark file as deletable, since its max time is older than the 60ns namespace retention
        // period ago
        catalog
            .repositories()
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();

        // Spawn a thread to run the parquet retention loop; spawn another thread to cancel the
        // the parquet retention loop once we have a metric indicating a successful run.
        let (perform_res, loop_res) = (
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                let shutdown = shutdown.clone();
                async move {
                    perform(
                        Arc::clone(&metric_registry),
                        shutdown.clone(),
                        catalog,
                        cutoff_calculator,
                        sleep_interval,
                    )
                    .await
                }
            }),
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                let shutdown = shutdown.clone();
                async move {
                    loop {
                        if let Some(metric) = metric_registry
                            .get_instrument::<Metric<U64Histogram>>("gc_deleter_deleted_parquets")
                        {
                            let observation = metric
                                .recorder(Attributes::from(&[("deleted", "parquet")]))
                                .fetch();
                            if observation.sample_count() == 1 {
                                shutdown.cancel();
                                break;
                            }
                        }
                    }
                }
            })
            .await,
        );

        // Samples should still be present
        // in metrics after loop has finished.
        assert_histogram!(
            metric_registry,
            DurationHistogram,
            "gc_deleter_parquet_runtime_duration",
            labels = Attributes::from(&[("result", "success")]),
            samples = 1,
        );
        // We should have exactly one 'success' sample in the histogram.
        // Prove it.
        assert_histogram!(
            metric_registry,
            DurationHistogram,
            "gc_deleter_parquet_runtime_duration",
            labels = Attributes::from(&[("result", "error")]),
            samples = 0,
        );
        assert_histogram!(
            metric_registry,
            U64Histogram,
            "gc_deleter_deleted_parquets",
            labels = Attributes::from(&[("deleted", "parquet")]),
            samples = 1,
        );
        // Verify that retention loop has finished (shutdown)
        // and that the loop exited gracefully.
        assert!(perform_res.is_finished());
        assert!(loop_res.is_ok());
    }
}
