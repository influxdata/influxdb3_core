use data_types::Timestamp;
use iox_catalog::interface::Catalog;
use metric::{DurationHistogram, Metric, U64Histogram, U64HistogramOptions};
use observability_deps::tracing::*;
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
    cutoff: Duration,
    sleep_interval: Duration,
) {
    let metrics = DeleterMetrics::new(metric_registry);

    loop {
        let start = Instant::now();
        let older_than = Timestamp::from(catalog.time_provider().now() - cutoff);

        // do the delete, returning the deleted files. log any errors
        match catalog
            .repositories()
            .parquet_files()
            .delete_old_ids_only(older_than, cutoff) // read/write
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

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(sleep_interval) => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_catalog_and_file_with_max_time;
    use metric::{assert_histogram, Attributes, Metric};
    use std::assert_eq;

    #[tokio::test]
    async fn delete_old_file() {
        let shutdown = CancellationToken::new();
        let metric_registry = Arc::new(metric::Registry::new());
        let (catalog, _file) = create_catalog_and_file_with_max_time(Timestamp::new(1)).await;

        // Set cutoff to anything older than 5 nanoseconds
        let cutoff = Duration::from_nanos(5);
        let sleep_interval = Duration::from_secs(5);

        // Mark file as deletable, since it's older than the cutoff.
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
                        cutoff,
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
