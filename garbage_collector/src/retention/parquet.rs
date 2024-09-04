use iox_catalog::{constants::MAX_PARQUET_L0_FILES_PER_PARTITION, interface::Catalog};
use metric::DurationHistogram;
use observability_deps::tracing::*;
use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    time::{sleep, Instant},
};
use tokio_util::sync::CancellationToken;

// Metrics for parquet retention
struct ParquetRetentionMetrics {
    // Track how long successful parquet retention loops take
    runtime_success_duration: DurationHistogram,

    // Track how long failed parquet retention loops take
    runtime_error_duration: DurationHistogram,
}

impl ParquetRetentionMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let parquet_retention_runtime_seconds = metric_registry
            .register_metric::<DurationHistogram>(
                "gc_parquet_retention_runtime",
                "GC parquet retention runtimes, bucketed by success/failure.",
            );
        let runtime_success_duration =
            parquet_retention_runtime_seconds.recorder(&[("result", "success")]);
        let runtime_error_duration =
            parquet_retention_runtime_seconds.recorder(&[("result", "error")]);

        Self {
            runtime_success_duration,
            runtime_error_duration,
        }
    }
}

pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    default_sleep_interval_minutes: u64,
) {
    let metrics = ParquetRetentionMetrics::new(metric_registry);
    let mut sleep_interval_minutes = default_sleep_interval_minutes;
    loop {
        let start = Instant::now();
        match catalog
            .repositories()
            .parquet_files()
            .flag_for_delete_by_retention() // read/write
            .await
        {
            Ok(flagged) => {
                info!(flagged_count = %flagged.len(), "gc::retention::parquet");

                if flagged.len() == MAX_PARQUET_L0_FILES_PER_PARTITION as usize {
                    // Since this enforcement is run every few minutes, there shouldn't ever be
                    // a massive spike in files to delete. They should trickle in. So if we're
                    // deleting the max we can per call, we're probably falling behind day by
                    // day.
                    if sleep_interval_minutes > 1 {
                        sleep_interval_minutes /= 2;
                    }
                } else if sleep_interval_minutes < default_sleep_interval_minutes {
                    sleep_interval_minutes *= 2;
                }
                metrics.runtime_success_duration.record(start.elapsed());
            }
            Err(e) => {
                metrics.runtime_error_duration.record(start.elapsed());
                warn!(
                    "error flagging parquet files for delete by retention from the catalog, \
                    continuing: {e}"
                );
            }
        };

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(Duration::from_secs(60 * sleep_interval_minutes)) => (),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::create_catalog_and_file_with_max_time;
    use data_types::Timestamp;
    use metric::{assert_histogram, Attributes, Metric};

    #[tokio::test]
    async fn delete_old_file() {
        let metric_registry = Arc::new(metric::Registry::new());
        let shutdown = CancellationToken::new();
        let (catalog, _file) = create_catalog_and_file_with_max_time(Timestamp::new(1)).await;

        // Spawn a thread to run the parquet retention loop; spawn another thread to cancel the
        // the parquet retention loop once we have a metric indicating a successful run.
        let (perform_res, loop_res) = (
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                let shutdown = shutdown.clone();
                async move { perform(Arc::clone(&metric_registry), shutdown.clone(), catalog, 1).await }
            }),
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                let shutdown = shutdown.clone();
                async move {
                    loop {
                        if let Some(metric) = metric_registry
                            .get_instrument::<Metric<DurationHistogram>>(
                                "gc_parquet_retention_runtime",
                            )
                        {
                            let observation = metric
                                .recorder(Attributes::from(&[("result", "success")]))
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
        // even after loop has exited.
        assert_histogram!(
            metric_registry,
            DurationHistogram,
            "gc_parquet_retention_runtime",
            labels = Attributes::from(&[("result", "success")]),
            samples = 1,
        );
        // If we succeeded exactly once
        // we should have failed 0 times.
        assert_histogram!(
            metric_registry,
            DurationHistogram,
            "gc_parquet_retention_runtime",
            labels = Attributes::from(&[("result", "error")]),
            samples = 0,
        );
        // Verify that retention loop has finished (shutdown)
        // and that the loop exited gracefully.
        assert!(perform_res.is_finished());
        assert!(loop_res.is_ok());
    }
}
