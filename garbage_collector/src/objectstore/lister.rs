use futures::prelude::*;
use metric::DurationHistogram;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, time::sleep};

/// Object store implementations will generally list all objects in the bucket/prefix. This limits
/// the total items pulled (assuming lazy streams) at a time to limit impact on the catalog.
/// Consider increasing this if throughput is an issue or shortening the loop/list sleep intervals.
/// Listing will list all files, including those not to be deleted, which may be a very large number.
const MAX_ITEMS_PROCESSED_PER_LOOP: usize = 10_000;

/// Structure that holds metrics for the garbage collector lister
#[derive(Debug)]
struct ListerMetrics {
    // Track how long succesful lister runs take
    run_success_duration: DurationHistogram,

    // Track how long failed lister runs take
    run_error_duration: DurationHistogram,
}

impl ListerMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let lister_runtime_duration_seconds = metric_registry.register_metric::<DurationHistogram>(
            "gc_lister_runtime_duration",
            "GC lister loop run-times, bucketed by operation success or failure.",
        );
        let run_success_duration =
            lister_runtime_duration_seconds.recorder(&[("result", "success")]);
        let run_error_duration = lister_runtime_duration_seconds.recorder(&[("result", "error")]);

        Self {
            run_success_duration,
            run_error_duration,
        }
    }
}

/// perform a object store list, limiting to ['MAX_ITEMS_PROCESSED_PER_LOOP'] files at a time,
/// waiting sleep interval before listing afresh.
pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
    sleep_interval_iteration_minutes: u64,
    sleep_interval_list_page_milliseconds: u64,
) -> Result<()> {
    info!("beginning object store listing");

    let metrics = ListerMetrics::new(metric_registry);

    loop {
        let lister_runtime_start = Instant::now();
        // there are issues with the service immediately hitting the os api (credentials, etc) on
        // startup. Retry as needed.
        let items = object_store.list(None);

        let mut chunked_items = items.chunks(MAX_ITEMS_PROCESSED_PER_LOOP);

        let mut count = 0;
        while let Some(v) = chunked_items.next().await {
            match process_item_list(v, &checker).await {
                // If the checker channel has closed, we should shut down too
                Err(e @ Error::CheckerExited { .. }) => {
                    warn!("checker has exited, stopping: {e}");
                    return Err(e);
                }
                // relist and sleep on an error to allow time for transient errors to dissipate
                // todo(pjb): react differently to different errors
                Err(e) => {
                    metrics
                        .run_error_duration
                        .record(lister_runtime_start.elapsed());

                    warn!("error processing items from object store, continuing: {e}");
                    // go back to start of loop to list again, hopefully to get past error.
                    break;
                }
                Ok(i) => {
                    count += i;
                }
            }
            sleep(Duration::from_millis(sleep_interval_list_page_milliseconds)).await;
            debug!("starting next chunk of listed files");
        }
        let lister_runtime_seconds = lister_runtime_start.elapsed();

        info!("end of object store item list; listed {count} files: will relist in {sleep_interval_iteration_minutes} minutes");
        let sleep_duration = Duration::from_secs(60 * sleep_interval_iteration_minutes);

        metrics.run_success_duration.record(lister_runtime_seconds);

        sleep(sleep_duration).await;
    }
}

async fn process_item_list(
    items: Vec<object_store::Result<ObjectMeta>>,
    checker: &mpsc::Sender<ObjectMeta>,
) -> Result<i32> {
    let mut i = 0;
    // TODO(10211): Remove this or make it configurable.
    let skip = object_store::path::Path::from_url_path("iceberg").expect("static input");
    for item in items {
        let item = item.context(MalformedSnafu)?;
        if item.location.prefix_matches(&skip) {
            debug!(location = %item.location, "icebergexporter configuration file SKIPPED");
            continue;
        }
        debug!(location = %item.location, "Object store item");
        checker.send(item).await?;
        i += 1;
    }
    debug!("processed {i} files of listed chunk");
    Ok(i)
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("The prefix could not be listed: {source}"))]
    Listing { source: object_store::Error },

    #[snafu(display("The object could not be listed: {source}"))]
    Malformed { source: object_store::Error },

    #[snafu(display("The checker task exited unexpectedly: {source}"))]
    #[snafu(context(false))]
    CheckerExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use crate::objectstore::lister;
    use crate::objectstore::lister::Result;
    use crate::BUFFER_SIZE;
    use metric::{assert_histogram, Attributes, DurationHistogram};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectMeta, ObjectStore};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    const TEST_PAYLOAD: &str = "bananas";
    const TEST_PAYLOAD_PATH: &str = "test-lister-metrics-payload";

    async fn mock_gc_checker(mut items: mpsc::Receiver<ObjectMeta>) -> Result<()> {
        let maybe_item = timeout(Duration::from_secs(5), items.recv())
            .await
            .ok()
            .flatten();

        if let Some(item) = maybe_item {
            let path = Path::parse(TEST_PAYLOAD_PATH)
                .unwrap_or_else(|_| panic!("Failed to parse path from: {TEST_PAYLOAD_PATH}"));
            assert_eq!(item.location, path);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_lister_metrics() {
        let metric_registry = Arc::new(metric::Registry::new());
        let object_store = Arc::new(InMemory::new());
        let lister_metric_registry = Arc::clone(&metric_registry);

        let payload_path = Path::parse(TEST_PAYLOAD_PATH)
            .unwrap_or_else(|_| panic!("Failed to parse path from: {TEST_PAYLOAD_PATH}"));

        object_store
            .put(&payload_path, TEST_PAYLOAD.into())
            .await
            .expect("Failed to write to object store");

        let (tx1, rx1) = mpsc::channel(BUFFER_SIZE);

        // Ideally, we would check the `Result` returned by each of these threads. In
        // this case we don't check the lister result, because deadlock is introduced.
        // If the lister is `.await`ed, it will block until the channel it sends on
        // has closed, and the channel won't close unless it has received something.
        let (lister, checker_res) = (
            tokio::spawn(async move {
                lister::perform(lister_metric_registry, object_store, tx1, 1, 1000).await
            }),
            tokio::spawn(async move { mock_gc_checker(rx1).await }).await,
        );

        assert!(checker_res.is_ok());
        assert_histogram!(
            metric_registry,
            DurationHistogram,
            "gc_lister_runtime_duration",
            labels = Attributes::from(&[("result", "success"), ("result", "error")]),
            samples = 0,
        );
        // Check that lister hasn't exited
        // with an error.
        assert!(!lister.is_finished());
    }
}
