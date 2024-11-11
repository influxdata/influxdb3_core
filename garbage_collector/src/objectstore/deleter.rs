use crate::PathsInCatalogBackups;

use catalog_backup_file_list::catalog_backup_directory_with_bloom_filter_file;
use futures::StreamExt;
use metric::U64Counter;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Structure that holds metrics for the garbage collector deleter
#[derive(Debug)]
struct DeleterMetrics {
    // Track how many items were successfully deleted
    object_delete_success_count: U64Counter,

    // Track how many items failed to be deleted
    object_delete_error_count: U64Counter,
}

impl DeleterMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let deleted_object_count = metric_registry.register_metric::<U64Counter>(
            "gc_deleter_deleted_objects",
            "GC deleter deleted object counts, bucketed success/failure.",
        );
        let object_delete_success_count = deleted_object_count.recorder(&[("result", "success")]);
        let object_delete_error_count = deleted_object_count.recorder(&[("result", "error")]);

        Self {
            object_delete_success_count,
            object_delete_error_count,
        }
    }
}

pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    shutdown: CancellationToken,
    object_store: Arc<DynObjectStore>,
    items: mpsc::Receiver<ObjectMeta>,
    paths_in_catalog_backups: Arc<PathsInCatalogBackups>,
) {
    let locations = tokio_stream::wrappers::ReceiverStream::new(items).map(|item| item.location);
    let metrics = DeleterMetrics::new(metric_registry);

    let stream_fu = async move {
        object_store
            .delete_stream(
                locations
                    .map(|path| {
                        info!(%path, "Deleting");
                        Ok(path)
                    })
                    .boxed(),
            )
            .for_each(|ret| async {
                match ret {
                    Ok(path) => {
                        metrics.object_delete_success_count.inc(1);
                        // If a catalog snapshot has just fallen out of
                        // retention and has been deleted, then the file list
                        // bloom filter must be reloaded to remove the entries
                        // that existed in the now-deleted snapshot.
                        if catalog_backup_directory_with_bloom_filter_file(&path).is_some() {
                            if let Err(e) =
                                paths_in_catalog_backups.reload_from_object_store().await
                            {
                                // If reloading the bloom filter fails, the GC
                                // MAY continue operating, which will result in
                                // GC incorrectly retaining the files that
                                // belong to the just-deleted snapshot until the
                                // next successful bloom filter reload.
                                warn!("error reloading bloom filters from object storage: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        metrics.object_delete_error_count.inc(1);
                        warn!("error deleting files from object storage: {e}");
                    }
                }
            })
            .await
    };

    tokio::select! {
        _ = shutdown.cancelled() => {
            // Exit gracefully
        }
        _ = stream_fu => {
            // Exit if the stream is closed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::ARBITRARY_BAD_OBJECT_META;

    use bytes::Bytes;
    use catalog_backup_file_list::{
        BloomFilter, BloomFilterFilePath, CatalogBackupDirectory, DesiredFileListTime,
    };
    use data_types::{NamespaceId, ObjectStoreId, PartitionId, TableId, TransitionPartitionId};
    use iox_time::{SystemProvider, TimeProvider};
    use metric::{assert_counter, Attributes, Metric};
    use object_store::{path::Path, PutPayload};
    use parquet_file::ParquetFilePath;
    use std::time::Duration;

    #[tokio::test]
    async fn perform_shutdown_gracefully() {
        let shutdown = CancellationToken::new();
        let metric_registry = Arc::new(metric::Registry::new());
        let nitems = 3;
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let items = populate_os_with_items(&object_store, nitems).await;
        let paths_in_catalog_backups = Arc::new(PathsInCatalogBackups::empty_for_testing(true));

        assert_eq!(count_os_element(&object_store).await, nitems);

        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn({
            let shutdown = shutdown.clone();

            async move {
                for item in items {
                    tx.send(item.clone()).await.unwrap();
                }

                // Send a shutdown signal
                shutdown.cancel();

                // Prevent this thread from exiting. Exiting this thread will
                // close the channel, which in turns close the processing stream.
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        });

        // This call should terminate because we send shutdown signal, but
        // nothing can be said about the number of elements in object store.
        // The processing stream may or may not have chance to process the
        // items for deletion.
        let perform_fu = perform(
            Arc::clone(&metric_registry),
            shutdown,
            Arc::clone(&object_store),
            rx,
            paths_in_catalog_backups,
        );
        // Unusual test because there is no assertion but the call below should
        // not panic which verifies that the deleter task shutdown gracefully.
        tokio::time::timeout(Duration::from_secs(3), perform_fu)
            .await
            .unwrap();

        // Normally we would call `assert_counter!()` here, but that
        // assertion fails in both cases because we immediately issue
        // a shutdown signal to the deleter. So nothing gets deleted
        // and there is no success or error metric to assert.
    }

    #[tokio::test]
    async fn shutdown_on_stream_close() {
        let metric_registry = Arc::new(metric::Registry::new());
        let shutdown = CancellationToken::new();
        let nitems = 3;
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let items = populate_os_with_items(&object_store, nitems).await;
        let paths_in_catalog_backups = Arc::new(PathsInCatalogBackups::empty_for_testing(true));

        assert_eq!(count_os_element(&object_store).await, nitems);

        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            for item in items {
                tx.send(item.clone()).await.unwrap();
            }

            // drop tx, closing the channel
        });

        // This call should terminate because we closed the channel, but
        // nothing can be said about the number of elements in object store.
        // The processing stream may or may not have chance to process the
        // items for deletion.
        let perform_fu = perform(
            Arc::clone(&metric_registry),
            shutdown,
            Arc::clone(&object_store),
            rx,
            paths_in_catalog_backups,
        );
        // Unusual test because there is no assertion but the call below should
        // not panic which verifies that the deleter task shutdown gracefully.
        tokio::time::timeout(Duration::from_secs(3), perform_fu)
            .await
            .unwrap();

        assert_object_delete_success_counter(&metric_registry, nitems as u64);
        assert_object_delete_error_counter(&metric_registry, 0);
    }

    #[tokio::test]
    async fn delete_bloom_reloads_union() {
        let metric_registry = Arc::new(metric::Registry::new());
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let time_provider = SystemProvider::new();

        let parquet_file_path1 = new_object_meta_location();
        let parquet_file_path2 = new_object_meta_location();

        // Save 2 bloom filters to object storage
        let mut bloom_filter1 = BloomFilter::empty();
        bloom_filter1.insert(&parquet_file_path1);
        let directory1 = CatalogBackupDirectory::new(DesiredFileListTime::new(time_provider.now()));
        bloom_filter1
            .save(&object_store, &directory1)
            .await
            .unwrap();
        let mut bloom_filter2 = BloomFilter::empty();
        bloom_filter2.insert(&parquet_file_path2);
        let directory2 = CatalogBackupDirectory::new(DesiredFileListTime::new(time_provider.now()));
        bloom_filter2
            .save(&object_store, &directory2)
            .await
            .unwrap();

        // Load unified bloom filter from object storage; should contain both paths
        let paths_in_catalog_backups = Arc::new(
            PathsInCatalogBackups::try_new(&object_store, true)
                .await
                .unwrap(),
        );
        assert!(paths_in_catalog_backups.contains(&parquet_file_path1));
        assert!(paths_in_catalog_backups.contains(&parquet_file_path2));

        // Delete the first bloom filter file
        let items = vec![ObjectMeta {
            location: BloomFilterFilePath::new(&directory1).object_store_path(),
            last_modified: time_provider.now().date_time(),
            size: 0,
            e_tag: None,
            version: None,
        }];
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            for item in items {
                tx.send(item.clone()).await.unwrap();
            }
            // Drop tx, closing channel.
        });

        let perform_fu = perform(
            Arc::clone(&metric_registry),
            CancellationToken::new(),
            Arc::clone(&object_store),
            rx,
            Arc::clone(&paths_in_catalog_backups),
        );
        tokio::time::timeout(Duration::from_secs(3), perform_fu)
            .await
            .unwrap();
        assert_object_delete_success_counter(&metric_registry, 1);

        // That should have reloaded the unified bloom filter; should contain only the 2nd path
        assert!(!paths_in_catalog_backups.contains(&parquet_file_path1));
        assert!(paths_in_catalog_backups.contains(&parquet_file_path2));
    }

    #[tokio::test]
    async fn try_delete_invalid_items() {
        let metric_registry = Arc::new(metric::Registry::new());
        let shutdown = CancellationToken::new();
        let nitems = 3;
        let object_store: Arc<DynObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let items = (0..nitems)
            .map(|_| ARBITRARY_BAD_OBJECT_META.clone())
            .collect::<Vec<ObjectMeta>>();
        let paths_in_catalog_backups = Arc::new(PathsInCatalogBackups::empty_for_testing(true));

        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            for item in items {
                tx.send(item.clone()).await.unwrap();
            }
            // Drop tx, closing channel.
        });

        // Spawn a thread to run the deleter loop; spawn another thread to cancel the
        // the deleter loop once we have a metric indicating a successful run.
        let (perform_res, loop_res) = (
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                let shutdown = shutdown.clone();
                async move {
                    perform(
                        Arc::clone(&metric_registry),
                        shutdown,
                        Arc::clone(&object_store),
                        rx,
                        paths_in_catalog_backups,
                    )
                    .await
                }
            }),
            tokio::spawn({
                let metric_registry = Arc::clone(&metric_registry);
                async move {
                    loop {
                        if let Some(metric) = metric_registry
                            .get_instrument::<Metric<U64Counter>>("gc_deleter_deleted_objects")
                        {
                            let observation = metric
                                .recorder(Attributes::from(&[("result", "error")]))
                                .fetch();
                            // We created 3 invalid items, so we expect 3 errors.
                            if observation == 3 {
                                break;
                            }
                        }
                    }
                }
            })
            .await,
        );

        assert_object_delete_error_counter(&metric_registry, nitems as u64);
        assert_object_delete_success_counter(&metric_registry, 0);
        // Verify that retention loop has finished (shutdown)
        // and that the loop exited gracefully.
        assert!(perform_res.is_finished());
        assert!(loop_res.is_ok());
    }

    async fn count_os_element(os: &Arc<DynObjectStore>) -> usize {
        let objects = os.list(None);
        objects.fold(0, |acc, _| async move { acc + 1 }).await
    }

    async fn populate_os_with_items(os: &Arc<DynObjectStore>, nitems: usize) -> Vec<ObjectMeta> {
        let mut items = vec![];
        for i in 0..nitems {
            let object_meta = ObjectMeta {
                location: new_object_meta_location(),
                last_modified: SystemProvider::new().now().date_time(),
                size: 0,
                e_tag: None,
                version: None,
            };
            os.put(
                &object_meta.location,
                PutPayload::from(Bytes::from(i.to_string())),
            )
            .await
            .unwrap();
            items.push(object_meta);
        }
        items
    }

    fn new_object_meta_location() -> Path {
        ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Catalog(PartitionId::new(4)),
            ObjectStoreId::new(),
        )
        .object_store_path()
    }

    fn assert_object_delete_success_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_deleter_deleted_objects",
            labels = Attributes::from(&[("result", "success")]),
            value = value,
        );
    }

    fn assert_object_delete_error_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_deleter_deleted_objects",
            labels = Attributes::from(&[("result", "error")]),
            value = value,
        );
    }
}
