mod file_classifier;
mod lists;

use chrono::{DateTime, Duration, Utc};
use data_types::ObjectStoreId;
use file_classifier::FileClassifier;
use iox_catalog::interface::{Catalog, ParquetFileRepo};
use lists::ImmediateDeletionList;
use metric::U64Counter;
use object_store::ObjectMeta;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected a file name"))]
    FileNameMissing,

    #[snafu(display("Channel closed unexpectedly"))]
    ChannelClosed,

    #[snafu(display("The catalog could not be queried for {object_store_id}"))]
    GetFile {
        source: iox_catalog::interface::Error,
        object_store_id: uuid::Uuid,
    },

    #[snafu(display("The catalog could not be queried for the batch"))]
    FileExists {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("The deleter task exited unexpectedly"))]
    DeleterExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

/// The number of parquet files we will ask the catalog to look for at once.
// todo(pjb): I have no idea what's a good value here to amortize the request. More than 1 is a start.
// Here's the idea: group everything you can for 100ms `RECEIVE_TIMEOUT`, because that's not so much
// of a delay that it would cause issues, but if you manage to get a huge number of file ids, stop
// accumulating at 100 `CATALOG_BATCH_SIZE`.
const CATALOG_BATCH_SIZE: usize = 100;
const RECEIVE_TIMEOUT: core::time::Duration = core::time::Duration::from_millis(100); // This may not be long enough to collect many objects.

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
struct CheckerMetrics {
    // Track how frequently we encounter errors when querying the catalog
    catalog_query_error_count: U64Counter,

    // Track how frequently we encounter missing file names
    missing_filename_error_count: U64Counter,

    // Track how frequently we encounter non-parquet files
    unexpected_file_count: U64Counter,

    // Track how frequently we encounter invalid UUIDs
    invalid_parquet_filename_count: U64Counter,
}

impl CheckerMetrics {
    fn new(metric_registry: Arc<metric::Registry>) -> Self {
        let checker_error_count = metric_registry.register_metric::<U64Counter>(
            "gc_checker_error",
            "GC checker error counts, bucketed by type.",
        );
        let catalog_query_error_count = checker_error_count.recorder(&[("error", "catalog_query")]);
        let missing_filename_error_count =
            checker_error_count.recorder(&[("error", "missing_filename")]);
        let unexpected_file_count = checker_error_count.recorder(&[("error", "unexpected_file")]);
        let invalid_parquet_filename_count =
            checker_error_count.recorder(&[("error", "invalid_parquet_filename")]);

        Self {
            catalog_query_error_count,
            missing_filename_error_count,
            unexpected_file_count,
            invalid_parquet_filename_count,
        }
    }
}

pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    cutoff: Duration,
    bulk_ingest_cutoff: Duration,
    items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut repositories = catalog.repositories();
    let parquet_files = repositories.parquet_files();

    perform_inner(
        metric_registry,
        parquet_files,
        cutoff,
        bulk_ingest_cutoff,
        items,
        deleter,
    )
    .await
}

/// Allows easier mocking of just `ParquetFileRepo` in tests.
async fn perform_inner(
    metric_registry: Arc<metric::Registry>,
    parquet_files: &mut dyn ParquetFileRepo,
    cutoff: Duration,
    bulk_ingest_cutoff: Duration,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let checker_metrics = CheckerMetrics::new(metric_registry);
    let mut batch = Vec::with_capacity(CATALOG_BATCH_SIZE);
    loop {
        let maybe_item = timeout(RECEIVE_TIMEOUT, items.recv()).await;

        // if we have an error, we timed out.
        let timedout = maybe_item.is_err();
        if let Ok(res) = maybe_item {
            match res {
                Some(item) => {
                    batch.push(item);
                }
                None => {
                    // The channel has been closed unexpectedly
                    warn!("receiver channel closed unexpectedly, exiting");
                    return Err(Error::ChannelClosed);
                }
            }
        };

        if batch.len() >= CATALOG_BATCH_SIZE || timedout {
            let older_than = chrono::offset::Utc::now() - cutoff;
            let bulk_ingest_older_than = chrono::offset::Utc::now() - bulk_ingest_cutoff;
            for item in should_delete(
                &checker_metrics,
                batch,
                older_than,
                bulk_ingest_older_than,
                parquet_files,
            )
            .await
            {
                deleter.send(item).await.context(DeleterExitedSnafu)?;
            }
            batch = Vec::with_capacity(100);
        }
    }
}

/// [should_delete] processes a list of object store file information to see if the object for this
/// [ObjectMeta] can be deleted.
/// It can be deleted if it is old enough AND there isn't a reference in the catalog for it anymore (or ever)
/// It will also say the file can be deleted if it isn't a parquet file or the uuid isn't valid.
/// [should_delete] returns a subset of the input, which are the items that "should" be deleted.
// It first processes the easy checks, age, uuid, file suffix, and other parse/data input errors. This
// checking is cheap. For the files that need to be checked against the catalog, it batches them to
// reduce the number of requests on the wire and amortize the catalog overhead. Setting the batch size
// to 1 will return this method to its previous behavior (1 request per file) and resource usage.
async fn should_delete(
    checker_metrics: &CheckerMetrics,
    items: Vec<ObjectMeta>,
    normal_expires_at: DateTime<Utc>,
    bulk_ingest_expires_at: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> ImmediateDeletionList {
    // Inspect all the object store items, classifying them into "OK to
    // immediately delete" and "maybe delete" lists.
    let (mut to_delete, to_check_in_catalog) = items
        .into_iter()
        .fold(
            FileClassifier::new(normal_expires_at, bulk_ingest_expires_at, checker_metrics),
            |mut acc, file| {
                acc.check(file);
                acc
            },
        )
        .into_lists();

    let to_check_in_catalog = to_check_in_catalog.into_candidate_vec();

    // do_not_delete contains the items that are present in the catalog
    let mut do_not_delete: HashSet<ObjectStoreId> =
        HashSet::with_capacity(to_check_in_catalog.len());
    for batch in to_check_in_catalog.chunks(CATALOG_BATCH_SIZE) {
        let just_uuids: Vec<_> = batch.iter().map(|id| id.0).collect();
        match check_ids_exists_in_catalog(just_uuids.clone(), parquet_files).await {
            Ok(present_uuids) => {
                do_not_delete.extend(present_uuids.iter());
            }
            Err(e) => {
                // on error assume all the uuids in this batch are present in the catalog
                do_not_delete.extend(just_uuids.iter());
                warn!(
                    error = %e,
                    reason = "error querying catalog",
                    "Ignoring batch and continuing",
                );
                checker_metrics.catalog_query_error_count.inc(1);
            }
        }
    }

    if enabled!(Level::DEBUG) {
        do_not_delete.iter().for_each(|uuid| {
            debug!(
                deleting = false,
                uuid = %uuid,
                reason = "Object is present in catalog, not deleting",
                "Ignoring object",
            )
        });
    }

    // Loop over all the files that are conditional candidates for deletion, and
    // add them to the immediate deletion list if they are not in the "do not
    // delete" list from the catalog.
    to_check_in_catalog
        .iter()
        .filter(|c| !do_not_delete.contains(&c.0))
        .for_each(|c| to_delete.push(c.1.clone()));

    to_delete
}

/// helper to check a batch of ids for presence in the catalog.
/// returns a list of the ids (from the original batch) that exist (or catalog error).
async fn check_ids_exists_in_catalog(
    candidates: Vec<ObjectStoreId>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Result<Vec<ObjectStoreId>> {
    parquet_files
        .exists_by_object_store_id_batch(candidates)
        .await
        .context(FileExistsSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_catalog_and_file_with_max_time, create_schema_and_file_with_max_time,
    };
    use async_trait::async_trait;
    use data_types::{
        CompactionLevel, NamespaceId, ObjectStoreId, ParquetFile, ParquetFileId, ParquetFileParams,
        PartitionId, PartitionKey, TableId, Timestamp, TransitionPartitionId,
    };
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use iox_time::SystemProvider;
    use metric::{assert_counter, Attributes};
    use object_store::path::Path;
    use parquet_file::ParquetFilePath;
    use service_grpc_bulk_ingest::BulkIngestParquetFilePath;
    use std::{assert_eq, sync::LazyLock, vec};

    static OLDER_TIME: LazyLock<DateTime<Utc>> = LazyLock::new(|| {
        DateTime::parse_from_str("2022-01-01T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static NEWER_TIME: LazyLock<DateTime<Utc>> = LazyLock::new(|| {
        DateTime::parse_from_str("2022-02-02T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static BETWEEN_TIME: LazyLock<DateTime<Utc>> =
        LazyLock::new(|| *OLDER_TIME + (*NEWER_TIME - *OLDER_TIME) / 2);

    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let (catalog, file_in_catalog) =
            create_catalog_and_file_with_max_time(Timestamp::new(NEWER_TIME.timestamp_micros()))
                .await;
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.transition_partition_id(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_new_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            ObjectStoreId::new(),
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[ignore = "fails due to checker skipping location checks for new items"]
    #[tokio::test]
    async fn dont_delete_new_invalid_parquet_file() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *OLDER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location: Path::from("5bced7bd-b8f9-4000-b8e8-96bfce0d862b"),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_unexpected_file_counter(&metric_registry, 1);
    }

    #[tokio::test]
    async fn delete_old_invalid_parquet_file() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from("5bced7bd-b8f9-4000-b8e8-96bfce0d862b"),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 1);
        assert_unexpected_file_counter(&metric_registry, 1);
    }

    #[ignore = "fails due to checker skipping location checks for new items"]
    #[tokio::test]
    async fn dont_delete_new_file_with_missing_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *OLDER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location: Path::from(""),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_missing_filename_counter(&metric_registry, 1);
    }

    #[tokio::test]
    async fn delete_old_file_with_missing_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from(""),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_missing_filename_counter(&metric_registry, 1);
    }

    #[ignore = "fails due to checker skipping location checks for new items"]
    #[tokio::test]
    async fn dont_delete_new_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *OLDER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_invalid_parquet_filename_counter(&metric_registry, 1);
    }

    #[ignore = "fails due to checker skipping location checks for new items"]
    #[tokio::test]
    async fn delete_old_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 1);
        assert_invalid_parquet_filename_counter(&metric_registry, 1);
    }

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let (catalog, file_in_catalog) =
            create_catalog_and_file_with_max_time(Timestamp::new(OLDER_TIME.timestamp_micros()))
                .await;
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.transition_partition_id(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn delete_old_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            ObjectStoreId::new(),
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let results = should_delete(
            &checker_metrics,
            vec![item.clone()],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        let results = results.into_iter().collect::<Vec<_>>();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], item);
    }

    /// The garbage collector checks the catalog for files it _should not delete_. If we can't reach
    /// the catalog (some error), assume we are keeping all the files we are checking.
    /// [do_not_delete_on_catalog_error] tests that.
    #[tokio::test]
    async fn do_not_delete_on_catalog_error() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let (catalog, file_in_catalog) =
            create_schema_and_file_with_max_time(catalog, Timestamp::new(1)).await;

        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        // A ParquetFileRepo that returns an error in the one method [should_delete] uses.
        let mut mocked_parquet_files = MockParquetFileRepo {
            inner: parquet_files,
        };

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *OLDER_TIME;

        let loc = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.transition_partition_id(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let item = ObjectMeta {
            location: loc,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        // check precondition, file exists in catalog
        let pf = mocked_parquet_files
            .get_by_object_store_id(file_in_catalog.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pf, file_in_catalog);

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        // because of the db error, there should be no results
        let results = should_delete(
            &checker_metrics,
            vec![item.clone()],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            &mut mocked_parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_catalog_query_error_counter(&metric_registry, 1);
    }

    async fn bulk_ingest_test(
        cutoff: DateTime<Utc>,
        bulk_ingest_cutoff: DateTime<Utc>,
        last_modified: DateTime<Utc>,
    ) -> Vec<ObjectMeta> {
        let metric_registry = Arc::new(metric::Registry::new());
        let table_id = TableId::new(2);
        let location = BulkIngestParquetFilePath::new(
            NamespaceId::new(1),
            table_id,
            &TransitionPartitionId::new(table_id, &PartitionKey::from("2023-04-12")),
            ObjectStoreId::new(),
        )
        .object_store_path()
        .to_owned();

        // The catalog shouldn't be contacted for bulk ingest files, so use a mock that errors
        let mut mocked_parquet_files = ErrorParquetFileRepo {};

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        should_delete(
            &checker_metrics,
            vec![item],
            cutoff,
            bulk_ingest_cutoff,
            &mut mocked_parquet_files,
        )
        .await
        .into_iter()
        .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn dont_delete_bulk_ingest_file_newer_than_cutoff_and_newer_than_bulk_ingest_cutoff() {
        let cutoff = *OLDER_TIME;
        let bulk_ingest_cutoff = *BETWEEN_TIME;
        let last_modified = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_bulk_ingest_file_older_than_cutoff_and_newer_than_bulk_ingest_cutoff() {
        let bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *BETWEEN_TIME;
        let cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn delete_bulk_ingest_file_newer_than_cutoff_but_older_than_bulk_ingest_cutoff() {
        let cutoff = *OLDER_TIME;
        let last_modified = *BETWEEN_TIME;
        let bulk_ingest_cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.into_iter().len(), 1);
    }

    #[tokio::test]
    async fn delete_bulk_ingest_file_older_than_cutoff_and_older_than_bulk_ingest_cutoff() {
        let last_modified = *OLDER_TIME;
        let cutoff = *BETWEEN_TIME;
        let bulk_ingest_cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.into_iter().len(), 1);
    }

    struct MockParquetFileRepo<'a> {
        inner: &'a mut dyn ParquetFileRepo,
    }

    #[async_trait]
    impl ParquetFileRepo for MockParquetFileRepo<'_> {
        async fn flag_for_delete_by_retention(
            &mut self,
        ) -> iox_catalog::interface::Result<Vec<(PartitionId, ObjectStoreId)>> {
            self.inner.flag_for_delete_by_retention().await
        }

        async fn delete_old_ids_only(
            &mut self,
            older_than: Timestamp,
            cutoff: std::time::Duration,
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            self.inner.delete_old_ids_only(older_than, cutoff).await
        }

        async fn list_by_partition_not_to_delete_batch(
            &mut self,
            partition_ids: Vec<PartitionId>,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner
                .list_by_partition_not_to_delete_batch(partition_ids)
                .await
        }

        async fn active_as_of(
            &mut self,
            as_of: Timestamp,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner.active_as_of(as_of).await
        }

        async fn get_by_object_store_id(
            &mut self,
            object_store_id: ObjectStoreId,
        ) -> iox_catalog::interface::Result<Option<ParquetFile>> {
            self.inner.get_by_object_store_id(object_store_id).await
        }

        async fn exists_by_object_store_id_batch(
            &mut self,
            _object_store_ids: Vec<ObjectStoreId>,
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            Err(iox_catalog::interface::Error::External {
                source: Box::<dyn std::error::Error + Send + Sync>::from("test".to_owned()).into(),
            })
        }

        async fn create_upgrade_delete(
            &mut self,
            partition_id: PartitionId,
            delete: &[ObjectStoreId],
            upgrade: &[ObjectStoreId],
            create: &[ParquetFileParams],
            target_level: CompactionLevel,
        ) -> iox_catalog::interface::Result<Vec<ParquetFileId>> {
            self.create_upgrade_delete(partition_id, delete, upgrade, create, target_level)
                .await
        }
    }

    struct ErrorParquetFileRepo {}

    #[async_trait]
    impl ParquetFileRepo for ErrorParquetFileRepo {
        async fn flag_for_delete_by_retention(
            &mut self,
        ) -> iox_catalog::interface::Result<Vec<(PartitionId, ObjectStoreId)>> {
            unimplemented!()
        }

        async fn delete_old_ids_only(
            &mut self,
            _older_than: Timestamp,
            _cutoff: std::time::Duration,
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            unimplemented!()
        }

        async fn list_by_partition_not_to_delete_batch(
            &mut self,
            _partition_ids: Vec<PartitionId>,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            unimplemented!()
        }

        async fn active_as_of(
            &mut self,
            _as_of: Timestamp,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            unimplemented!()
        }

        async fn get_by_object_store_id(
            &mut self,
            _object_store_id: ObjectStoreId,
        ) -> iox_catalog::interface::Result<Option<ParquetFile>> {
            unimplemented!()
        }

        async fn exists_by_object_store_id_batch(
            &mut self,
            _object_store_ids: Vec<ObjectStoreId>,
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            unimplemented!()
        }

        async fn create_upgrade_delete(
            &mut self,
            _partition_id: PartitionId,
            _delete: &[ObjectStoreId],
            _upgrade: &[ObjectStoreId],
            _create: &[ParquetFileParams],
            _target_level: CompactionLevel,
        ) -> iox_catalog::interface::Result<Vec<ParquetFileId>> {
            unimplemented!()
        }
    }

    fn assert_catalog_query_error_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_checker_error",
            labels = Attributes::from(&[("error", "catalog_query")]),
            value = value,
        );
    }

    fn assert_missing_filename_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_checker_error",
            labels = Attributes::from(&[("error", "missing_filename")]),
            value = value,
        );
    }

    fn assert_invalid_parquet_filename_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_checker_error",
            labels = Attributes::from(&[("error", "invalid_parquet_filename")]),
            value = value,
        );
    }

    fn assert_unexpected_file_counter(metric_registry: &metric::Registry, value: u64) {
        assert_counter!(
            metric_registry,
            U64Counter,
            "gc_checker_error",
            labels = Attributes::from(&[("error", "unexpected_file")]),
            value = value,
        );
    }
}
