mod file_classifier;
use file_classifier::FileClassifier;
mod lists;
use lists::DeletionCandidateList;

use std::{collections::HashSet, sync::Arc, time::Duration};

use clap_blocks::garbage_collector::CutoffDuration;
use data_types::ObjectStoreId;
use iox_catalog::interface::{Catalog, ParquetFileRepo};
use iox_time::AsyncTimeProvider;
use metric::U64Counter;
use object_store::ObjectMeta;
use observability_deps::tracing::*;
use snafu::prelude::*;
use tokio::{sync::mpsc, time::timeout};

use crate::PathsInCatalogBackups;

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

    // Track how frequently we encounter errors when getting the current time from the catalog
    catalog_time_error_count: U64Counter,
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
        let catalog_time_error_count = checker_error_count.recorder(&[("error", "catalog_time")]);

        Self {
            catalog_query_error_count,
            missing_filename_error_count,
            unexpected_file_count,
            invalid_parquet_filename_count,
            catalog_time_error_count,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn perform(
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    time_provider: Arc<impl AsyncTimeProvider>,
    cutoff: CutoffDuration,
    bulk_ingest_cutoff: CutoffDuration,
    catalog_backup_hourly_cutoff: CutoffDuration,
    catalog_backup_daily_cutoff: CutoffDuration,
    paths_in_catalog_backups: Arc<PathsInCatalogBackups>,
    items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut repositories = catalog.repositories();
    let parquet_files = repositories.parquet_files();

    perform_inner(
        metric_registry,
        parquet_files,
        time_provider,
        cutoff,
        bulk_ingest_cutoff,
        catalog_backup_hourly_cutoff,
        catalog_backup_daily_cutoff,
        paths_in_catalog_backups,
        items,
        deleter,
    )
    .await
}

/// Allows easier mocking of just `ParquetFileRepo` in tests.
#[allow(clippy::too_many_arguments)]
async fn perform_inner(
    metric_registry: Arc<metric::Registry>,
    parquet_files: &mut dyn ParquetFileRepo,
    time_provider: Arc<impl AsyncTimeProvider>,
    cutoff: CutoffDuration,
    bulk_ingest_cutoff: CutoffDuration,
    catalog_backup_hourly_keep_cutoff: CutoffDuration,
    catalog_backup_daily_keep_cutoff: CutoffDuration,
    paths_in_catalog_backups: Arc<PathsInCatalogBackups>,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let metrics = CheckerMetrics::new(metric_registry);
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
                    // The channel has closed unexpectedly. We don't
                    // need to warn here, because from the checker's
                    // perspective there is simply no work left to be done.
                    info!("item queue closed, shutting down object store checker");
                    return Ok(());
                }
            }
        };

        if batch.len() >= CATALOG_BATCH_SIZE || timedout {
            match time_provider.now().await {
                Ok(now) => {
                    trace!("received catalog time: {now}");
                    let normal_expires_at = now - cutoff.duration();
                    let bulk_ingest_expires_at = now - bulk_ingest_cutoff.duration();
                    // backups expire at the configured keep + 1.  If the keep is 1 day, then the backups expire at
                    // 2 days, maintaining an oldest backup between 1 and 2 days, thus ensuring we always satisfy the
                    // 1 day keep.
                    let catalog_backup_hourly_expires_at = now
                        - catalog_backup_hourly_keep_cutoff
                            .duration()
                            .checked_add(Duration::from_secs(60 * 60))
                            .unwrap();
                    let catalog_backup_daily_expires_at = now
                        - catalog_backup_daily_keep_cutoff
                            .duration()
                            .checked_add(Duration::from_secs(60 * 60 * 24))
                            .unwrap();

                    let file_classifier = FileClassifier::new(
                        normal_expires_at,
                        bulk_ingest_expires_at,
                        catalog_backup_hourly_expires_at,
                        catalog_backup_daily_expires_at,
                        &paths_in_catalog_backups,
                        &metrics,
                    );

                    // Inspect all the object store items, classifying them into "OK to immediately
                    // delete" and "maybe delete" lists.
                    let (to_delete, to_check_in_catalog) = batch
                        .into_iter()
                        .fold(file_classifier, |mut acc, file| {
                            acc.check(file);
                            acc
                        })
                        .into_lists();
                    // For the "maybe delete" items, check if they're in the catalog and only
                    // delete those that aren't in the catalog.
                    let to_delete_not_in_catalog =
                        should_delete(&metrics, to_check_in_catalog, parquet_files).await;

                    // Send both the "OK to immediately delete" items and the "OK to delete because
                    // they're not in the catalog" items to the deleter.
                    for item in to_delete
                        .into_iter()
                        .chain(to_delete_not_in_catalog.into_iter())
                    {
                        deleter.send(item).await.context(DeleterExitedSnafu)?;
                    }
                    batch = Vec::with_capacity(100);
                }
                Err(e) => {
                    // If we failed to get the current time from the catalog
                    // record the error and try again next time.
                    metrics.catalog_time_error_count.inc(1);
                    debug!("error getting current time from catalog: {e}");
                }
            };
        }
    }
}

/// Batch deletion candidates and query the catalog for presence. Filter out any deletion
/// candidates that are present in the catalog so that they are not deleted.
async fn should_delete(
    checker_metrics: &CheckerMetrics,
    to_check_in_catalog: DeletionCandidateList,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Vec<ObjectMeta> {
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

    // Loop over all the files that are conditional candidates for deletion, and return them for
    // deletion if they are not in the "do not delete" list from the catalog.
    to_check_in_catalog
        .into_iter()
        .filter_map(|(object_store_id, object_meta)| {
            (!do_not_delete.contains(&object_store_id)).then_some(object_meta)
        })
        .collect()
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
    use crate::test_utils::create_schema_and_file_with_max_time;
    use async_trait::async_trait;
    use data_types::{
        CompactionLevel, NamespaceId, ObjectStoreId, ParquetFile, ParquetFileId, ParquetFileParams,
        PartitionId, TableId, Timestamp, TransitionPartitionId,
    };
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use iox_time::{SystemProvider, Time};
    use metric::{assert_counter, Attributes};
    use object_store::path::Path;
    use parquet_file::ParquetFilePath;
    use std::sync::LazyLock;

    static OLDER_TIME: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-01-01T00:00:00z").unwrap());
    static NEWER_TIME: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-02-02T00:00:00z").unwrap());

    fn catalog() -> Arc<dyn Catalog> {
        // None of these tests verify the catalog's metrics
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        Arc::new(MemCatalog::new(metric_registry, time_provider))
    }

    // Set up a scenario for an object store file with the given location in a
    // `DeletionCandidateList` to be checked with the catalog.
    async fn regular_file_test(
        catalog: Arc<dyn Catalog>,
        metric_registry: Arc<metric::Registry>,
        location: Path,
        object_store_id: ObjectStoreId,
        last_modified: Time,
    ) -> Vec<ObjectMeta> {
        let item = ObjectMeta {
            location,
            last_modified: last_modified.date_time(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let checker_metrics = CheckerMetrics::new(Arc::clone(&metric_registry));
        let mut deletion_candidate_list = DeletionCandidateList::default();
        deletion_candidate_list.push(object_store_id, item);

        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        should_delete(&checker_metrics, deletion_candidate_list, parquet_files).await
    }

    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let catalog = catalog();
        let metric_registry = Arc::new(metric::Registry::new());

        let file_in_catalog =
            create_schema_and_file_with_max_time(Arc::clone(&catalog), (*NEWER_TIME).into()).await;
        let location = ParquetFilePath::from(&file_in_catalog).object_store_path();
        let last_modified = *NEWER_TIME;

        let results = regular_file_test(
            catalog,
            Arc::clone(&metric_registry),
            location,
            file_in_catalog.object_store_id,
            last_modified,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {
        let catalog = catalog();
        let metric_registry = Arc::new(metric::Registry::new());

        let file_in_catalog =
            create_schema_and_file_with_max_time(Arc::clone(&catalog), (*OLDER_TIME).into()).await;
        let location = ParquetFilePath::from(&file_in_catalog).object_store_path();

        let last_modified = *OLDER_TIME;

        let results = regular_file_test(
            catalog,
            Arc::clone(&metric_registry),
            location,
            file_in_catalog.object_store_id,
            last_modified,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
    }

    #[tokio::test]
    async fn delete_old_file_not_in_catalog() {
        let catalog = catalog();
        let metric_registry = Arc::new(metric::Registry::new());

        let object_store_id = ObjectStoreId::new();
        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Catalog(PartitionId::new(4)),
            object_store_id,
        )
        .object_store_path();

        let last_modified = *OLDER_TIME;

        let results = regular_file_test(
            catalog,
            Arc::clone(&metric_registry),
            location.clone(),
            object_store_id,
            last_modified,
        )
        .await;
        let results = results.into_iter().collect::<Vec<_>>();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].location, location);
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
        let file_in_catalog =
            create_schema_and_file_with_max_time(Arc::clone(&catalog), Timestamp::new(1)).await;

        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        // A ParquetFileRepo that returns an error in the one method [should_delete] uses.
        let mut mocked_parquet_files = MockParquetFileRepo {
            inner: parquet_files,
        };

        let last_modified = OLDER_TIME.date_time();

        let loc = ParquetFilePath::from(&file_in_catalog).object_store_path();

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
        let mut deletion_candidate_list = DeletionCandidateList::default();
        deletion_candidate_list.push(file_in_catalog.object_store_id, item);

        // because of the db error, there should be no results
        let results = should_delete(
            &checker_metrics,
            deletion_candidate_list,
            &mut mocked_parquet_files,
        )
        .await;
        assert_eq!(results.into_iter().len(), 0);
        assert_catalog_query_error_counter(&metric_registry, 1);
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
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            self.inner.delete_old_ids_only(older_than).await
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

        async fn list_by_table_id(
            &mut self,
            table_id: TableId,
            compaction_level: Option<CompactionLevel>,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.list_by_table_id(table_id, compaction_level).await
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
}
