use chrono::{DateTime, Duration, Utc};
use data_types::ObjectStoreId;
use iox_catalog::interface::{Catalog, ParquetFileRepo};
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

pub(crate) async fn perform(
    catalog: Arc<dyn Catalog>,
    cutoff: Duration,
    bulk_ingest_cutoff: Duration,
    items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut repositories = catalog.repositories();
    let parquet_files = repositories.parquet_files();

    perform_inner(parquet_files, cutoff, bulk_ingest_cutoff, items, deleter).await
}

/// Allows easier mocking of just `ParquetFileRepo` in tests.
async fn perform_inner(
    parquet_files: &mut dyn ParquetFileRepo,
    cutoff: Duration,
    bulk_ingest_cutoff: Duration,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
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
                    return Err(Error::ChannelClosed);
                }
            }
        };

        if batch.len() >= CATALOG_BATCH_SIZE || timedout {
            let older_than = chrono::offset::Utc::now() - cutoff;
            let bulk_ingest_older_than = chrono::offset::Utc::now() - bulk_ingest_cutoff;
            for item in
                should_delete(batch, older_than, bulk_ingest_older_than, parquet_files).await
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
    items: Vec<ObjectMeta>,
    cutoff: DateTime<Utc>,
    bulk_ingest_cutoff: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Vec<ObjectMeta> {
    // to_delete is the vector we will return to the caller containing ObjectMeta we think should be deleted.
    // it is never longer than `items`
    let mut to_delete = Vec::with_capacity(items.len());
    // After filtering out potential errors and non-parquet files, this vector accumulates the objects
    // that need to be checked against the catalog to see if we can delete them.
    let mut to_check_in_catalog = Vec::with_capacity(items.len());

    for candidate in items {
        if is_bulk_ingest_file(&candidate.location) {
            if bulk_ingest_cutoff < candidate.last_modified {
                debug!(
                    location = %candidate.location,
                    deleting = false,
                    reason = "bulk ingest file too new",
                    %bulk_ingest_cutoff,
                    last_modified = %candidate.last_modified,
                    "Ignoring object",
                );
                // Not old enough; do not delete
                continue;
            } else {
                // Bulk ingest file is old enough. It won't be in the catalog, so don't check that,
                // add it to the delete list.
                to_delete.push(candidate);
                continue;
            }
        } else if cutoff < candidate.last_modified {
            // expected to be a common reason to skip a file
            debug!(
                location = %candidate.location,
                deleting = false,
                reason = "too new",
                cutoff = %cutoff,
                last_modified = %candidate.last_modified,
                "Ignoring object",
            );
            // Not old enough; do not delete
            continue;
        }

        let file_name = candidate.location.parts().last();
        if file_name.is_none() {
            warn!(
                location = %candidate.location,
                deleting = true,
                reason = "bad location",
                "Ignoring object",
            );
            // missing file name entirely! likely not a valid object store file entry
            // skip it
            continue;
        }

        // extract the file suffix, delete it if it isn't a parquet file
        if let Some(uuid) = file_name.unwrap().as_ref().strip_suffix(".parquet") {
            if let Ok(object_store_id) = uuid.parse::<ObjectStoreId>() {
                // add it to the list to check against the catalog
                // push a tuple that maps the uuid to the object meta struct so we don't have generate the uuid again
                to_check_in_catalog.push((object_store_id, candidate))
            } else {
                // expected to be a rare situation so warn.
                warn!(
                    location = %candidate.location,
                    deleting = true,
                    uuid,
                    reason = "not a valid UUID",
                    "Scheduling file for deletion",
                );
                to_delete.push(candidate)
            }
        } else {
            // expected to be a rare situation so warn.
            warn!(
                location = %candidate.location,
                deleting = true,
                reason = "not a .parquet file",
                "Scheduling file for deletion",
            );
            to_delete.push(candidate)
        }
    }

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

    // we have a Vec of uuids for the files we _do not_ want to delete (present in the catalog)
    // remove these uuids from the Vec of all uuids we checked, adding the remainder to the delete list
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

/// Check if a file's location matches the naming of bulk ingested files and thus the bulk ingest
/// cutoff should apply rather than the regular cutoff.
fn is_bulk_ingest_file(location: &object_store::path::Path) -> bool {
    let mut parts: Vec<_> = location.parts().collect();
    let filename = parts.pop();
    let last_directory = parts.pop();

    filename
        .map(|f| f.as_ref().strip_suffix(".parquet").is_some())
        .unwrap_or(false)
        && last_directory
            .map(|dir| dir.as_ref() == "bulk_ingest")
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, NamespaceId, ObjectStoreId, ParquetFile,
        ParquetFileId, ParquetFileParams, PartitionId, PartitionKey, TableId, Timestamp,
        TransitionPartitionId,
    };
    use iox_catalog::{
        interface::{Catalog, ParquetFileRepoExt},
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };
    use iox_time::SystemProvider;
    use object_store::path::Path;
    use once_cell::sync::Lazy;
    use parquet_file::ParquetFilePath;
    use service_grpc_bulk_ingest::BulkIngestParquetFilePath;
    use std::{assert_eq, vec};

    static OLDER_TIME: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_str("2022-01-01T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static NEWER_TIME: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_str("2022-02-02T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static BETWEEN_TIME: Lazy<DateTime<Utc>> =
        Lazy::new(|| *OLDER_TIME + (*NEWER_TIME - *OLDER_TIME) / 2);

    async fn create_catalog_and_file() -> (Arc<dyn Catalog>, ParquetFile) {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        create_schema_and_file(catalog).await
    }

    async fn create_schema_and_file(catalog: Arc<dyn Catalog>) -> (Arc<dyn Catalog>, ParquetFile) {
        let mut repos = catalog.repositories();
        let namespace = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
        let partition = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            partition_hash_id: partition.hash_id().cloned(),
            object_store_id: ObjectStoreId::new(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
            source: None,
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        (catalog, parquet_file)
    }

    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let (catalog, file_in_catalog) = create_catalog_and_file().await;
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

        let results = should_delete(
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.len(), 0);
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

        let results = should_delete(
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.len(), 0);
    }

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

        let results = should_delete(
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {
        let (catalog, file_in_catalog) = create_catalog_and_file().await;
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

        let results = should_delete(
            vec![item],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.len(), 0);
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
        let results = should_delete(
            vec![item.clone()],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], item);
    }

    #[tokio::test]
    async fn delete_old_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider = Arc::new(SystemProvider::new());
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
        let mut repositories = catalog.repositories();
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let arbitrary_bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        };

        let results = should_delete(
            vec![item.clone()],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            parquet_files,
        )
        .await;
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
        let (catalog, file_in_catalog) = create_schema_and_file(catalog).await;

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

        // because of the db error, there should be no results
        let results = should_delete(
            vec![item.clone()],
            cutoff,
            arbitrary_bulk_ingest_cutoff,
            &mut mocked_parquet_files,
        )
        .await;
        assert_eq!(results.len(), 0);
    }

    async fn bulk_ingest_test(
        cutoff: DateTime<Utc>,
        bulk_ingest_cutoff: DateTime<Utc>,
        last_modified: DateTime<Utc>,
    ) -> Vec<ObjectMeta> {
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

        should_delete(
            vec![item],
            cutoff,
            bulk_ingest_cutoff,
            &mut mocked_parquet_files,
        )
        .await
    }

    #[tokio::test]
    async fn dont_delete_bulk_ingest_file_newer_than_cutoff_and_newer_than_bulk_ingest_cutoff() {
        let cutoff = *OLDER_TIME;
        let bulk_ingest_cutoff = *BETWEEN_TIME;
        let last_modified = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_bulk_ingest_file_older_than_cutoff_and_newer_than_bulk_ingest_cutoff() {
        let bulk_ingest_cutoff = *OLDER_TIME;
        let last_modified = *BETWEEN_TIME;
        let cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn delete_bulk_ingest_file_newer_than_cutoff_but_older_than_bulk_ingest_cutoff() {
        let cutoff = *OLDER_TIME;
        let last_modified = *BETWEEN_TIME;
        let bulk_ingest_cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn delete_bulk_ingest_file_older_than_cutoff_and_older_than_bulk_ingest_cutoff() {
        let last_modified = *OLDER_TIME;
        let cutoff = *BETWEEN_TIME;
        let bulk_ingest_cutoff = *NEWER_TIME;

        let results = bulk_ingest_test(cutoff, bulk_ingest_cutoff, last_modified).await;
        assert_eq!(results.len(), 1);
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
        ) -> iox_catalog::interface::Result<Vec<ObjectStoreId>> {
            unimplemented!()
        }

        async fn list_by_partition_not_to_delete_batch(
            &mut self,
            _partition_ids: Vec<PartitionId>,
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

    #[test]
    fn test_is_bulk_ingest_file() {
        use object_store::path::Path;

        let table_id = TableId::new(2);
        let bulk_ingest_file_path = BulkIngestParquetFilePath::new(
            NamespaceId::new(1),
            table_id,
            &TransitionPartitionId::new(table_id, &PartitionKey::from("2024-04-16")),
            ObjectStoreId::new(),
        );
        assert!(is_bulk_ingest_file(
            bulk_ingest_file_path.object_store_path()
        ));

        let not_parquet = Path::from("/bulk_ingest/not-parquet.txt");
        assert!(!is_bulk_ingest_file(&not_parquet));

        let regular_ingest_file_path = ParquetFilePath::new(
            NamespaceId::new(1),
            table_id,
            &TransitionPartitionId::new(table_id, &PartitionKey::from("2024-04-16")),
            ObjectStoreId::new(),
        );
        assert!(!is_bulk_ingest_file(
            &regular_ingest_file_path.object_store_path()
        ));

        let too_short = Path::from("hi");
        assert!(!is_bulk_ingest_file(&too_short));

        let too_early = Path::from("/bulk_ingest/another_directory/filename.parquet");
        assert!(!is_bulk_ingest_file(&too_early));

        let filename_contains_bulk_ingest = Path::from("/hi/bye/bulk_ingest.parquet");
        assert!(!is_bulk_ingest_file(&filename_contains_bulk_ingest));

        let not_exact_directory_match = Path::from("/hi/bye/aaabulk_ingestaaa/1.parquet");
        assert!(!is_bulk_ingest_file(&not_exact_directory_match));
    }
}
