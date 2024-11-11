//! # Tests of the garbage collector's actions taken on all possible Parquet file states
//!
//! This test's goal is to ensure the garbage collector as a whole is taking the action we expect
//! it to on any valid Parquet file situation it encounters. The test sets up Parquet files in
//! object store and in the catalog in various states relative to the garbage collector's
//! configuration, lets the garbage collector run "once" (there are multiple independent threads
//! the garbage collector runs, but the intent is to let all of them have a chance to run in this
//! test), then check the outcomes are what we want to happen.
//!
//! ## Possible Valid States
//!
//! This test sets up the following cases and asserts the garbage collector takes the associated
//! actions (or inactions):
//!
//! | In catalog? | Soft-deleted? | In a snapshot? | Expired? | In object store? | GC Action                |
//! |-------------|---------------|----------------|----------|------------------|--------------------------|
//! | ✅          |               |                |          | ✅               | Add to snapshot          |
//! | ✅          |               | 📸             |          | ✅               | Nothing                  |
//! | ✅          | 🚮            |                |          | ✅               | Nothing                  |
//! | ✅          | 🚮            | 📸             |          | ✅               | Nothing                  |
//! | ✅          |               |                | ☠️       | ✅               | Soft-delete - retention; add to snapshot  |
//! | ✅          |               | 📸             | ☠️       | ✅               | Soft-delete - retention  |
//! | ✅          | 🚮            |                | ☠️       | ✅               | Delete catalog record    |
//! | ✅          | 🚮            | 📸             | ☠️       | ✅               | Delete catalog record    |
//! | ❌          | n/a           |                |          | ✅               | Nothing                  |
//! | ❌          | n/a           | 📸             |          | ✅               | Nothing                  |
//! | ❌          | n/a           |                | ☠️       | ✅               | Delete object store file |
//! | ❌          | n/a           | 📸             | ☠️       | ✅               | Nothing                  |
//!
//! Definitions:
//!
//! * *In catalog*: If true (✅), a record exists for this Parquet file in the `parquet_file` table.
//! * *Soft-deleted*: If true (🚮), the record for this Parquet file in the catalog has a
//!   `to_delete` value in the past.
//! * *In a snapshot*: If true (📸), this Parquet file's object store path appears in a catalog
//!   backup data snapshot bloom filter file.
//! * *Expired*: If true (☠️),
//!   * If NOT soft deleted, the record for this Parquet file in the catalog has a `max_time` value
//!     older than its namespace's retention period ago.
//!   * If it IS soft deleted (🚮), the record for this Parquet file in the catalog has a
//!     `to_delete` value older than the GC's `INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF` ago.
//!   * If there's no record in the catalog but there is a file in object storage, the file's
//!     `last_modified` metadata value is older than the GC's `INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF`
//!     ago.
//! * *In object store*: A Parquet file exists at the object store path:
//!   `{namespace ID}/{table ID}/{partition ID}/{object store ID}.parquet`
//!
//! ## Logically impossible states
//!
//! These states do not appear in the above truth table because they don't make sense:
//!
//! Not in the catalog and either soft-deleted or not:
//!
//! | In catalog? | Soft-deleted? |
//! |-------------|---------------|
//! | ❌          |               |
//! | ❌          | 🚮            |
//!
//! Soft deleted is one of the columns _in the catalog record_, so if there is no catalog record,
//! then soft-deleted is not applicable.
//!
//! Nonexistent records:
//!
//! | In catalog? | Soft-deleted? | In a snapshot? | Expired? | In object store? |
//! |-------------|---------------|----------------|----------|------------------|
//! | ❌          | n/a           |                | ☠️       | ❌               |
//! | ❌          | n/a           |                |          | ❌               |
//!
//! These states are impossible to encounter in the code because there are no records that would be
//! returned from the catalog, the object store, or the catalog file list snapshots.
//!
//! ## Enforced impossible states
//!
//! These states do not appear in the above truth table because there are enforcements in the code
//! ensuring they don't happen:
//!
//! In the catalog but not in object storage:
//!
//! | In catalog? |  In object store? |
//! |-------------|-------------------|
//! | ✅          | ❌                |
//!
//! This state MUST NOT happen; this is enforced by the GC checking the catalog and not deleting
//! anything present.
//!
//! In a catalog backup data snapshot file, but not in the catalog or object storage:
//!
//! | In catalog? | In a snapshot? | In object store? |
//! |-------------|----------------|------------------|
//! | ❌          | 📸             | ❌               |
//!
//! This state MUST NOT happen; this is enforced by the GC checking the set of all snapshots and
//! not deleting anything present.

// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![allow(unused_crate_dependencies)]

use bytes::Bytes;
use catalog_backup_file_list::{BloomFilter, CatalogBackupDirectory, DesiredFileListTime};
use chrono::Utc;
use clap_blocks::garbage_collector::{CutoffDuration, GarbageCollectorConfig};
use data_types::{CompactionLevel, Namespace, ParquetFileParams, Partition, Table, Timestamp};
use futures::{stream::BoxStream, TryStreamExt};
use garbage_collector::{Config, GarbageCollector};
use iox_catalog::{
    interface::{Catalog, ParquetFileRepoExt},
    mem::MemCatalog,
    test_helpers::*,
};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::{DynObjectStore, ObjectStore};
use parquet_file::ParquetFilePath;
use std::{
    collections::HashMap,
    fmt,
    ops::Range,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};
use test_helpers::maybe_start_logging;

// In catalog -----------------------------------------------------------------------------

#[tokio::test]
async fn active_gets_snapshotted() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: false,
        in_a_snapshot: false,
        expired: false,
        // After GC runs, this file should be in a snapshot and otherwise unchanged.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: false,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn active_in_snapshot_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: false,
        in_a_snapshot: true,
        expired: false,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: false,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn recently_soft_deleted_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: true,
        in_a_snapshot: false,
        expired: false,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: true,
            in_a_snapshot: false,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn recently_soft_deleted_and_snapshotted_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: true,
        in_a_snapshot: true,
        expired: false,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: true,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn expired_gets_soft_deleted() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: false,
        in_a_snapshot: false,
        expired: true,
        // GC should mark this file as soft-deleted due to retention as it is expired.
        // It will also get included in the previous hour's snapshot because the soft-deletion
        // happened after the hour (the "current time" that the garbage collector is running)
        // and the snapshot looks for files active as of on the hour exactly.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: true,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn expired_in_snapshot_gets_soft_deleted() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: false,
        in_a_snapshot: true,
        expired: true,
        // GC should mark this file as soft-deleted due to retention as it is expired.
        end_state: EndState {
            in_catalog: true,
            soft_deleted: true,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn soft_deleted_expired_gets_deleted_from_catalog() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: true,
        in_a_snapshot: false,
        expired: true,
        // GC should delete this catalog record.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: false,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn soft_deleted_expired_snapshotted_gets_deleted_from_catalog() {
    run_garbage_collector_situation(Situation {
        in_catalog: true,
        soft_deleted: true,
        in_a_snapshot: true,
        expired: true,
        // GC should delete this catalog record.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

// Not in catalog (and thus not soft-deleted) ----------------------------------------

#[tokio::test]
async fn recent_only_in_object_store_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: false,
        soft_deleted: false,
        in_a_snapshot: false,
        expired: false,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: false,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn recent_only_in_object_store_and_snapshotted_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: false,
        soft_deleted: false,
        in_a_snapshot: true,
        expired: false,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

#[tokio::test]
async fn old_only_in_object_store_gets_deleted() {
    run_garbage_collector_situation(Situation {
        in_catalog: false,
        soft_deleted: false,
        in_a_snapshot: false,
        expired: true,
        // GC should delete the object store file.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: false,
            in_object_storage: false,
        },
    })
    .await;
}

#[tokio::test]
async fn old_in_object_store_and_snapshotted_unchanged() {
    run_garbage_collector_situation(Situation {
        in_catalog: false,
        soft_deleted: false,
        in_a_snapshot: true,
        expired: true,
        // GC should not change this file.
        end_state: EndState {
            in_catalog: false,
            soft_deleted: false,
            in_a_snapshot: true,
            in_object_storage: true,
        },
    })
    .await;
}

const ONE_HOUR: Duration = Duration::from_secs(60 * 60);
static ONE_DAY: LazyLock<CutoffDuration> = LazyLock::new(|| CutoffDuration::try_new("1d").unwrap());
static FOURTEEN_DAYS: LazyLock<CutoffDuration> =
    LazyLock::new(|| CutoffDuration::try_new("14d").unwrap());
// see https://github.com/influxdata/influxdb_iox/issues/11420 if you're mad that some fields take
// a duration and some take ints
const THIRTY: u64 = 30;

/// Test function that:
///
/// - Sets up a particular situation
/// - Runs the garbage collector
/// - Shuts down the garbage collector
/// - Verifies the end state of the situation
async fn run_garbage_collector_situation(situation: Situation) {
    maybe_start_logging();
    let metric_registry = Arc::new(metric::Registry::new());
    // Mock the time for setup
    let mock_time_provider = Arc::new(MockProvider::new(
        Time::from_rfc3339("2024-08-12T13:04:58+00:00").unwrap(),
    ));

    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
        Arc::clone(&metric_registry),
        Arc::clone(&mock_time_provider) as _,
    ));
    let mut repos = catalog.repositories();
    // Set the namespace's retention to 7 days
    let namespace_retention_ns = 7 * 24 * 60 * 60 * 1_000_000_000;
    let namespace = arbitrary_namespace_with_retention_policy(
        &mut *repos,
        "test_namespace",
        namespace_retention_ns,
    )
    .await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
    let partition = repos
        .partitions()
        .create_or_get("one".into(), table.id)
        .await
        .unwrap();

    let object_store = Arc::new(MockTimeObjectStore::new());

    let sub_config = GarbageCollectorConfig {
        objectstore_cutoff: *FOURTEEN_DAYS,
        bulk_ingest_objectstore_cutoff: *ONE_DAY,
        objectstore_sleep_interval_minutes: THIRTY,
        objectstore_sleep_interval_batch_milliseconds: 100,
        parquetfile_cutoff: *FOURTEEN_DAYS,
        parquetfile_sleep_interval_minutes: Some(THIRTY),
        parquetfile_sleep_interval: None,
        retention_sleep_interval_minutes: THIRTY + 5,
        replica_dsn: None,
        enable_partition_row_deletion: true,
        enable_object_store_deletion: true,
        enable_parquet_file_deletion: true,
        create_catalog_backup_data_snapshot_files: true,
        delete_using_catalog_backup_data_snapshot_files: true,
        keep_hourly_catalog_backup_file_lists: *ONE_DAY,
        keep_daily_catalog_backup_file_lists: *FOURTEEN_DAYS,
        enable_pg_dump: false,
    };

    let config = Config {
        object_store: Arc::clone(&object_store) as _,
        catalog: Arc::clone(&catalog),
        replica_catalog: None,
        sub_config,
        dump_dsn: None,
    };

    let built_situation = situation
        .build(
            &mock_time_provider,
            &catalog,
            &object_store,
            &config.sub_config,
            &namespace,
            &table,
            &partition,
        )
        .await;

    let garbage_collector = GarbageCollector::start(config, metric_registry)
        .await
        .unwrap();

    // Sleep to give the garbage collector tasks a chance to run
    tokio::time::sleep(Duration::from_secs(3)).await;

    let shutdown_garbage_collector = garbage_collector.shutdown_handle();
    shutdown_garbage_collector();

    built_situation
        .verify_end_state(&catalog, &garbage_collector, &object_store)
        .await;
}

/// Describes the attributes of a particular situation of a Parquet file the garbage collector
/// might encounter.
#[derive(Debug)]
struct Situation {
    in_catalog: bool,
    soft_deleted: bool,
    in_a_snapshot: bool,
    expired: bool,
    end_state: EndState,
}

impl Situation {
    /// Setting up this situation.
    #[allow(clippy::too_many_arguments)]
    async fn build(
        self,
        mock_time_provider: &Arc<MockProvider>,
        catalog: &Arc<dyn Catalog>,
        object_store: &Arc<MockTimeObjectStore>,
        garbage_collector_config: &GarbageCollectorConfig,
        namespace: &Namespace,
        table: &Table,
        partition: &Partition,
    ) -> BuiltSituation {
        let max_time = if self.expired {
            catalog.time_provider().now().timestamp_nanos()
                - namespace
                    .retention_period_ns
                    .expect("this test should set namespace retention")
                - 1_000_000
        } else {
            catalog.time_provider().now().timestamp_nanos()
        };
        let parquet_file_params = ParquetFileParams {
            max_time: Timestamp::new(max_time),
            ..arbitrary_parquet_file_params(namespace, table, partition)
        };

        // All test cases have an associated file in object storage.
        let parquet_file_path = ParquetFilePath::from(&parquet_file_params);
        let object_store_path = parquet_file_path.object_store_path();
        let last_modified = if !self.in_catalog && self.expired {
            catalog.time_provider().now()
                - garbage_collector_config.objectstore_cutoff.duration()
                - ONE_DAY.duration()
        } else {
            catalog.time_provider().now()
        };

        object_store
            .put_with_time(&object_store_path, vec![].into(), last_modified.date_time())
            .await
            .unwrap();

        if self.in_a_snapshot {
            let mut bloom_filter = BloomFilter::empty();
            bloom_filter.insert(&object_store_path);
            let directory = CatalogBackupDirectory::new(DesiredFileListTime::new(
                catalog.time_provider().now() - ONE_DAY.duration() + ONE_HOUR,
            ));
            bloom_filter
                .save(
                    &(Arc::clone(object_store) as Arc<DynObjectStore>),
                    &directory,
                )
                .await
                .unwrap();
        } else {
            // There should be a snapshot that DOESN'T contain the current file (rather than having
            // no snapshots) so that the garbage collector knows snapshot creation is working and
            // it's safe to delete catalog records that have had an opportunity to be captured.
            let bloom_filter = BloomFilter::empty();
            let directory = CatalogBackupDirectory::new(DesiredFileListTime::new(
                catalog.time_provider().now() - ONE_HOUR,
            ));
            bloom_filter
                .save(
                    &(Arc::clone(object_store) as Arc<DynObjectStore>),
                    &directory,
                )
                .await
                .unwrap();
        }

        if self.in_catalog {
            let parquet_file = catalog
                .repositories()
                .parquet_files()
                .create(parquet_file_params)
                .await
                .unwrap();

            if self.soft_deleted {
                let saved_now = mock_time_provider.now();
                if self.expired {
                    // Fake this deletion happening greater than parquetfile_cutoff ago
                    mock_time_provider
                        .set(saved_now - FOURTEEN_DAYS.duration() - ONE_DAY.duration());
                } else {
                    // Otherwise, fake it happening an hour ago
                    mock_time_provider.set(saved_now - ONE_HOUR);
                }
                catalog
                    .repositories()
                    .parquet_files()
                    .create_upgrade_delete(
                        partition.id,
                        &[parquet_file.object_store_id],
                        &[],
                        &[],
                        CompactionLevel::Initial,
                    )
                    .await
                    .unwrap();
                // Restore the time provider to its previous "now"
                mock_time_provider.set(saved_now);
            }
        } else if self.soft_deleted {
            panic!("soft_deleted: true doesn't make sense with in_catalog: false");
        }

        BuiltSituation {
            input: self,
            parquet_file_path,
        }
    }
}

/// A situation that has been set up along with the path to the Parquet file in object storaget
/// that was created during said setup.
struct BuiltSituation {
    input: Situation,
    parquet_file_path: ParquetFilePath,
}

impl fmt::Debug for BuiltSituation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltSituation")
            .field("input", &self.input)
            .field("parquet_file_path", &self.parquet_file_path.to_string())
            .finish()
    }
}

impl BuiltSituation {
    /// To be called after running garbage collection. Verifies the situation is as described in
    /// its `end_state`.
    async fn verify_end_state(
        &self,
        catalog: &Arc<dyn Catalog>,
        garbage_collector: &GarbageCollector,
        object_store: &Arc<MockTimeObjectStore>,
    ) {
        let maybe_catalog_record = catalog
            .repositories()
            .parquet_files()
            .get_by_object_store_id(self.parquet_file_path.object_store_id())
            .await
            .unwrap();

        if self.input.end_state.in_catalog {
            let catalog_record = maybe_catalog_record.unwrap_or_else(|| {
                panic!("Expected catalog record to exist for situation {self:#?}");
            });

            if self.input.end_state.soft_deleted {
                assert!(
                    catalog_record.to_delete.is_some(),
                    "Expected catalog record to be soft deleted; \
                    got {catalog_record:#?} for situation {self:#?}",
                );
            } else {
                assert!(
                    catalog_record.to_delete.is_none(),
                    "Expected catalog record to not be soft deleted; \
                    got {catalog_record:#?} for situation {self:#?}",
                );
            }
        } else {
            assert!(
                maybe_catalog_record.is_none(),
                "Expected no catalog record; got {maybe_catalog_record:#?} for situation {self:#?}",
            );
        }

        if self.input.end_state.in_a_snapshot {
            assert!(
                garbage_collector
                    .paths_in_catalog_backups()
                    .contains(&self.parquet_file_path.object_store_path()),
                "Expected bloom filter to contain path {} but it didn't, in situation {self:#?}",
                self.parquet_file_path
            );
        } else {
            assert!(
                !garbage_collector
                    .paths_in_catalog_backups()
                    .contains(&self.parquet_file_path.object_store_path()),
                "Expected bloom filter to not contain path {} but it did, in situation {self:#?}",
                self.parquet_file_path
            );
        }

        let maybe_in_object_store = object_store
            .head(&self.parquet_file_path.object_store_path())
            .await;

        if self.input.end_state.in_object_storage {
            assert!(
                maybe_in_object_store.is_ok(),
                "Expected {} to be present in object store, it was absent, in situation {self:#?}",
                self.parquet_file_path
            );
        } else {
            assert!(
                maybe_in_object_store.is_err(),
                "Expected {} to be absent from object store, \
                it was present, in situation {self:#?}",
                self.parquet_file_path
            );
        }
    }
}

/// Describes the expected state of the Parquet file in a particular situation after running
/// garbage collection.
#[derive(Debug)]
struct EndState {
    in_catalog: bool,
    soft_deleted: bool,
    in_a_snapshot: bool,
    in_object_storage: bool,
}

/// A wrapper for an object store that allows faking the last modified time of files
#[derive(Debug)]
struct MockTimeObjectStore {
    inner: Arc<DynObjectStore>,
    last_modified_times: Arc<RwLock<HashMap<object_store::path::Path, chrono::DateTime<Utc>>>>,
}

impl fmt::Display for MockTimeObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MockTimeObjectStore")
    }
}

impl MockTimeObjectStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            last_modified_times: Default::default(),
        }
    }

    // Save a fake last_modified time for this location
    async fn put_with_time(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        last_modified: chrono::DateTime<Utc>,
    ) -> object_store::Result<object_store::PutResult> {
        self.last_modified_times
            .write()
            .unwrap()
            .insert(location.clone(), last_modified);
        self.inner.put(location, payload).await
    }

    // If we've previously saved a last_modified time for the path in this metadata, replace the
    // last_modified time with the one we saved.
    fn fake_time(&self, meta: object_store::ObjectMeta) -> object_store::ObjectMeta {
        let object_store::ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version,
        } = meta;

        match self.last_modified_times.read().unwrap().get(&location) {
            None => object_store::ObjectMeta {
                location,
                last_modified,
                size,
                e_tag,
                version,
            },
            Some(fake_time) => object_store::ObjectMeta {
                location,
                last_modified: *fake_time,
                size,
                e_tag,
                version,
            },
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for MockTimeObjectStore {
    async fn get(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<object_store::GetResult> {
        let inner_object = self.inner.get(location).await?;

        let object_store::GetResult {
            payload,
            meta,
            range,
            attributes,
        } = inner_object;

        let fake_meta = self.fake_time(meta);

        object_store::Result::Ok(object_store::GetResult {
            payload,
            meta: fake_meta,
            range,
            attributes,
        })
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        Box::pin(
            self.inner
                .list(prefix)
                .map_ok(|object_meta| self.fake_time(object_meta)),
        )
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await.map(|list| {
            let object_store::ListResult {
                common_prefixes,
                objects,
            } = list;

            let fake_objects: Vec<_> = objects
                .into_iter()
                .map(|object_meta| self.fake_time(object_meta))
                .collect();

            object_store::ListResult {
                common_prefixes,
                objects: fake_objects,
            }
        })
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.last_modified_times.write().unwrap().remove(location);
        self.inner.delete(location).await
    }

    // The rest of the methods delegate to the inner object store

    async fn put(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &object_store::path::Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &object_store::path::Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<object_store::ObjectMeta> {
        self.inner.head(location).await
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
