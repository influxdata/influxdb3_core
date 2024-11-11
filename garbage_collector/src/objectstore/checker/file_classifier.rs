use std::sync::Arc;

use data_types::ObjectStoreId;
use iox_time::Time;
use object_store::ObjectMeta;
use observability_deps::tracing::warn;

use super::{
    lists::{DeletionCandidateList, ImmediateDeletionList},
    CheckerMetrics,
};
use crate::PathsInCatalogBackups;

/// A [`FileClassifier`] inspects [`ObjectMeta`] listings from an IOx object
/// storage bucket, grouping them into three sets:
///
///   * The implicit set of files that should be left alone (do not delete)
///   * The [`ImmediateDeletionList`], of which files can be immediately
///     deleted.
///   * The [`DeletionCandidateList`] that MAY be eligible for deletion subject
///     to catalog / soft-deletion checks.
///
#[derive(Debug)]
pub(super) struct FileClassifier<'a> {
    /// The timestamp cut-off after which "normal" files are considered eligible
    /// for deletion subject to further checks (critically catalog presence &
    /// soft deletion markers).
    normal_expires_at: Time,

    /// The timestamp cut-off after which bulk ingest files are considered
    /// eligible for deletion.
    bulk_ingest_expires_at: Time,

    /// The timestamp cutoff after which hourly (non-midnight) catalog backup file list files
    /// are considered eligible for deletion.
    catalog_backup_hourly_expires_at: Time,

    /// The timestamp cutoff after which daily (midnight) catalog backup file list files
    /// are considered eligible for deletion.
    catalog_backup_daily_expires_at: Time,

    /// A bloom filter that indicates presence of a file in a catalog backup file list
    /// data snapshot. If the file is present in a catalog backup, the file should be kept.
    paths_in_catalog_backups: &'a Arc<PathsInCatalogBackups>,

    /// The set of objects that should definitely be deleted.
    to_delete: ImmediateDeletionList,

    /// The set of objects that should be deleted only if they are not
    /// referenced in the catalog.
    to_check_in_catalog: DeletionCandidateList,

    metrics: &'a CheckerMetrics,
}

impl<'a> FileClassifier<'a> {
    /// Create a new instance from the configuration. Panics if catalog backups are enabled and
    /// `catalog_backup_hourly_expires_at` is earlier than `catalog_backup_daily_expires_at`
    /// because the logic of the classifier assumes we'll always want to keep daily catalog backups
    /// longer than we keep hourly ones.
    pub(crate) fn new(
        normal_expires_at: Time,
        bulk_ingest_expires_at: Time,
        catalog_backup_hourly_expires_at: Time,
        catalog_backup_daily_expires_at: Time,
        paths_in_catalog_backups: &'a Arc<PathsInCatalogBackups>,
        metrics: &'a CheckerMetrics,
    ) -> Self {
        if paths_in_catalog_backups.enabled() {
            assert!(catalog_backup_hourly_expires_at >= catalog_backup_daily_expires_at);
        }

        Self {
            normal_expires_at,
            bulk_ingest_expires_at,
            catalog_backup_hourly_expires_at,
            catalog_backup_daily_expires_at,
            paths_in_catalog_backups,
            to_delete: Default::default(),
            to_check_in_catalog: Default::default(),
            metrics,
        }
    }

    /// Evaluate `candidate` for deletion.
    ///
    /// If `candidate` is considered deletable, it is placed into the internal
    /// deletion list, avaliable by calling [`FileClassifier::into_lists()`].
    ///
    /// If `candidate` is not considered deletable, this call is a no-op.
    pub(super) fn check(&mut self, candidate: ObjectMeta) {
        // Is this a bulk ingest file? If so, dispatch to a specific handler.
        if is_bulk_ingest_file(&candidate.location) {
            return self.evaluate_bulk_ingest_file(candidate);
        }

        // Is this a catalog backup file? If so, dispatch to a specific handler.
        if is_catalog_backup_file(&candidate.location) {
            return self.evaluate_catalog_backup_file(candidate);
        }

        // Otherwise the file last_modified exceeds the cut-off and it is
        // eligible for deletion.
        self.evaluate_normal_file(candidate)
    }

    /// Inspect `candidate` for deletion as a "normal" file (non-IOx or IOx data
    /// file).
    fn evaluate_normal_file(&mut self, candidate: ObjectMeta) {
        // This file is not a parquet file, and it was not previously filtered
        // as a known file for bulk ingest / catalog backup.
        assert!(!is_bulk_ingest_file(&candidate.location));
        assert!(!is_catalog_backup_file(&candidate.location));

        // Is this file within the "normal" retention window?
        if self.normal_expires_at.date_time() < candidate.last_modified {
            // It is - do nothing.
            return;
        }

        // Is this file in the catalog backup data snapshot bloom filter, meaning
        // it is in at least one snapshot?
        //
        // Even if this is check is racing with a new catalog backup being created, it's fine
        // because this check is _after_ the check of `normal_expires_at`. `normal_expires_at`
        // comes from a `CutoffDuration` that must be at least 3h to be valid. For more
        // information, see the comment containing "Invariant" in
        // `garbage_collector::catalog_backups`.
        if self.paths_in_catalog_backups.contains(&candidate.location) {
            // It is - do nothing.
            return;
        }

        // Extract the filename
        let filename = match candidate.location.parts().last() {
            Some(v) => v,
            None => {
                // This file doesn't have a filename.
                //
                // This is an unexpected case that should not occur. Do not
                // delete anything that isn't well-known by this GC process.
                warn!(?candidate, "ignoring unexpected empty path");
                self.metrics.missing_filename_error_count.inc(1);
                return;
            }
        };

        let uuid_str = match filename.as_ref().strip_suffix(".parquet") {
            Some(v) => v,
            None => {
                warn!(?candidate, "unexpected filetype in object store");
                self.metrics.unexpected_file_count.inc(1);

                // It is not a parquet file - it can be immediately deleted.
                return self.to_delete.push(candidate);
            }
        };

        // Extract the UUID from this parquet filename.
        match uuid_str.parse::<ObjectStoreId>() {
            Ok(id) => {
                // This parquet file can only be deleted if it is not referenced
                // in the catalog.
                self.to_check_in_catalog.push(id, candidate);
            }
            Err(e) => {
                warn!(?candidate, error=%e, "parquet file has invalid UUID");
                self.metrics.invalid_parquet_filename_count.inc(1);
                self.to_delete.push(candidate);
            }
        }
    }

    // Evaluate a "bulk ingest" file for deletion.
    fn evaluate_bulk_ingest_file(&mut self, candidate: ObjectMeta) {
        // Evaluate file age against the bulk ingest specific cut-off.
        if self.bulk_ingest_expires_at.date_time() >= candidate.last_modified {
            self.to_delete.push(candidate);
        }
    }

    // Evaluate a "catalog backup" file for deletion.
    fn evaluate_catalog_backup_file(&mut self, candidate: ObjectMeta) {
        // If catalog backups aren't enabled, don't clean up catalog backup files.
        if !self.paths_in_catalog_backups.enabled() {
            return;
        }

        if let Some((catalog_backup_dir, _rest)) =
            catalog_backup_file_list::catalog_backup_directory_and_rest(&candidate.location)
        {
            let is_midnight = catalog_backup_dir.as_of().hour() == 0
                && catalog_backup_dir.as_of().minute() == 0
                && catalog_backup_dir.as_of().second() == 0;

            if is_midnight {
                if self.catalog_backup_daily_expires_at >= catalog_backup_dir.as_of() {
                    self.to_delete.push(candidate);
                }
            } else if self.catalog_backup_hourly_expires_at >= catalog_backup_dir.as_of() {
                self.to_delete.push(candidate);
            }
        }
        // If the file doesn't start with a catalog backup dir, never delete it.
    }

    /// Consume this [`FileClassifier`], returning the set of files eligible for
    /// immediate deletion ([`ImmediateDeletionList`]) and the set of files that are
    /// candidates for deletion subject to further checks (critically catalog
    /// presence & soft deletion markers).
    pub(crate) fn into_lists(self) -> (ImmediateDeletionList, DeletionCandidateList) {
        (self.to_delete, self.to_check_in_catalog)
    }
}

/// Check if a file's location matches the naming of bulk ingested files and
/// thus the bulk ingest cutoff should apply rather than the regular cutoff.
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

/// Check if a file's location matches the naming of catalog backup files and
/// thus should be cleaned up differently than the data files themselves.
fn is_catalog_backup_file(location: &object_store::path::Path) -> bool {
    location
        .parts()
        .next()
        .map(|dir| dir.as_ref() == catalog_backup_file_list::OBJECT_STORE_PREFIX)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, LazyLock},
        time::Duration,
    };

    use catalog_backup_file_list::{CatalogBackupDirectory, DesiredFileListTime};
    use chrono::{DurationRound, TimeDelta};
    use metric::{assert_counter, Attributes, U64Counter};
    use object_store::path::Path;

    use super::*;

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

    static NORMAL_EXPIRES_AT: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-01-01T00:00:00z").unwrap());
    static BULK_EXPIRES_AT: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-02-02T00:00:00z").unwrap());

    static CATALOG_BACKUP_HOURLY_EXPIRES_AT: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-02-15T00:00:00z").unwrap());
    static CATALOG_BACKUP_DAILY_EXPIRES_AT: LazyLock<Time> =
        LazyLock::new(|| Time::from_rfc3339("2022-01-15T00:00:00z").unwrap());

    /// The expected form of parquet file paths.
    static PARQUET_PATH: LazyLock<Path> = LazyLock::new(|| {
        Path::parse("1/2/4/00000000-0000-0000-0000-000000000000.parquet").unwrap()
    });

    /// Produce a timestamp that causes the file to be expired relative to `ts`.
    fn expired(ts: Time) -> Time {
        ts - Duration::from_secs(42)
    }

    /// Produce a timestamp that causes the file to be not expired relative to
    /// `ts`.
    fn not_expired(ts: Time) -> Time {
        ts + Duration::from_secs(42)
    }

    /// Expected outcomes of [`FileClassifier::check()`] call.
    #[derive(Debug)]
    enum Outcome {
        Keep,
        ImmediateDelete,
        MaybeDelete,
    }

    macro_rules! test_classify {
        (
            $name:ident,
            use_bloom = $use_bloom:expr,
            in_bloom = $in_bloom:expr,
            file = $file:expr,
            want = $want:expr,
            $(metrics = $(($which_metric:ident, $metric_value:literal)),+)?
        ) => {
            paste::paste! {
                #[test]
                fn [<test_classify_ $name>]() {
                    let metrics = Arc::new(metric::Registry::default());
                    let checker_metrics = CheckerMetrics::new(Arc::clone(&metrics));

                    let candidate: ObjectMeta = $file;
                    let want: Outcome = $want;

                    let paths_in_catalog_backups = Arc::new(PathsInCatalogBackups::empty_for_testing($use_bloom));

                    if $in_bloom {
                        let mut bloom_filter = catalog_backup_file_list::BloomFilter::empty();
                        bloom_filter.insert(&candidate.location);
                        paths_in_catalog_backups.union(&bloom_filter);
                    }

                    let mut c = FileClassifier::new(
                        *NORMAL_EXPIRES_AT,
                        *BULK_EXPIRES_AT,
                        *CATALOG_BACKUP_HOURLY_EXPIRES_AT,
                        *CATALOG_BACKUP_DAILY_EXPIRES_AT,
                        &paths_in_catalog_backups,
                        &checker_metrics,
                    );

                    c.check(candidate.clone());

                    // Extract the results
                    #[allow(clippy::dbg_macro)] // Help understand failures
                    let (delete, check) = dbg!(c).into_lists();

                    // Convert them to vecs to assert against.
                    let mut delete = delete.into_iter().collect::<Vec<_>>();
                    let mut check = check.into_candidate_vec();

                    match want {
                        Outcome::Keep => {
                            assert!(delete.is_empty(), "wanted keep");
                            assert!(check.is_empty(), "wanted keep");
                        }
                        Outcome::ImmediateDelete => {
                            assert!(check.is_empty(), "wanted immediate delete");
                            assert_eq!(delete.len(), 1, "wanted immediate delete");
                            let got = delete.pop().unwrap();
                            assert_eq!(got, candidate);
                        }
                        Outcome::MaybeDelete => {
                            assert!(delete.is_empty(), "wanted maybe delete");
                            assert_eq!(check.len(), 1, "wanted maybe delete");
                            let (_id, got) = check.pop().unwrap();
                            assert_eq!(got, candidate);
                        }
                    }

                    $(
                        $(
                            $which_metric(&metrics, $metric_value);
                        )+
                    )?
                }
            }
        };
    }

    test_classify!(
        parquet_file_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_within_expiry_in_bloom,
        use_bloom = true,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_within_expiry_in_bloom_but_disabled,
        use_bloom = false,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    test_classify!(
        parquet_file_outside_expiry_in_bloom,
        use_bloom = true,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_outside_expiry_in_bloom_but_disabled,
        use_bloom = false,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    test_classify!(
        parquet_file_at_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: NORMAL_EXPIRES_AT.date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    test_classify!(
        parquet_file_at_expiry_in_bloom,
        use_bloom = true,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: NORMAL_EXPIRES_AT.date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_at_expiry_in_bloom_but_disabled,
        use_bloom = false,
        in_bloom = true,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: NORMAL_EXPIRES_AT.date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    // A file that isn't expired but does not contain a filename.
    test_classify!(
        no_filename_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("/").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // A file that is expired but does not contain a filename.
    test_classify!(
        no_filename_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("/").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // A file that isn't expired but has an empty path.
    // last_modified is checked and this file remains before even getting to location checks,
    // so the missing filename metric should be 0.
    test_classify!(
        empty_filename_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::from(""),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
        metrics = (assert_missing_filename_counter, 0)
    );

    // A file that is expired but has an empty path.
    test_classify!(
        empty_filename_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::from(""),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
        metrics = (assert_missing_filename_counter, 1)
    );

    // A file that isn't expired, but has an unknown extension.
    test_classify!(
        file_extension_not_parquet_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("1/2/4/00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // A file that is expired, but has an unknown extension.
    test_classify!(
        file_extension_not_parquet_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("1/2/4/00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // A file that isn't expired, but has an invalid filename.
    test_classify!(
        parquet_file_invalid_filename_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("1/2/4/bananas.parquet").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // A file that is expired, but has an invalid filename.
    test_classify!(
        parquet_file_invalid_filename_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("1/2/4/bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // A parquet file that has not expired that is at the root - it is not
    // eligible for deletion irrespective of the directory path.
    test_classify!(
        parquet_file_at_root_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.parquet").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // An expired parquet file that is at the root - it is still validated
    // against the catalog irrespective of the directory path.
    test_classify!(
        parquet_file_at_root_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    // A non-expired, non-parquet file that is at the root
    test_classify!(
        file_extension_not_parquet_at_root_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // An expired, non-parquet file that is at the root
    test_classify!(
        file_extension_not_parquet_at_root_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // A non-expired file with no extension that is at the root
    // last_modified is checked and this file remains before even getting to location checks,
    // so the unexpected file metric should be 0.
    test_classify!(
        no_file_extension_at_root_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("5bced7bd-b8f9-4000-b8e8-96bfce0d862b").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
        metrics = (assert_unexpected_file_counter, 0)
    );

    // An expired file with no extension that is at the root
    test_classify!(
        no_file_extension_at_root_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("5bced7bd-b8f9-4000-b8e8-96bfce0d862b").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
        metrics = (assert_unexpected_file_counter, 1)
    );

    // A non-expired parquet file with an invalid filename that is at the root.
    // last_modified is checked and this file remains before even getting to location checks,
    // so the invalid filename metric should be 0.
    test_classify!(
        parquet_file_invalid_filename_at_root_within_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("bananas.parquet").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
        metrics = (assert_invalid_parquet_filename_counter, 0)
    );

    // An expired parquet file with an invalid filename that is at the root -
    // it is still immediately deleted.
    test_classify!(
        parquet_file_invalid_filename_at_root_outside_expiry,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
        metrics = (assert_invalid_parquet_filename_counter, 1)
    );

    // Delete bulk ingest files that have expired.
    test_classify!(
        bulk_ingest_expired,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.parquet")
                .unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Do not delete bulk ingest files that have not expired.
    test_classify!(
        bulk_ingest_not_expired,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.parquet")
                .unwrap(),
            last_modified: not_expired(*BULK_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // Keep bulk ingest files that have expired relative to the bulk ingest
    // time, but do not have a .parquet extension.
    test_classify!(
        bulk_ingest_expired_file_no_parquet_extension,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.bananas")
                .unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // Delete bulk ingest files that have expired relative to the normal expiry
    // time, but do not have a .parquet extension.
    test_classify!(
        bulk_ingest_normal_expired_file_no_parquet_extension,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.bananas")
                .unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Delete bulk ingest files that have expired relative to the bulk ingest
    // time, but have an invalid UUID filename
    test_classify!(
        bulk_ingest_expired_file_invalid_parquet_filename,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/bananas.parquet").unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Delete bulk ingest files that have expired relative to the normal expiry
    // time, but have an invalid UUID filename
    test_classify!(
        bulk_ingest_normal_expired_file_invalid_parquet_filename,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Do not delete non-backup expired files in the top-level catalog backup directory.
    // This ensures regular garbage collector file expiration logic isn't messing with the catalog
    // backups, which have their own logic using dates in filenames.
    test_classify!(
        catalog_backup_files_expired,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("/catalog_backup_file_lists/bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // Do not delete non-backup non-expired files in the top-level catalog backup directory.
    test_classify!(
        catalog_backup_files_not_expired,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: Path::parse("/catalog_backup_file_lists/bananas").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    fn catalog_backup_non_midnight_before(time: Time) -> Path {
        let truncated_to_midnight = time.date_time().duration_trunc(TimeDelta::days(1)).unwrap();
        let hour_before_midnight = truncated_to_midnight - Duration::from_secs(60 * 60);
        catalog_backup_path(Time::from_datetime(hour_before_midnight))
    }

    fn catalog_backup_non_midnight_after(time: Time) -> Path {
        let day_after = time.date_time().duration_trunc(TimeDelta::days(1)).unwrap()
            + Duration::from_secs(60 * 60 * 24);
        let hour_after_midnight = day_after + Duration::from_secs(60 * 60);
        catalog_backup_path(Time::from_datetime(hour_after_midnight))
    }

    fn catalog_backup_midnight_before(time: Time) -> Path {
        let truncated_to_midnight = time.date_time().duration_trunc(TimeDelta::days(1)).unwrap();
        catalog_backup_path(Time::from_datetime(truncated_to_midnight))
    }

    fn catalog_backup_midnight_after(time: Time) -> Path {
        let day_after = time.date_time().duration_trunc(TimeDelta::days(1)).unwrap()
            + Duration::from_secs(60 * 60 * 24);
        catalog_backup_path(Time::from_datetime(day_after))
    }

    fn catalog_backup_path(time: Time) -> Path {
        let as_of = DesiredFileListTime::new(time);
        let catalog_backup_directory = CatalogBackupDirectory::new(as_of);
        catalog_backup_directory.object_store_path().clone()
    }

    test_classify!(
        catalog_backup_files_non_midnight_not_expired,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_non_midnight_after(*CATALOG_BACKUP_HOURLY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        catalog_backup_files_non_midnight_expired,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_non_midnight_before(*CATALOG_BACKUP_HOURLY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    test_classify!(
        catalog_backup_files_non_midnight_expired_but_disabled,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_non_midnight_before(*CATALOG_BACKUP_HOURLY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        catalog_backup_files_midnight_not_expired_hourly,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_midnight_after(*CATALOG_BACKUP_HOURLY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // Even when a snapshot is considered expired according to the hourly cutoff, if it's a
    // "daily" snapshot taken at midnight, keep it around until it's older than the daily cutoff.
    test_classify!(
        catalog_backup_files_midnight_expired_hourly,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_midnight_before(*CATALOG_BACKUP_HOURLY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        catalog_backup_files_midnight_not_expired_daily,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_midnight_after(*CATALOG_BACKUP_DAILY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        catalog_backup_files_midnight_expired_daily,
        use_bloom = true,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_midnight_before(*CATALOG_BACKUP_DAILY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    test_classify!(
        catalog_backup_files_midnight_expired_daily_but_disabled,
        use_bloom = false,
        in_bloom = false,
        file = ObjectMeta {
            location: catalog_backup_midnight_before(*CATALOG_BACKUP_DAILY_EXPIRES_AT),
            last_modified: (*NORMAL_EXPIRES_AT).date_time(),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );
}
