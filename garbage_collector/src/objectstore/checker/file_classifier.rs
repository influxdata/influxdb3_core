use chrono::{DateTime, Utc};
use data_types::ObjectStoreId;
use object_store::ObjectMeta;
use observability_deps::tracing::warn;

use super::{
    lists::{DeletionCandidateList, ImmediateDeletionList},
    CheckerMetrics,
};

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
    normal_expires_at: DateTime<Utc>,

    /// The timestamp cut-off after which bulk ingest files are considered
    /// eligible for deletion.
    bulk_ingest_expires_at: DateTime<Utc>,

    /// The set of objects that should definitely be deleted.
    to_delete: ImmediateDeletionList,

    /// The set of objects that should be deleted only if they are not
    /// referenced in the catalog.
    to_check_in_catalog: DeletionCandidateList,

    metrics: &'a CheckerMetrics,
}

impl<'a> FileClassifier<'a> {
    pub(crate) fn new(
        normal_expires_at: DateTime<Utc>,
        bulk_expires_at: DateTime<Utc>,
        metrics: &'a CheckerMetrics,
    ) -> Self {
        Self {
            normal_expires_at,
            bulk_ingest_expires_at: bulk_expires_at,
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
        if self.normal_expires_at < candidate.last_modified {
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
        if self.bulk_ingest_expires_at >= candidate.last_modified {
            self.to_delete.push(candidate);
        }
    }

    // Evaluate a "catalog backup" file for deletion.
    fn evaluate_catalog_backup_file(&mut self, _candidate: ObjectMeta) {
        // Never delete these files.
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

    use object_store::path::Path;

    use super::*;

    static NORMAL_EXPIRES_AT: LazyLock<DateTime<Utc>> = LazyLock::new(|| {
        DateTime::parse_from_str("2022-01-01T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static BULK_EXPIRES_AT: LazyLock<DateTime<Utc>> = LazyLock::new(|| {
        DateTime::parse_from_str("2022-02-02T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });

    /// The expected form of parquet file paths.
    static PARQUET_PATH: LazyLock<Path> = LazyLock::new(|| {
        Path::parse("1/2/4/00000000-0000-0000-0000-000000000000.parquet").unwrap()
    });

    /// Produce a timestamp that causes the file to be expired relative to `ts`.
    fn expired(ts: DateTime<Utc>) -> DateTime<Utc> {
        ts - Duration::from_secs(42)
    }

    /// Produce a timestamp that causes the file to be not expired relative to
    /// `ts`.
    fn not_expired(ts: DateTime<Utc>) -> DateTime<Utc> {
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
            file = $file:expr,
            want = $want:expr,
        ) => {
            paste::paste! {
                #[test]
                fn [<test_classify_ $name>]() {
                    let metrics = Arc::new(metric::Registry::default());
                    let checker_metrics = CheckerMetrics::new(Arc::clone(&metrics));

                    let mut c = FileClassifier::new(
                        *NORMAL_EXPIRES_AT,
                        *BULK_EXPIRES_AT,
                        &checker_metrics,
                    );

                    let candidate: ObjectMeta = $file;
                    let want: Outcome = $want;

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
                }
            }
        };
    }

    test_classify!(
        parquet_file_within_expiry,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    test_classify!(
        parquet_file_outside_expiry,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    test_classify!(
        parquet_file_at_expiry,
        file = ObjectMeta {
            location: PARQUET_PATH.clone(),
            last_modified: *NORMAL_EXPIRES_AT,
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    // A file that is "expired" but does not contain a filename.
    test_classify!(
        no_filename,
        file = ObjectMeta {
            location: Path::parse("/").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // A file that has "expired", but has an unknown extension.
    test_classify!(
        file_extension_not_parquet,
        file = ObjectMeta {
            location: Path::parse("1/2/4/00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // A file that has "expired", but has an invalid filename extension.
    test_classify!(
        parquet_file_invalid_filename,
        file = ObjectMeta {
            location: Path::parse("1/2/4/bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // A parquet file that has not expired that is at the root - it is not
    // eligible for deletion irrespective of the directory path.
    test_classify!(
        parquet_file_not_expired_at_root,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.parquet").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // An expired parquet file that is at the root - it is still validated
    // against the catalog irrespective of the directory path.
    test_classify!(
        expired_parquet_file_at_root,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::MaybeDelete,
    );

    // An expired, non-parquet file that is at the root - it is still
    // immediately deleted.
    test_classify!(
        file_extension_not_parquet_at_root,
        file = ObjectMeta {
            location: Path::parse("00000000-0000-0000-0000-000000000000.bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // An expired, parquet file with an invalid filename that is at the root -
    // it is still immediately deleted.
    test_classify!(
        parquet_file_invalid_filename_at_root,
        file = ObjectMeta {
            location: Path::parse("bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Do not delete anything in the catalog backup directory.
    test_classify!(
        catalog_backup_files_expired,
        file = ObjectMeta {
            location: Path::parse("/catalog_backup_file_lists/bananas").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );
    test_classify!(
        catalog_backup_files_not_expired,
        file = ObjectMeta {
            location: Path::parse("/catalog_backup_file_lists/bananas").unwrap(),
            last_modified: not_expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::Keep,
    );

    // Delete bulk ingest files that have expired.
    test_classify!(
        bulk_ingest_expired,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.parquet")
                .unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );

    // Do not delete bulk ingest files that have not expired.
    test_classify!(
        bulk_ingest_not_expired,
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.parquet")
                .unwrap(),
            last_modified: not_expired(*BULK_EXPIRES_AT),
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
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.bananas")
                .unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT),
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
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/00000000-0000-0000-0000-000000000000.bananas")
                .unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
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
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/bananas.parquet").unwrap(),
            last_modified: expired(*BULK_EXPIRES_AT),
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
        file = ObjectMeta {
            location: Path::parse("0/1/2/bulk_ingest/bananas.parquet").unwrap(),
            last_modified: expired(*NORMAL_EXPIRES_AT),
            size: 42,
            e_tag: None,
            version: None,
        },
        want = Outcome::ImmediateDelete,
    );
}
