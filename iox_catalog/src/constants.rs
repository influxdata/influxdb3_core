//! Constants that are hold for all catalog implementations.

use std::time::Duration;

/// Time column.
pub const TIME_COLUMN: &str = "time";

/// Default retention period for data in the catalog.
pub const DEFAULT_RETENTION_PERIOD: Option<i64> = None;

/// Maximum number of files touched by [`ParquetFileRepo::flag_for_delete_by_retention`] at a time.
///
///
/// [`ParquetFileRepo::flag_for_delete_by_retention`]: crate::interface::ParquetFileRepo::flag_for_delete_by_retention
pub const MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION: i64 = 1_000;

/// Maximum number of files returned by ['ParquetFileRepo::list_by_partition_not_to_delete_batch'] at a time.
///
///
/// [`ParquetFileRepo::list_by_partition_not_to_delete_batch`]: crate::interface::ParquetFileRepo::list_by_partition_not_to_delete_batch
pub const MAX_PARQUET_L0_FILES_PER_PARTITION: i64 = 1_000;

/// Maximum number of files touched by [`PartitionRepo::delete_by_retention`] at a time.
///
///
/// [`PartitionRepo::delete_by_retention`]: crate::interface::PartitionRepo::delete_by_retention
pub const MAX_PARTITION_SELECTED_ONCE_FOR_DELETE: usize = 1_000;

/// Default period for which partition records in the catalog must exist before being eligible for
/// being garbage collected for having no Parquet files and being out of retention.
///
/// Having this duration set to one day prevents the garbage collector and bulk ingester from
/// conflicting with each other. Passing a different value into
/// `PartitionRepo::delete_by_retention` is useful in tests.
pub const PARTITION_DELETION_CUTOFF: Duration = Duration::from_secs(60 * 60 * 24);

/// Table names are encoded into objectstore keys when creating Data Snapshots.  There is a maximum key length
/// of 1024 bytes, so table names must be well under that to allow for encoding of special charactors in the
/// table name text that is included in the key.
pub const MAX_TABLE_NAME_LENGTH: usize = 500;
