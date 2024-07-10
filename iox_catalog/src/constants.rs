//! Constants that are hold for all catalog implementations.

/// Time column.
pub const TIME_COLUMN: &str = "time";

/// Default retention period for data in the catalog.
pub const DEFAULT_RETENTION_PERIOD: Option<i64> = None;

/// Maximum number of files touched by [`ParquetFileRepo::flag_for_delete_by_retention`] at a time.
///
///
/// [`ParquetFileRepo::flag_for_delete_by_retention`]: crate::interface::ParquetFileRepo::flag_for_delete_by_retention
pub const MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION: i64 = 1_000;

/// Maximum number of files touched by [`ParquetFileRepo::delete_old_ids_only`] at a time.
///
///
/// [`ParquetFileRepo::delete_old_ids_only`]: crate::interface::ParquetFileRepo::delete_old_ids_only
pub const MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE: i64 = 10_000;

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
