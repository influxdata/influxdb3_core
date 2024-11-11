//! Garbage Collector configuration
use clap::Parser;
use humantime::{format_duration, parse_duration};
use snafu::{ensure, ResultExt, Snafu};
use std::{fmt::Debug, time::Duration};

/// The minimum viable cutoff period (3 hours) for the object store, bulk ingest, and parquet
/// files expiry that the garbage collector should be configured to use.
///
/// # Notes
///
/// This is not a scientific measure, it simply acts as an arbitrary guard against
/// the tunable values being configured far too low, with an obvious error when
/// it occurs.
const MINIMUM_CUTOFF_PERIOD: Duration = Duration::from_secs(3600 * 3);

/// A duration guaranteed to be at least `MINUMUM_CUTOFF_PERIOD` (3 hours) long, to guard against
/// durations being too short and causing race conditions or unexpected garbage collection.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub struct CutoffDuration {
    inner: Duration,
}

impl CutoffDuration {
    /// Parse a given duration string with [`humantime::parse_duration`] and validate the duration
    /// is at least as long as the [`MINIMUM_CUTOFF_PERIOD`] (3 hours).
    pub fn try_new(d: &str) -> Result<Self, CutoffDurationError> {
        let duration = parse_duration(d).context(InvalidSnafu {
            given: d.to_string(),
        })?;

        ensure!(
            duration >= MINIMUM_CUTOFF_PERIOD,
            TooShortSnafu { duration }
        );

        Ok(Self { inner: duration })
    }

    /// Read access to the inner [`Duration`].
    pub fn duration(&self) -> Duration {
        self.inner
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum CutoffDurationError {
    #[snafu(display(
        "Cutoff cannot be less than three hours, was specified as {}",
        format_duration(*duration)
    ))]
    TooShort { duration: Duration },

    #[snafu(display("Unable to parse duration from `{given}`: {source}"))]
    Invalid {
        given: String,
        source: humantime::DurationError,
    },
}

/// Configuration specific to the object store garbage collector
#[derive(Debug, Clone, Parser)]
pub struct GarbageCollectorConfig {
    /// Items in the object store that are older than this duration that are not referenced in the
    /// catalog will be deleted.
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        value_parser = CutoffDuration::try_new,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF"
    )]
    pub objectstore_cutoff: CutoffDuration,

    /// Bulk ingest items in the object store that are older than this duration will be deleted.
    /// Bulk ingest files are those that need to be finalized and have not yet been added to the
    /// catalog, so the catalog will not be checked for these files.
    ///
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 1 day ago.
    #[clap(
        long,
        default_value = "1d",
        value_parser = CutoffDuration::try_new,
        env = "INFLUXDB_IOX_GC_BULK_INGEST_OBJECTSTORE_CUTOFF"
    )]
    pub bulk_ingest_objectstore_cutoff: CutoffDuration,

    /// Number of minutes to sleep between iterations of the objectstore list loop.
    /// This is the sleep between entirely fresh list operations.
    /// Defaults to 30 minutes.
    #[clap(
        long,
        default_value_t = 30,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_SLEEP_INTERVAL_MINUTES"
    )]
    pub objectstore_sleep_interval_minutes: u64,

    /// Number of milliseconds to sleep between listing consecutive chunks of objecstore files.
    /// Object store listing is processed in batches; this is the sleep between batches.
    /// Defaults to 1000 milliseconds.
    #[clap(
        long,
        default_value_t = 1000,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_SLEEP_INTERVAL_BATCH_MILLISECONDS"
    )]
    pub objectstore_sleep_interval_batch_milliseconds: u64,

    /// Parquet file rows in the catalog flagged for deletion before this duration will be deleted.
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        value_parser = CutoffDuration::try_new,
        env = "INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF"
    )]
    pub parquetfile_cutoff: CutoffDuration,

    /// Number of minutes to sleep between iterations of the parquet file deletion loop.
    ///
    /// Defaults to 30 minutes.
    ///
    /// If both INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES and
    /// INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL are specified, the smaller is chosen
    #[clap(long, env = "INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES")]
    pub parquetfile_sleep_interval_minutes: Option<u64>,

    /// Duration to sleep between iterations of the parquet file deletion loop.
    ///
    /// Defaults to 30 minutes.
    ///
    /// If both INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES and
    /// INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL are specified, the smaller is chosen
    #[clap(
        long,
        value_parser = parse_duration,
        env = "INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL"
    )]
    pub parquetfile_sleep_interval: Option<Duration>,

    /// Number of minutes to sleep between iterations of the retention code.
    /// Defaults to 35 minutes to reduce incidence of it running at the same time as the parquet
    /// file deleter.
    ///
    /// If the Garbage Collecter cannot keep up with retention at this call frequency, it
    /// will reduce the interval (potentially down to 1m) until it can keep up.
    #[clap(
        long,
        default_value_t = 35,
        env = "INFLUXDB_IOX_GC_RETENTION_SLEEP_INTERVAL_MINUTES"
    )]
    pub retention_sleep_interval_minutes: u64,

    /// Read replica catalog connection string to be used for querying for catalog backup lists of
    /// Parquet files, to avoid adding load to the primary read/write catalog.
    ///
    /// If not specified, defaults to the value specified for
    /// `--catalog-dsn`/`INFLUXDB_IOX_CATALOG_DSN`, which is usually the primary catalog.
    ///
    /// The dsn determines the type of catalog used.
    ///
    /// PostgreSQL: `postgresql://postgres@localhost:5432/postgres`
    ///
    /// Sqlite (a local filename /tmp/foo.sqlite): `sqlite:///tmp/foo.sqlite` -
    /// note sqlite is for development/testing only and should not be used for
    /// production workloads.
    ///
    /// Memory (ephemeral, only useful for testing): `memory`
    ///
    /// Catalog service: `http://catalog-service-0:8080; http://catalog-service-1:8080`
    ///
    #[clap(
        long = "catalog-replica-dsn",
        env = "INFLUXDB_IOX_CATALOG_REPLICA_DSN",
        action
    )]
    pub replica_dsn: Option<String>,

    /// Feature flag that toggles whether or not the GC will delete partitions.
    /// For context see: <https://github.com/influxdata/influxdb_iox/issues/12270>
    #[clap(
        long = "enable-partition-row-deletion",
        env = "INFLUXDB_IOX_ENABLE_PARTITION_ROW_DELETION"
    )]
    pub enable_partition_row_deletion: bool,

    /// Feature flag that toggles whether or not the GC will delete parquet files.
    /// For context see: <https://github.com/influxdata/influxdb_iox/issues/12271>
    #[clap(
        long = "enable-parquet-file-deletion",
        env = "INFLUXDB_IOX_ENABLE_PARQUET_FILE_DELETION"
    )]
    pub enable_parquet_file_deletion: bool,

    /// Feature flag that toggles whether or not the GC will delete from object store.
    /// For context see: <https://github.com/influxdata/influxdb_iox/issues/12271>
    #[clap(
        long = "enable-object-store-deletion",
        env = "INFLUXDB_IOX_ENABLE_OBJECT_STORE_DELETION"
    )]
    pub enable_object_store_deletion: bool,

    /// Feature flag enabling the creation of catalog backup data snapshots. Off by default; hidden
    /// so that no one turns this on out of curiosity by reading the docs; config setting for ease
    /// of enabling/disabling in tests.
    #[clap(
        hide = true,
        long = "create-catalog-backup-data-snapshot-files",
        env = "INFLUXDB_IOX_CREATE_CATALOG_BACKUP_DATA_SNAPSHOT_FILES"
    )]
    pub create_catalog_backup_data_snapshot_files: bool,

    /// Feature flag enabling the use of catalog backup data snapshots for the purpose of deciding
    /// which object store files should be kept or deleted. Off by default; hidden so that no one
    /// turns it on out of curiosity by reading the docs.
    ///
    /// Independent from `create_catalog_backup_data_snapshot_files` so that an environment can
    /// continue to create the backup files without making deletion decisions based on the contents
    /// of the files if the decision logic is incorrect.
    #[clap(
        hide = true,
        long = "delete-using-catalog-backup-data-snapshot-files",
        env = "INFLUXDB_IOX_DELETE_USING_CATALOG_BACKUP_DATA_SNAPSHOT_FILES"
    )]
    pub delete_using_catalog_backup_data_snapshot_files: bool,

    /// How long to keep hourly catalog backup file list snapshots, other than the snapshot taken
    /// at midnight UTC (which is considered the "daily" snapshot).
    ///
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>.
    ///
    /// If not specified, defaults to 30 days.
    ///
    /// The interpretation of this value is that we should always keep an hourly snapshot of at least this
    /// age, to maintain the ability to restore to snapshots back to this age.  Snapshots become eligible
    /// for deleation at 1h past this configured value, ensuring we have hourly snapshots for at least the
    /// configured 'keep'.
    /// When set to 30d, the oldest hourly snapshot will be between 30 days and 30 days 1 hour old.
    ///
    /// Must be shorter than the time to keep the daily snapshots.
    ///
    /// Hidden until the `create_catalog_backup_data_snapshot_files` feature flag is on everywhere
    /// and the feature is officially launched. Has no effect if that feature flag is off.
    #[clap(
        hide = true,
        long = "keep-hourly-catalog-backup-file-lists",
        default_value = "30d",
        value_parser = CutoffDuration::try_new,
        env = "INFLUXDB_IOX_KEEP_HOURLY_CATALOG_BACKUP_FILE_LISTS"
    )]
    pub keep_hourly_catalog_backup_file_lists: CutoffDuration,

    /// How long to keep daily catalog backup file list snapshots (those taken at midnight UTC).
    ///
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>.
    ///
    /// If not specified, defaults to 90 days.
    ///
    /// The interpretation of this value is that we should always keep a daily snapshot of at least this
    /// age, to maintain the ability to restore to snapshots back to this age.  Snapshots become eligible
    /// for deleation at 1d past this configured value, ensuring we have daily snapshots for at least the
    /// configured 'keep'.
    /// When set to 90d, the oldest snapshot will be between 90 and 91 days old.
    ///
    /// Must be longer than the time to keep the hourly snapshots.
    ///
    /// Hidden until the `create_catalog_backup_data_snapshot_files` feature flag is on everywhere
    /// and the feature is officially launched. Has no effect if that feature flag is off.
    #[clap(
        hide = true,
        long = "keep-daily-catalog-backup-file-lists",
        default_value = "90d",
        value_parser = CutoffDuration::try_new,
        env = "INFLUXDB_IOX_KEEP_DAILY_CATALOG_BACKUP_FILE_LISTS"
    )]
    pub keep_daily_catalog_backup_file_lists: CutoffDuration,

    /// Temporary flag enabling use of pg_dump initiated by the garbage collector
    #[clap(
        hide = true,
        long = "enable-pg-dump",
        default_value = "false",
        env = "INFLUXDB_IOX_ENABLE_PG_DUMP"
    )]
    pub enable_pg_dump: bool,
}

impl GarbageCollectorConfig {
    /// Returns the parquet_file sleep interval
    pub fn parquetfile_sleep_interval(&self) -> Duration {
        match (
            self.parquetfile_sleep_interval,
            self.parquetfile_sleep_interval_minutes,
        ) {
            (None, None) => Duration::from_secs(30 * 60),
            (Some(d), None) => d,
            (None, Some(m)) => Duration::from_secs(m * 60),
            (Some(d), Some(m)) => d.min(Duration::from_secs(m * 60)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquetfile_sleep_config() {
        let a: &[&str] = &[];
        let config = GarbageCollectorConfig::parse_from(a);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(30 * 60)
        );

        let config =
            GarbageCollectorConfig::parse_from(["something", "--parquetfile-sleep-interval", "3d"]);

        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(24 * 60 * 60 * 3)
        );

        let config = GarbageCollectorConfig::parse_from([
            "something",
            "--parquetfile-sleep-interval-minutes",
            "34",
        ]);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(34 * 60)
        );

        let config = GarbageCollectorConfig::parse_from([
            "something",
            "--parquetfile-sleep-interval-minutes",
            "34",
            "--parquetfile-sleep-interval",
            "35m",
        ]);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(34 * 60)
        );
    }

    #[test]
    fn cutoff_duration_validation() {
        let error_message = CutoffDuration::try_new("35m").unwrap_err().to_string();
        assert_eq!(
            error_message,
            "Cutoff cannot be less than three hours, was specified as 35m"
        );

        let error_message = CutoffDuration::try_new("more like banaNO")
            .unwrap_err()
            .to_string();
        assert_eq!(
            error_message,
            "Unable to parse duration from `more like banaNO`: expected number at 0"
        );

        let valid_duration = CutoffDuration::try_new("6h").unwrap();
        assert_eq!(valid_duration.duration(), Duration::from_secs(60 * 60 * 6));
    }
}
