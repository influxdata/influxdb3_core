//! CLI config for the ingester using the RPC write path

use std::{num::NonZeroUsize, path::PathBuf};

use crate::{
    gossip::GossipConfig, memory_size::MemorySize, parquet_write_hint::ParquetWriteHintConfig,
};

/// CLI config for the ingester using the RPC write path
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct IngesterConfig {
    /// Gossip config.
    #[clap(flatten)]
    pub gossip_config: GossipConfig,

    /// Parquet write hint config.
    #[clap(flatten)]
    pub parquet_write_hint_config: ParquetWriteHintConfig,

    /// Where this ingester instance should store its write-ahead log files. Each ingester instance
    /// must have its own directory.
    #[clap(long = "wal-directory", env = "INFLUXDB_IOX_WAL_DIRECTORY", action)]
    pub wal_directory: PathBuf,

    /// Specify the maximum allowed incoming RPC write message size sent by the
    /// Router.
    #[clap(
        long = "rpc-write-max-incoming-bytes",
        env = "INFLUXDB_IOX_RPC_WRITE_MAX_INCOMING_BYTES",
        default_value = "104857600", // 100MiB
    )]
    pub rpc_write_max_incoming_bytes: usize,

    /// The number of seconds between WAL file rotations.
    #[clap(
        long = "wal-rotation-period-seconds",
        env = "INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS",
        default_value = "300",
        action
    )]
    pub wal_rotation_period_seconds: u64,

    /// The size in bytes which the ingester will aim to rotate the active WAL
    /// segment at, in order approximately bound WAL segment file sizes.
    #[clap(
        long = "wal-segment-size-soft-limit-bytes",
        env = "INFLUXDB_IOX_WAL_SEGMENT_SIZE_SOFT_LIMIT_BYTES",
        default_value = "512000000", // 512,000,000
        action
    )]
    pub wal_segment_size_soft_limit_bytes: NonZeroUsize,

    /// Sets how many queries the ingester will handle simultaneously before
    /// rejecting further incoming requests.
    #[clap(
        long = "concurrent-query-limit",
        env = "INFLUXDB_IOX_CONCURRENT_QUERY_LIMIT",
        default_value = "20",
        action
    )]
    pub concurrent_query_limit: usize,

    /// The maximum number of persist tasks that can run simultaneously.
    #[clap(
        long = "persist-max-parallelism",
        env = "INFLUXDB_IOX_PERSIST_MAX_PARALLELISM",
        default_value = "5",
        action
    )]
    pub persist_max_parallelism: NonZeroUsize,

    /// The maximum number of persist tasks that can be queued at any one time.
    ///
    /// Once this limit is reached, ingest is blocked until the persist backlog
    /// is reduced.
    #[clap(
        long = "persist-queue-depth",
        env = "INFLUXDB_IOX_PERSIST_QUEUE_DEPTH",
        default_value = "250",
        action
    )]
    pub persist_queue_depth: NonZeroUsize,

    /// The limit at which a partition's estimated persistence cost causes it to
    /// be queued for persistence.
    #[clap(
        long = "persist-hot-partition-cost",
        env = "INFLUXDB_IOX_PERSIST_HOT_PARTITION_COST",
        default_value = "20000000", // 20,000,000
        action
    )]
    pub persist_hot_partition_cost: usize,

    /// Specify the size of the thread-pool used by DataFusion for persisting
    /// data.
    #[clap(
        long = "persist-datafusion-thread-count",
        env = "INFLUXDB_IOX_PERSIST_DATAFUSION_THREAD_COUNT",
        default_value = as_clap_str(half_cores()),
        action
    )]
    pub persist_datafusion_thread_count: NonZeroUsize,

    /// Specify the targetted concurrency for each persist task run within
    /// datafusion.
    #[clap(
        long = "persist-datafusion-exec-concurrency",
        env = "INFLUXDB_IOX_PERSIST_DATAFUSION_EXEC_CONCURRENCY",
        default_value = "1",
        action
    )]
    pub persist_datafusion_exec_concurrency: NonZeroUsize,

    /// The amount of memory DataFusion is allowed to consume across all
    /// concurrent persist operations.
    #[clap(
        long = "persist-datafusion-memory-bytes",
        env = "INFLUXDB_IOX_PERSIST_DATAFUSION_MEMORY_BYTES",
        action
    )]
    pub persist_datafusion_memory_bytes: Option<MemorySize>,

    /// An optional lower bound byte size limit that buffered data within a
    /// partition must reach in order to be converted into an incremental
    /// snapshot at query time.
    ///
    /// Snapshots improve query performance by amortising response generation at
    /// the expense of a small memory overhead. Snapshots are retained until the
    /// buffer is persisted.
    #[clap(
        long = "min-partition-snapshot-size",
        env = "INFLUXDB_IOX_MIN_PARTITION_SNAPSHOT_SIZE"
    )]
    pub min_partition_snapshot_size: Option<NonZeroUsize>,

    /// Limit the number of partitions that may be buffered in a single
    /// namespace (across all tables) at any one time.
    ///
    /// This limit is disabled by default.
    #[clap(
        long = "max-partitions-per-namespace",
        env = "INFLUXDB_IOX_MAX_PARTITIONS_PER_NAMESPACE"
    )]
    pub max_partitions_per_namespace: Option<NonZeroUsize>,

    /// Limit the memory usage of the ingester by applying a soft usage limit,
    /// specified in bytes or as a percentage (expressed as 'N%').
    ///
    /// Once this limit is reached, this ingester stops accepting write requests
    /// until the memory usage drops below 2/3rds of this value.
    ///
    /// This limit is inexact, and recovery requires allocating additional
    /// memory to reduce the overall memory pressure. Provide at least 10%
    /// headroom between this limit and any hard cap imposed on this process.
    ///
    /// This limit is applied to the resident set size of the process.
    #[cfg(feature = "jemalloc")]
    #[clap(
        long = "ram-soft-limit-bytes",
        env = "INFLUXDB_IOX_RAM_SOFT_LIMIT_BYTES"
    )]
    pub ram_soft_limit_bytes: Option<MemorySize>,
}

/// Returns exactly half the number of logical cores, or 1 if on a single core
/// system.
fn half_cores() -> usize {
    let cores = std::thread::available_parallelism()
        .expect("failed to lookup cpu core count")
        .get();

    std::cmp::max(cores / 2, 1)
}

fn as_clap_str(v: usize) -> clap::builder::OsStr {
    clap::builder::OsStr::from(std::ffi::OsString::from(v.to_string()))
}
