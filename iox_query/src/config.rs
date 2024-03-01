use std::{str::FromStr, time::Duration};

use datafusion::{common::extensions_options, config::ConfigExtension};

/// IOx-specific config extension prefix.
pub const IOX_CONFIG_PREFIX: &str = "iox";

extensions_options! {
    /// Config options for IOx.
    pub struct IoxConfigExt {
        /// When splitting de-duplicate operations based on IOx partitions[^iox_part], this is the maximum number of IOx
        /// partitions that should be considered. If there are more partitions, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        ///
        ///
        /// [^iox_part]: "IOx partition" refers to a partition within the IOx catalog, i.e. a partition within the
        ///              primary key space. This is NOT the same as a DataFusion partition which refers to a stream
        ///              within the physical plan data flow.
        pub max_dedup_partition_split: usize, default = 10_000

        /// When splitting de-duplicate operations based on time-based overlaps, this is the maximum number of groups
        /// that should be considered. If there are more groups, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        pub max_dedup_time_split: usize, default = 100

        /// When multiple parquet files are required in a sorted way (e.g. for de-duplication), we have two options:
        ///
        /// 1. **In-mem sorting:** Put them into [`target_partitions`] DataFusion partitions. This limits the fan-out,
        ///    but requires that we potentially chain multiple parquet files into a single DataFusion partition. Since
        ///    chaining sorted data does NOT automatically result in sorted data (e.g. AB-AB is not sorted), we need to
        ///    preform an in-memory sort using [`SortExec`] afterwards. This is expensive.
        /// 2. **Fan-out:** Instead of chaining files within DataFusion partitions, we can accept a fan-out beyond
        ///    [`target_partitions`]. This prevents in-memory sorting but may result in OOMs (out-of-memory).
        ///
        /// We try to pick option 2 up to a certain number of files, which is configured by this setting.
        ///
        ///
        /// [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec
        /// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
        pub max_parquet_fanout: usize, default = 40

        /// Number of input streams to prefect for ProgressiveEvalExec
        /// Since ProgressiveEvalExec only polls one stream at a time in their stream order,
        /// we do not need to prefetch all streams at once to save resources. However, if the
        /// streams' IO time is way more than their CPU/procesing time, prefetching them will help
        /// improve the performance.
        /// Default is 2 which means we will prefetch one extra stream before polling the current one.
        /// Increasing this value if IO time to read a stream is often much more than CPU time to process its previous one.
        pub progressive_eval_num_prefetch_input_streams: usize, default = 2

        /// Cuttoff date for InfluxQL metadata queries.
        pub influxql_metadata_cutoff: MetadataCutoff, default = MetadataCutoff::Relative(Duration::from_secs(3600 * 24))

        /// Limit for the number of partitions to scan in a single query. Zero means no limit.
        pub partition_limit: usize, default = 0

        /// Limit for the number of parquet files to scan in a single query. Zero means no limit.
        pub parquet_file_limit: usize, default = 0
    }
}

impl IoxConfigExt {
    /// Get the partition limit as an Option.
    pub fn partition_limit_opt(&self) -> Option<usize> {
        match self.partition_limit {
            0 => None,
            n => Some(n),
        }
    }

    /// Set the partition limit. If the limit is already set it will not be increased.
    pub fn set_partition_limit(&mut self, limit: usize) {
        match (self.partition_limit, limit) {
            (_, 0) => {}
            (0, n) => self.partition_limit = n,
            (a, b) => self.partition_limit = a.min(b),
        }
    }

    /// Get the parquet file limit as an Option.
    pub fn parquet_file_limit_opt(&self) -> Option<usize> {
        match self.parquet_file_limit {
            0 => None,
            n => Some(n),
        }
    }

    /// Set the parquet file limit. If the limit is already set it will not be increased.
    pub fn set_parquet_file_limit(&mut self, limit: usize) {
        match (self.parquet_file_limit, limit) {
            (_, 0) => {}
            (0, n) => self.parquet_file_limit = n,
            (a, b) => self.parquet_file_limit = a.min(b),
        }
    }
}

impl ConfigExtension for IoxConfigExt {
    const PREFIX: &'static str = IOX_CONFIG_PREFIX;
}

/// Optional datetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCutoff {
    Absolute(chrono::DateTime<chrono::Utc>),
    Relative(Duration),
}

#[derive(Debug)]
pub struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

impl FromStr for MetadataCutoff {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(s) = s.strip_prefix('-') {
            let delta = u64::from_str(s).map_err(|e| ParseError(e.to_string()))?;
            let delta = Duration::from_nanos(delta);
            Ok(Self::Relative(delta))
        } else {
            let dt = chrono::DateTime::<chrono::Utc>::from_str(s)
                .map_err(|e| ParseError(e.to_string()))?;
            Ok(Self::Absolute(dt))
        }
    }
}

impl std::fmt::Display for MetadataCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relative(delta) => write!(f, "-{}", delta.as_nanos()),
            Self::Absolute(dt) => write!(f, "{}", dt),
        }
    }
}
