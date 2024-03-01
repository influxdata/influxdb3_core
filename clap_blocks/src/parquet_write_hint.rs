//! CLI config for commands that support parquet write hinting behaviour.

use url::Url;

/// CLI config for the parqeut write hinting system.
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct ParquetWriteHintConfig {
    /// When set with a non-empty list, will enable sharded write hinting of new parquet
    /// files to the provided object store caches at each URL, which should be
    /// given in the format:
    /// `<HOST_URL>/<BUCKET_NAME>,<HOST_URL>/<BUCKET_NAME>`.
    ///
    /// Endpoints present in the list MUST match across instances and have identical
    /// ordering.
    ///
    /// This is disabled by default.
    #[clap(
        long = "parquet-write-hint-endpoints",
        env = "INFLUXDB_IOX_PARQUET_WRITE_HINT_ENDPOINTS",
        num_args=1..,
        value_delimiter = ','
    )]
    pub parquet_write_hint_endpoints: Option<Vec<Url>>,
}
