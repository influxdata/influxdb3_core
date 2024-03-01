//! CLI handling for parquet data cache config (via CLI arguments and environment variables).

/// Config for cache client.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ParquetCacheClientConfig {
    /// The address for the service namespace (not a given instance).
    ///
    /// When the client comes online, it discovers the keyspace
    /// by issue requests to this address.
    #[clap(
        long = "parquet-cache-namespace-addr",
        env = "INFLUXDB_IOX_PARQUET_CACHE_NAMESPACE_ADDR",
        required = false
    )]
    pub namespace_addr: String,
}

/// Cache Policy.
#[derive(Debug, Copy, Clone, Default, clap::Parser)]
pub struct ParquetCachePolicy {
    /// Maximum bytes of parquet files to cache.
    #[clap(
        long = "parquet-cache-capacity",
        env = "INFLUXDB_IOX_PARQUET_CACHE_CAPACITY",
        required = false,
        default_value = format!("{}", 40_u64 * 1024 * 1024 * 1024) // 40 GiB
    )]
    pub max_capacity: u64,

    /// Maximum Cache TTL (without prior eviction).
    #[clap(
        long = "parquet-cache-ttl",
        env = "INFLUXDB_IOX_PARQUET_CACHE_TTL",
        required = false,
        default_value = format!("{}", 1_000_000_000_u64 * 60*60*72) // 72 hrs
    )]
    pub event_recency_max_duration_nanoseconds: u64,
}

/// Config for cache instance.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ParquetCacheInstanceConfig {
    /// The path to the config file for the keyspace.
    #[clap(
        long = "parquet-cache-keyspace-config-path",
        env = "INFLUXDB_IOX_PARQUET_CACHE_KEYSPACE_CONFIG_PATH",
        required = true
    )]
    pub keyspace_config_path: String,

    /// The hostname of the cache instance (k8s pod) running this process.
    ///
    /// Cache controller should be setting this env var.
    #[clap(
        long = "parquet-cache-instance-hostname",
        env = "HOSTNAME",
        required = true
    )]
    pub instance_hostname: String,

    /// The local directory to store data.
    #[clap(
        long = "parquet-cache-local-dir",
        env = "INFLUXDB_IOX_PARQUET_CACHE_LOCAL_DIR",
        required = true
    )]
    pub local_dir: String,

    /// During catalog querying for pre-warming,
    /// what is the max tables that should be queried at once?
    #[clap(
        long = "parquet-cache-prewarm-table-concurrency",
        env = "INFLUXDB_IOX_PARQUET_CACHE_PREWARM_TABLE_CONCURRENCY",
        required = false,
        default_value = "10"
    )]
    pub prewarming_table_concurrency: usize,

    /// Cache Policy
    #[clap(flatten)]
    pub cache_policy: ParquetCachePolicy,
}

impl From<ParquetCacheInstanceConfig> for parquet_cache::ParquetCacheServerConfig {
    fn from(instance_config: ParquetCacheInstanceConfig) -> Self {
        let ParquetCachePolicy {
            max_capacity,
            event_recency_max_duration_nanoseconds,
        } = instance_config.cache_policy;

        Self {
            keyspace_config_path: instance_config.keyspace_config_path,
            hostname: instance_config.instance_hostname,
            local_dir: instance_config.local_dir,
            policy_config: parquet_cache::PolicyConfig {
                max_capacity,
                event_recency_max_duration_nanoseconds,
            },
            prewarming_table_concurrency: instance_config.prewarming_table_concurrency,
        }
    }
}
