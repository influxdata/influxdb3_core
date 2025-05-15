//! Querier-related configs.
use crate::{
    gossip::GossipConfig,
    ingester_address::IngesterAddress,
    memory_size::MemorySize,
    object_store::Endpoint,
    object_store_cache::ObjectStoreCacheMetrics,
    single_tenant::{CONFIG_AUTHZ_ENV_NAME, CONFIG_AUTHZ_FLAG},
};
use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf};

/// CLI config for querier configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct QuerierConfig {
    /// Addr for connection to authz
    #[clap(long = CONFIG_AUTHZ_FLAG, env = CONFIG_AUTHZ_ENV_NAME)]
    pub authz_address: Option<String>,

    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(
        long = "num-query-threads",
        env = "INFLUXDB_IOX_NUM_QUERY_THREADS",
        action
    )]
    pub num_query_threads: Option<NonZeroUsize>,

    /// The number of DataFusion target partitions.
    ///
    /// Sets the "fan-out" to parallelize an individual query. Higher numbers allow potentially better CPU usage for
    /// long-running queries, but also come with higher overhead for short-running queries.
    ///
    /// Should not be higher than `--num-query-threads`/`INFLUXDB_IOX_NUM_QUERY_THREADS`.
    ///
    /// Defaults to the minimum of:
    /// - constant: `4`
    /// - half of query threads: half of `--num-query-threads`/`INFLUXDB_IOX_NUM_QUERY_THREADS` or -- if not specified
    ///   -- half of the the CPU core count, but at least 1
    #[clap(
        long = "num-query-partitions",
        env = "INFLUXDB_IOX_NUM_QUERY_PARTITIONS",
        action
    )]
    pub num_query_partitions: Option<NonZeroUsize>,

    /// Size of memory pool used during query exec, in bytes.
    ///
    /// If queries attempt to allocate more than this many bytes
    /// during execution, they will error with "ResourcesExhausted".
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: MemorySize,

    /// The minimum size of the memory pool pre-reserved for each query, in bytes.
    ///
    /// Set to zero to disable.
    ///
    /// Can be given as an absolute value, such as 26214400 (25 MB). Currently, a value
    /// in percentage of the total available memory is not supported.
    ///
    /// This configuration controls enabling/disabling per-query memory pool (see graph below),
    /// which ensures that each query has a dedicated memory pool with minimum memory budget.
    /// The per-query memory pool is separate from the central memory pool. Most queries will
    /// run successfully within the 25 MB reservation. This setup maximizes the chances of simple
    /// queries succeeding while allowing queries that require additional memory to allocate the
    /// excess from the central memory pool.
    ///
    /// This value is used in `PerQueryMemoryPool`, where each query is reserved
    /// a minimum amount of memory for execution.
    ///
    /// The total pre-reserved memory for all queries is calculated as:
    ///
    /// `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES * INFLUXDB_IOX_MAX_CONCURRENT_QUERIES`
    ///
    /// Given the following configurations:
    ///   - `INFLUXDB_IOX_EXEC_MEM_POOL_BYTES` = 8GB
    ///   - `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES` = 25MB
    ///   - `INFLUXDB_IOX_MAX_CONCURRENT_QUERIES` = 80
    ///
    /// The total pre-reserved memory is:
    ///
    /// `25 MB/query x 80 queries = 2 GB`
    ///
    /// ```text
    ///    │<---- INFLUXDB_IOX_EXEC_MEM_POOL_BYTES 8 GB ------>│
    ///    |<-- 2 GB --->||<------------- 6 GB --------------->|
    ///    ┌─────────────┐┌────────────────────────────────────┐
    ///    │             ││                                    │
    ///    │  Per Query  ││           Central                  │
    ///    │   Memory    ││           Memory                   │
    ///    │    Pool     ││            Pool                    │
    ///    │             ││                                    │
    ///    └──────▲──────┘└──────────────▲─────────────────────┘
    ///           │                      │
    ///         1 │                    2 │
    ///           │                      │
    ///      Query A  ───────────────────┘
    /// ```
    ///
    /// 1. Query A uses memory from the "PerQueryMemoryPool," which has a
    ///    fixed reservation per query, e.g. 25 MB.
    ///
    /// 2. If the 25 MB reservation is insufficient, Query A will use memory
    ///    from the "Central Memory Pool."
    #[clap(
        long = "exec-per-query-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES",
        default_value = "0",
        action
    )]
    pub exec_per_query_mem_pool_bytes: usize,

    /// gRPC address for the router to talk with the ingesters. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    #[clap(
        long = "ingester-addresses",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        required = false,
        num_args = 0..,
        value_delimiter = ','
    )]
    pub ingester_addresses: Vec<IngesterAddress>,

    /// Optional replication factor for ingestion.
    ///
    /// This value specifies the total number of copies of data after
    /// replication, defaulting to 1.
    ///
    /// The querier uses this information to determine how many ingesters
    /// should respond to a read before considering the read complete.
    #[clap(
        long = "rpc-write-replicas",
        env = "INFLUXDB_IOX_RPC_WRITE_REPLICAS",
        default_value = "1"
    )]
    pub ingester_write_replicas: NonZeroUsize,

    /// Size of the RAM cache used to store data in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "ram-pool-data-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_DATA_BYTES",
        default_value = "1073741824",  // 1GB
        action
    )]
    pub ram_pool_data_bytes: MemorySize,

    /// Size of the parquet in-mem data cache in bytes.
    ///
    /// If NOT set, this will default to `--ram-pool-data-bytes`/`INFLUXDB_IOX_RAM_POOL_DATA_BYTES`.
    ///
    /// Set to zero to disable.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "parquet-data-mem-cache-bytes",
        env = "INFLUXDB_IOX_PARQUET_DATA_MEM_CACHE_BYTES",
        action
    )]
    pub parquet_data_mem_cache_bytes: Option<MemorySize>,

    /// Size of the parquet in-mem metadata cache in bytes.
    ///
    /// If NOT set, this will default to `--ram-pool-data-bytes`/`INFLUXDB_IOX_RAM_POOL_DATA_BYTES`.
    ///
    /// Set to zero to disable.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "parquet-metadata-mem-cache-bytes",
        env = "INFLUXDB_IOX_PARQUET_METADATA_MEM_CACHE_BYTES",
        action
    )]
    pub parquet_metadata_mem_cache_bytes: Option<MemorySize>,

    /// Use object store cache metrics.
    #[clap(flatten)]
    pub object_store_mem_cache_metrics: ObjectStoreCacheMetrics,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "max-concurrent-queries",
        env = "INFLUXDB_IOX_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub max_concurrent_queries: usize,

    /// After how many ingester query errors should the querier enter circuit breaker mode?
    ///
    /// The querier normally contacts the ingester for any unpersisted data during query planning.
    /// However, when the ingester can not be contacted for some reason, the querier will begin
    /// returning results that do not include unpersisted data and enter "circuit breaker mode"
    /// to avoid continually retrying the failing connection on subsequent queries.
    ///
    /// If circuits are open, the querier will NOT contact the ingester and no unpersisted data
    /// will be presented to the user.
    ///
    /// Circuits will switch to "half open" after some jittered timeout and the querier will try to
    /// use the ingester in question again. If this succeeds, we are back to normal, otherwise it
    /// will back off exponentially before trying again (and again ...).
    ///
    /// In a production environment the `ingester_circuit_state` metric should be monitored.
    #[clap(
        long = "ingester-circuit-breaker-threshold",
        env = "INFLUXDB_IOX_INGESTER_CIRCUIT_BREAKER_THRESHOLD",
        default_value = "10",
        action
    )]
    pub ingester_circuit_breaker_threshold: u64,

    /// DataFusion config.
    #[clap(
        long = "datafusion-config",
        env = "INFLUXDB_IOX_DATAFUSION_CONFIG",
        default_value = "",
        value_parser = parse_datafusion_config,
        action
    )]
    pub datafusion_config: HashMap<String, String>,

    /// Enable meta cache
    #[clap(
        long = "optimize-for-meta-cache",
        env = "INFLUXDB_IOX_OPTIMIZE_FOR_META_CACHE",
        action
    )]
    pub optimize_for_meta_cache: bool,

    /// Enable meta cache & compute column statistics for file pruning
    #[clap(
        long = "optimize-for-meta-cache-and-file-pruning",
        env = "INFLUXDB_IOX_OPTIMIZE_FOR_META_CACHE_AND_FILE_PRUNING",
        default_value = "false",
        action
    )]
    pub optimize_for_meta_cache_and_file_pruning: bool,

    /// Enable object store caching by specifying one or more cache endpoints as
    /// a comma delimited string.
    ///
    /// NOTE: this value should be consistent (in content and order) across all
    /// IOx instances.
    #[clap(
        long = "object-store-cache-endpoints",
        env = "INFLUXDB_IOX_OBJECT_STORE_CACHE_ENDPOINTS",
        num_args=1..,
        value_delimiter = ','
    )]
    pub object_store_cache_endpoints: Option<Vec<Endpoint>>,

    /// The threshold for moving items from the small queue to main queue
    /// in S3Fifo. Expressed as a percentage (ie 10 for 10%)
    ///
    /// This impacts the permanence of objects, as items in the main queue
    /// are slower to evict and quicker to return.
    #[clap(
        long = "s3fifo-cache-main-threshold",
        env = "INFLUXDB_IOX_S3FIFO_CACHE_MAIN_THRESHOLD",
        default_value = "25",
        action
    )]
    pub s3fifo_cache_main_threshold: usize,

    /// Size of S3-FIFO ghost set in bytes.
    #[clap(
        long = "s3fifo-cache-ghost-memory-limit",
        env = "INFLUXDB_IOX_S3FIFO_CACHE_GHOST_MEMORY_LIMIT",
        default_value = "134217728",  // 128MB
        action
    )]
    pub s3_fifo_ghost_memory_limit: NonZeroUsize,

    /// Observe file access patterns with the given coalesce value.
    ///
    /// See <https://github.com/influxdata/influxdb_iox/issues/13063>.
    #[clap(
        long = "observer-file-access",
        env = "INFLUXDB_IOX_OBSERVE_FILE_ACCESS",
        action
    )]
    pub observe_file_access: Option<usize>,

    /// Limit to the amount of heap memory that can be allocated by the
    /// querier before queries will be stopped.
    #[clap(
        long = "heap-memory-limit",
        env = "INFLUXDB_IOX_HEAP_MEMORY_LIMIT",
        action
    )]
    pub heap_memory_limit: Option<MemorySize>,

    /// Chunk object store GET requests into the given byte size.
    ///
    /// If racing is active, we race the individual chunks, NOT the entire GET request.
    #[clap(
        long = "chunk-object-store-requests",
        env = "INFLUXDB_IOX_CHUNK_OBJECT_STORE_REQUESTS",
        action
    )]
    pub chunk_object_store_requests: Option<NonZeroUsize>,

    /// If object store chunking is enabled (see
    /// `--chunk-object-store-requests`/`INFLUXDB_IOX_CHUNK_OBJECT_STORE_REQUESTS`) then this sets the maximum number of
    /// concurrent tasks that is created from a single GET request.
    #[clap(
        long = "chunk-object-store-max-concurrent-tasks-per-request",
        env = "INFLUXDB_IOX_CHUNK_OBJECT_STORE_MAX_CONCURRENT_TASKS_PER_REQUEST",
        default_value_t = NonZeroUsize::MAX,
        action
    )]
    pub chunk_object_store_max_concurrent_tasks_per_request: NonZeroUsize,

    /// Number of concurrent object store requests that should race for completion.
    #[clap(
        long = "race-object-store-requests",
        env = "INFLUXDB_IOX_RACE_OBJECT_STORE_REQUESTS",
        action
    )]
    pub race_object_store_requests: Option<NonZeroUsize>,

    /// Maximum number of background tasks that are used to drain losing object store requests.
    #[clap(
        long = "race-object-store-max-draining-tasks",
        env = "INFLUXDB_IOX_RACE_OBJECT_STORE_MAX_DRAINING_TASKS",
        default_value_t = 100,
        action
    )]
    pub race_object_store_max_draining_tasks: usize,

    /// Disk storage location for query-related caches.
    ///
    /// This will mostly hold parquet files for now.
    ///
    /// Cache is NOT used if this value is not provided.
    #[clap(long = "query-cache-dir", env = "INFLUXDB_IOX_QUERY_CACHE_DIR", action)]
    pub parquet_data_disk_cache_location: Option<PathBuf>,

    /// Whether or not to cache recently persisted parquet files into the
    /// querier's object store cache.
    #[clap(
        long = "query-cache-persisted-parquet-files",
        env = "INFLUXDB_IOX_QUERY_CACHE_PERSISTED_PARQUET_FILES",
        default_value = "false"
    )]
    pub cache_persisted_parquet_files: bool,

    /// Gossip configuration for the querier. Only used if the querier is
    /// configured to cache recently persisted parquet files.
    #[clap(flatten)]
    pub gossip_config: GossipConfig,
}

fn parse_datafusion_config(
    s: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(HashMap::with_capacity(0));
    }

    let mut out = HashMap::new();
    for part in s.split(',') {
        let kv = part.trim().splitn(2, ':').collect::<Vec<_>>();
        match kv.as_slice() {
            [key, value] => {
                let key_owned = key.trim().to_owned();
                let value_owned = value.trim().to_owned();
                let existed = out.insert(key_owned, value_owned).is_some();
                if existed {
                    return Err(format!("key '{key}' passed multiple times").into());
                }
            }
            _ => {
                return Err(
                    format!("Invalid key value pair - expected 'KEY:VALUE' got '{s}'").into(),
                );
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use test_helpers::assert_contains;

    #[test]
    fn test_default() {
        let actual = QuerierConfig::try_parse_from(["my_binary"]).unwrap();

        assert_eq!(actual.num_query_threads, None);
        assert!(actual.ingester_addresses.is_empty());
        assert!(actual.datafusion_config.is_empty());
    }

    #[test]
    fn test_num_threads() {
        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--num-query-threads", "42"]).unwrap();

        assert_eq!(
            actual.num_query_threads,
            Some(NonZeroUsize::new(42).unwrap())
        );
    }

    #[test]
    fn test_ingester_addresses_list() {
        let querier = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "http://ingester-0:8082,http://ingester-1:8082",
        ])
        .unwrap();

        let actual: Vec<_> = querier
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();

        let expected = vec!["http://ingester-0:8082/", "http://ingester-1:8082/"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn bad_ingester_addresses_list() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "\\ingester-0:8082",
        ])
        .unwrap_err()
        .to_string();

        assert_contains!(
            actual,
            "error: \
            invalid value '\\ingester-0:8082' \
            for '--ingester-addresses [<INGESTER_ADDRESSES>...]': \
            invalid uri character"
        );
    }

    #[test]
    fn test_datafusion_config() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--datafusion-config= foo : bar , x:y:z  ",
        ])
        .unwrap();

        assert_eq!(
            actual.datafusion_config,
            HashMap::from([
                (String::from("foo"), String::from("bar")),
                (String::from("x"), String::from("y:z")),
            ]),
        );
    }

    #[test]
    fn bad_datafusion_config() {
        let actual = QuerierConfig::try_parse_from(["my_binary", "--datafusion-config=foo"])
            .unwrap_err()
            .to_string();
        assert_contains!(
            actual,
            "error: invalid value 'foo' for '--datafusion-config <DATAFUSION_CONFIG>': Invalid key value pair - expected 'KEY:VALUE' got 'foo'"
        );

        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--datafusion-config=foo:bar,baz:1,foo:2"])
                .unwrap_err()
                .to_string();
        assert_contains!(
            actual,
            "error: invalid value 'foo:bar,baz:1,foo:2' for '--datafusion-config <DATAFUSION_CONFIG>': key 'foo' passed multiple times"
        );
    }

    #[test]
    fn test_object_store_mem_cache_metrics() {
        let querier = QuerierConfig::try_parse_from([
            "my_binary",
            "--object-store-cache-metrics-num-objects",
            "1000000000",
            "--object-store-cache-metrics-false-positive-rate",
            "10",
        ])
        .unwrap();

        let ObjectStoreCacheMetrics {
            number_of_unique_objects,
            false_positive_rate,
        } = querier.object_store_mem_cache_metrics;

        assert_eq!(number_of_unique_objects, NonZeroUsize::new(1000000000));
        assert_eq!(false_positive_rate, NonZeroUsize::new(10));
    }

    #[test]
    fn bad_object_store_mem_cache_metrics() {
        // missing other arg
        let err = QuerierConfig::try_parse_from([
            "my_binary",
            "--object-store-cache-metrics-num-objects",
            "1000000000",
        ])
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "the following required arguments were not provided:\n  --object-store-cache-metrics-false-positive-rate <FALSE_POSITIVE_RATE>"
        );

        // missing other arg
        let err = QuerierConfig::try_parse_from([
            "my_binary",
            "--object-store-cache-metrics-false-positive-rate",
            "10",
        ])
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "the following required arguments were not provided:\n  --object-store-cache-metrics-num-objects <NUMBER_OF_UNIQUE_OBJECTS>"
        );

        // NaN error
        let err = QuerierConfig::try_parse_from([
            "my_binary",
            "--object-store-cache-metrics-false-positive-rate",
            "foo",
        ])
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Should be a valid usize number, instead found 'foo'"
        );

        // NotPercentage error
        let err = QuerierConfig::try_parse_from([
            "my_binary",
            "--object-store-cache-metrics-false-positive-rate",
            "112",
        ])
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Should be a number >0 and <=100, instead found 112"
        );
    }
}
