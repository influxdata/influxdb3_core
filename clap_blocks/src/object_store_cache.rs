//! Object store cache-related configs.

use std::{path::PathBuf, time::Duration};

use humantime::parse_duration;

/// CLI config for object-store-cache configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct ObjectStoreCacheConfig {
    /// Storage location for object store disk data cache.
    #[clap(
        long = "object-store-disk-cache-location",
        env = "INFLUXDB_IOX_OBJECT_STORE_DISK_CACHE_LOCATION",
        action
    )]
    pub cache_location: PathBuf,

    /// When to start to pruning, specific in percentage of disk usage.
    #[clap(
        long = "object-store-disk-cache-usage-threshold-percentage",
        env = "INFLUXDB_IOX_OBJECT_STORE_DISK_CACHE_USAGE_THRESHOLD_PERCENTAGE",
        default_value = "80",
        action
    )]
    pub usage_threshold_percentage: u64,

    /// When the usage triggered (see [`usage_threshold_percentage`](Self::usage_threshold_percentage)), then we wait a
    /// short time until triggering pruning again.
    #[clap(
        long = "object-store-disk-cache-usage-trigger-backoff",
        env = "INFLUXDB_IOX_OBJECT_STORE_DISK_CACHE_USAGE_TRIGGER_BACKOFF",
        default_value = "60s",
        value_parser = parse_duration
    )]
    pub usage_trigger_backoff: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_default() {
        let actual = ObjectStoreCacheConfig::try_parse_from([
            "my_binary",
            "--object-store-disk-cache-location=path/to/cache",
        ])
        .unwrap();

        assert_eq!(actual.cache_location.display().to_string(), "path/to/cache");
    }
}
