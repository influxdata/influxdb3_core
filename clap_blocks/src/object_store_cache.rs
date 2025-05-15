//! Object store cache-related configs.

use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use humantime::parse_duration;
use snafu::Snafu;

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

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum ParsePercentageError {
    #[snafu(display("Should be a valid usize number, instead found '{}'", input))]
    NaN { input: String },

    #[snafu(display("Should be a number >0 and <=100, instead found {}", input))]
    NotValidPercentage { input: usize },
}

/// Parse a percentage value between zero and 100.
fn parse_optional_percentage(s: &str) -> Result<NonZeroUsize, ParsePercentageError> {
    let should_be_percent: usize =
        s.to_string()
            .parse()
            .map_err(|_| ParsePercentageError::NaN {
                input: s.to_string(),
            })?;
    let percent = NonZeroUsize::new(should_be_percent)
        .and_then(|num| if num.get() <= 100 { Some(num) } else { None })
        .ok_or(ParsePercentageError::NotValidPercentage {
            input: should_be_percent,
        })?;
    Ok(percent)
}

/// CLI config for object store cache metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::Parser, Default)]
pub struct ObjectStoreCacheMetrics {
    /// Anticipated max number of unique objects
    /// to have been seen by the cache between rollouts.
    #[clap(
        long = "object-store-cache-metrics-num-objects",
        env = "INFLUXDB_IOX_OBJECT_STORE_CACHE_METRICS_NUM_OBJECTS",
        required = false,
        requires = "false_positive_rate",
        action
    )]
    pub number_of_unique_objects: Option<NonZeroUsize>,

    /// Acceptable rate positive rate when determining which objects
    /// have been previously seen by the cache. Percentage between 0-100.
    ///
    /// Used in a calculation for the bloom filter size based upon
    /// <https://hur.st/bloomfilter>.
    #[clap(
        long = "object-store-cache-metrics-false-positive-rate",
        env = "INFLUXDB_IOX_OBJECT_STORE_CACHE_METRICS_FALSE_POSITIVE_RATE",
        required = false,
        requires = "number_of_unique_objects",
        value_parser = parse_optional_percentage,
        action
    )]
    pub false_positive_rate: Option<NonZeroUsize>,
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
