//! Metadata index cache.

#![allow(dead_code)]

use std::{mem::size_of, sync::Arc, time::Duration};

use crate::cache_system::{
    cache::Cache,
    hook::{chain::HookChain, limit::MemoryLimiter, observer::ObserverHook},
    interfaces::HasSize,
    reactor::{ticker, Reactor, TriggerExt},
};
use arrow::{
    array::{ArrayRef, RecordBatch},
    error::ArrowError,
};
use data_types::ObjectStoreId;
use datafusion::{common::Statistics, scalar::ScalarValue};
use futures::StreamExt;
use metric::U64Counter;
use tokio::{runtime::Handle, sync::Mutex};

const CACHE_NAME: &str = "parquet_metadata";

/// Parameters for [`MetaIndexCache`].
#[derive(Debug)]
pub struct MetaIndexCacheParams<'a> {
    /// Memory limit in bytes.
    pub memory_limit: usize,

    /// After an OOM event triggered an emergency GC (= garbage collect) run, how how should we wait until the next
    /// run?
    ///
    /// If there is another OOM event during the cooldown period, new elements will NOT be cached.
    pub oom_throttle: Duration,

    /// How often should the GC (= garbage collector) remove unused elements and elements that failed.
    pub gc_interval: Duration,

    /// Metric registry for metrics.
    pub metrics: &'a metric::Registry,

    /// Tokio runtime handle for the background task that drives the GC (= garbage collector).
    pub handle: &'a Handle,
}

impl<'a> MetaIndexCacheParams<'a> {
    /// Build store from parameters.
    pub fn build(self) -> MetaIndexCache {
        let Self {
            memory_limit,
            oom_throttle,
            gc_interval,
            metrics,
            handle,
        } = self;

        let memory_limiter = MemoryLimiter::new(memory_limit);
        let oom_notify = memory_limiter.oom();

        let cache = Arc::new(Cache::new(Arc::new(HookChain::new([
            Arc::new(memory_limiter) as _,
            Arc::new(ObserverHook::new(
                CACHE_NAME,
                metrics,
                Some(memory_limit as u64),
            )) as _,
        ]))));

        let cache_captured = Arc::downgrade(&cache);
        let reactor = Reactor::new(
            [
                oom_notify
                    .boxed()
                    .throttle(oom_throttle)
                    .observe(CACHE_NAME, "oom", metrics),
                ticker(gc_interval).observe(CACHE_NAME, "gc", metrics),
            ],
            Box::new(move || {
                if let Some(cache) = cache_captured.upgrade() {
                    cache.prune();
                }
            }),
            handle,
        );

        MetaIndexCache {
            file_index: cache,
            col_stats_metrics: Arc::new(Mutex::new(StatsCachedMetrics::new(metrics))),
            reactor,
        }
    }

    // build store for testing
    pub fn build_for_default(memory_limit: usize) -> MetaIndexCache {
        // todo: use more consts
        let memory_limiter = MemoryLimiter::new(memory_limit);
        let cache = Arc::new(Cache::new(Arc::new(HookChain::new([
            Arc::new(memory_limiter) as _,
        ]))));

        let merics = metric::Registry::new();
        let col_stats_metrics = Arc::new(Mutex::new(StatsCachedMetrics::new(&merics)));

        let cache_captured = Arc::downgrade(&cache);
        let reactor = Reactor::new(
            [ticker(Duration::from_secs(60)).observe(CACHE_NAME, "gc", &merics)],
            Box::new(move || {
                if let Some(cache) = cache_captured.upgrade() {
                    cache.prune();
                }
            }),
            &Handle::current(),
        );

        MetaIndexCache {
            file_index: cache,
            col_stats_metrics,
            reactor,
        }
    }
}

// Capture metrics for metadata index cache
#[derive(Debug, Default, Clone)]
struct StatsCachedMetrics {
    // counter for counting number of times a column is asked for
    // stats of a file and has it cached
    col_cache_hit: U64Counter,
    // counter for counting number of times a column is asked for
    // stats of a file but has no stats cached.
    col_cache_miss: U64Counter,
}

impl StatsCachedMetrics {
    fn has_col_stats(&mut self) {
        self.col_cache_hit.inc(1);
    }

    fn has_no_col_stats(&mut self) {
        self.col_cache_miss.inc(1);
    }

    // return the counters
    fn num_requests_with_stats(&self) -> U64Counter {
        self.col_cache_hit.clone()
    }
    fn num_requests_without_stats(&self) -> U64Counter {
        self.col_cache_miss.clone()
    }
}

impl StatsCachedMetrics {
    fn new(metrics: &metric::Registry) -> Self {
        let m = metrics.register_metric::<U64Counter>(
            "metadata_index_cache_access",
            "Counts acccesses to metadata index cache",
        );
        Self {
            col_cache_hit: m.recorder(&[("status", "cached")]),
            col_cache_miss: m.recorder(&[("status", "not_cached")]),
        }
    }
}

/// A index to metadata of all files we want to cache
/// Note: Currently, this will be a big hash map for all files in all namespaces/DBs and tables
///       If we hit performance issue, we will split this big hashmap into more structural hash maps
///       (e.g. namespace hashmap, then table hashmap, than partitions hashmap, then file hashmap)
///        See: `<https://github.com/influxdata/influxdb_iox/pull/11282#pullrequestreview-2126017399>`
///        for 3 different options
#[derive(Debug)]
pub struct MetaIndexCache {
    file_index: Arc<Cache<ObjectStoreId, FileMetas>>,

    // cache metrics
    col_stats_metrics: Arc<Mutex<StatsCachedMetrics>>,

    // reactor must just kept alive
    #[allow(dead_code)]
    reactor: Reactor,
}

impl MetaIndexCache {
    // Feature flag to enable/disable meta index cache
    pub fn use_meta_index_cache() -> bool {
        std::env::var("IOX_QUERIER_USE_META_INDEX_CACHE")
            .ok()
            .map(|s| {
                let s = s.to_lowercase();
                (s == "1") || (s == "true")
            })
            .unwrap_or_default()
    }

    /// add new file with its stats if not already added
    pub async fn add_file_with_stats(&mut self, file_uuid: ObjectStoreId, file_stats: Statistics) {
        let (fut, _state) = self.file_index.get_or_fetch(&file_uuid, |_any| async move {
            let cols_stats = file_stats
                .column_statistics
                .iter()
                .map(|col_stat| {
                    if let (Some(min), Some(max)) = (
                        col_stat.min_value.get_value(),
                        col_stat.max_value.get_value(),
                    ) {
                        Some(ColStats {
                            min: min.clone(),
                            max: max.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect();

            Ok(FileMetas {
                col_metas: cols_stats,
            })
        });

        let _ = fut.await;
    }

    pub async fn num_requests_with_stats(&self) -> U64Counter {
        self.col_stats_metrics
            .lock()
            .await
            .num_requests_with_stats()
    }

    pub async fn num_requests_without_stats(&self) -> U64Counter {
        self.col_stats_metrics
            .lock()
            .await
            .num_requests_without_stats()
    }

    pub async fn get_file_stats(&self, file_uuid: &ObjectStoreId) -> Option<Arc<FileMetas>> {
        let result = self.file_index.get(file_uuid).await;

        if let Some(file_metas) = result {
            if let Some(Ok(file_metas)) = file_metas.peek() {
                return Some(Arc::clone(file_metas));
            }
        }

        None
    }

    /// return number of entries in the file index
    pub fn len(&self) -> usize {
        self.file_index.len()
    }

    /// return if the file index is empty
    pub fn is_empty(&self) -> bool {
        self.file_index.len() == 0
    }

    /// get stats of a specified column index of the table
    pub async fn get_col_stats(
        &mut self,
        file_uuid: &ObjectStoreId,
        col_index: usize,
    ) -> Option<Option<ColStats>> {
        let mut handle = self.col_stats_metrics.lock().await;

        if let Some(file_meta) = self.get_file_stats(file_uuid).await {
            if col_index < file_meta.col_metas.len() {
                if file_meta.col_metas[col_index].is_some() {
                    handle.has_col_stats();
                } else {
                    handle.has_no_col_stats();
                }
                return Some(file_meta.col_metas[col_index].clone());
            }
        }

        handle.has_no_col_stats();
        None
    }

    /// Build array refs of the file min max for a given column index on a given list of files
    /// Return a FilesStats that contains the array refs of four fields:
    ///     file_uuid_most_significant, file_uuid_least_significant, min, max
    /// Return None if none of the file has stats for the column index
    pub async fn build_array_ref_for_a_column_on_files(
        &mut self,
        // files we want to get stats from
        file_uuids: &[ObjectStoreId],
        // column index of the table
        col_index: usize,
    ) -> Result<Option<FilesStats>, ArrowError> {
        // collect files with stats
        let mut file_uuids_with_stats = Vec::with_capacity(file_uuids.len());
        let mut mins = Vec::with_capacity(file_uuids.len());
        let mut maxes = Vec::with_capacity(file_uuids.len());
        for file_uuid in file_uuids {
            if let Some(Some(col_stats)) = self.get_col_stats(file_uuid, col_index).await {
                // make a FixedSizeBinary for the file_uuid
                let uuid = file_uuid.get_uuid();
                let uuid = uuid.as_bytes();
                let uuid = ScalarValue::FixedSizeBinary(16, Some(uuid.to_vec()));

                file_uuids_with_stats.push(uuid);
                mins.push(col_stats.min.clone());
                maxes.push(col_stats.max.clone());
            }
        }

        if file_uuids_with_stats.is_empty() {
            return Ok(None);
        }

        // convert to array refs
        let file_uuids_with_stats = ScalarValue::iter_to_array(file_uuids_with_stats.clone())?;
        let mins = ScalarValue::iter_to_array(mins.clone())?;
        let maxes = ScalarValue::iter_to_array(maxes.clone())?;

        Ok(Some(FilesStats {
            file_uuids: Arc::new(file_uuids_with_stats),
            mins,
            maxes,
        }))
    }
}

/// ArrayRefs of the file min max for a given column index on a given list of files
#[derive(Debug)]
pub struct FilesStats {
    file_uuids: ArrayRef,
    mins: ArrayRef,
    maxes: ArrayRef,
}

impl FilesStats {
    /// Convert to a record batch for testing
    pub fn to_record_batch(&self) -> RecordBatch {
        const FILE_UUID: &str = "file_uuid";
        const MIN: &str = "min";
        const MAX: &str = "max";

        RecordBatch::try_from_iter(vec![
            (FILE_UUID, Arc::clone(&self.file_uuids)),
            (MIN, Arc::clone(&self.mins)),
            (MAX, Arc::clone(&self.maxes)),
        ])
        .unwrap()
    }
}

/// Metadata of all columns of a file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetas {
    // Metadata of all columns of the file's table
    // The index of these columns must be in the same order of their appearance in the table schema.
    // Note: Even though we allow new columns added to the table, we do not allow columns to be removed from the table.
    //       Thus, we do not need to store table schema here to know the column index in the table. The query has
    //       table schema at query time. The number of columns here are alway the first columns in the table schema
    col_metas: Vec<Option<ColStats>>,
}

impl HasSize for FileMetas {
    fn size(&self) -> usize {
        self.col_metas
            .iter()
            .map(|col| {
                let mut size = size_of::<Option<ColStats>>();
                if let Some(col) = col {
                    size += col.size();
                }
                size
            })
            .sum()
    }
}

/// Stastistics of a column in a file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColStats {
    pub min: ScalarValue,
    pub max: ScalarValue,
    // If needed we can add row count, null count and distinct count in the future.
    // I do not see they are useful for pruning and do not want to overwhelm the cache for nowß
}

impl ColStats {
    // estimate the size of a column stats
    fn size(&self) -> usize {
        self.min.size() + self.max.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use datafusion::common::{stats::Precision, ColumnStatistics};
    use uuid::Uuid;

    // marco to assert content of the metrics
    macro_rules! assert_metrics {
        ($meta_index:expr, $with_stats:expr, $without_stats:expr) => {
            assert_eq!(
                $meta_index.num_requests_with_stats().await.fetch(),
                $with_stats
            );
            assert_eq!(
                $meta_index.num_requests_without_stats().await.fetch(),
                $without_stats
            );
        };
    }

    #[tokio::test]
    async fn test_file_meta_index() {
        // empty file meta index
        let mut meta_index = MetaIndexCacheParams::build_for_default(100000);

        // initial metric values
        assert_metrics!(meta_index, 0, 0);

        // get stats of a file that does not exist
        let file_1 = ObjectStoreId::from_uuid(Uuid::from_u128(1));
        assert_eq!(meta_index.get_file_stats(&file_1).await, None);

        // add stats for a file
        let (file_stats_1, expected_col_stats_1) = create_df_stats_3_columns();
        meta_index
            .add_file_with_stats(file_1, file_stats_1.clone())
            .await;
        // length of the file index should be 1
        assert_eq!(meta_index.len(), 1);
        // content of the index for file 1
        assert_eq!(
            meta_index.get_file_stats(&file_1).await.unwrap().col_metas,
            expected_col_stats_1
        );

        // add the same file stats again and check if it is not added
        meta_index
            .add_file_with_stats(file_1, file_stats_1.clone())
            .await;
        // length of the file index is still 1
        assert_eq!(meta_index.len(), 1);
        // the stats of file 1 should be the same
        assert_eq!(
            meta_index.get_file_stats(&file_1).await.unwrap().col_metas,
            expected_col_stats_1
        );

        // add stats for another file
        let file_2 = ObjectStoreId::from_uuid(Uuid::from_u128(2));
        let (file_stats_2, expected_col_stats_2) = create_df_stats_4_columns();
        meta_index
            .add_file_with_stats(file_2, file_stats_2.clone())
            .await;
        // length of the file index should be 2
        assert_eq!(meta_index.len(), 2);
        // content of the index for file 2
        assert_eq!(
            meta_index.get_file_stats(&file_2).await.unwrap().col_metas,
            expected_col_stats_2
        );

        // check metrics: still nothing because we have not asked for stats yet
        assert_metrics!(meta_index, 0, 0);

        // build array ref for the first column of both files
        let record_batch = meta_index
            .build_array_ref_for_a_column_on_files(&[file_1, file_2], 0)
            .await
            .unwrap()
            .unwrap()
            .to_record_batch();
        let expected = vec![
            "+----------------------------------+-----+-----+",
            "| file_uuid                        | min | max |",
            "+----------------------------------+-----+-----+",
            "| 00000000000000000000000000000001 | 1   | 10  |",
            "| 00000000000000000000000000000002 | 100 | 110 |",
            "+----------------------------------+-----+-----+",
        ];
        assert_batches_eq!(expected, &[record_batch]);
        //
        // check metrics: one column is asked for stats on 2 files with stats
        // --> num_requests_with_stats increases by 2
        assert_metrics!(meta_index, 2, 0);

        // build array ref for the second column of both files
        let file_stats = meta_index
            .build_array_ref_for_a_column_on_files(&[file_1, file_2], 1)
            .await
            .unwrap();
        // Both of them have no stats for the second column
        assert!(file_stats.is_none());
        //
        // check metrics: one column is asked for stats on 2 files without stats
        // --> num_requests_without_stats increases by 2
        assert_metrics!(meta_index, 2, 2);

        // build array ref for the second column of second file.
        // The second file has no stats for the second column
        assert!(meta_index
            .build_array_ref_for_a_column_on_files(&[file_2], 1)
            .await
            .unwrap()
            .is_none());
        //
        // check metrics: one column is asked for stats on 1 file without stats
        // --> num_requests_without_stats increases by 1
        assert_metrics!(meta_index, 2, 3);

        // build array ref for the third column of first file
        let record_batch = meta_index
            .build_array_ref_for_a_column_on_files(&[file_1], 2)
            .await
            .unwrap()
            .unwrap()
            .to_record_batch();
        // Only file 1 has stats for the third column
        let expected = vec![
            "+----------------------------------+-----+-----+",
            "| file_uuid                        | min | max |",
            "+----------------------------------+-----+-----+",
            "| 00000000000000000000000000000001 |     |     |",
            "+----------------------------------+-----+-----+",
        ];
        assert_batches_eq!(expected, &[record_batch]);
        //
        // check metrics: one column is asked for stats on 1 file with stats
        // --> num_requests_with_stats increases by 1
        assert_metrics!(meta_index, 3, 3);

        // build array ref for the forth column of both files
        let record_batch = meta_index
            .build_array_ref_for_a_column_on_files(&[file_1, file_2], 3)
            .await
            .unwrap()
            .unwrap()
            .to_record_batch();
        // Only file 2 has stats for the forth column
        let expected = vec![
            "+----------------------------------+-----+-----+",
            "| file_uuid                        | min | max |",
            "+----------------------------------+-----+-----+",
            "| 00000000000000000000000000000002 | a   | b   |",
            "+----------------------------------+-----+-----+",
        ];
        assert_batches_eq!(expected, &[record_batch]);
        //
        // check metrics: one column is asked for stats on 2 files one with stats and in without stats
        // --> both num_requests_with_stats and num_requests_without_stats increases by 1
        assert_metrics!(meta_index, 4, 4);

        // build array ref for the second column of an unknonw file
        let unknonw_file_uuid = ObjectStoreId::from_uuid(Uuid::from_u128(100));
        assert!(meta_index
            .build_array_ref_for_a_column_on_files(&[unknonw_file_uuid], 1)
            .await
            .unwrap()
            .is_none());
        //
        // check metrics: one column is asked for stats on 1 file not exist in the index cache (=no stats)
        // --> num_requests_without_stats increases by 1
        assert_metrics!(meta_index, 4, 5);

        // build array ref for an unknonw column of a known file
        assert!(meta_index
            .build_array_ref_for_a_column_on_files(&[unknonw_file_uuid], 10)
            .await
            .unwrap()
            .is_none());
        //
        // check metrics
        // one column is asked for stats on 1 file not exist in the index cache (=no stats) --> num_requests_without_stats increases by 1
        assert_metrics!(meta_index, 4, 6);

        // print the meta index
        // println!("=======\n {}", meta_index);
    }

    // ----------- Test helper

    // create DF Statistics and its respective ColStats for 3 columns
    fn create_df_stats_3_columns() -> (Statistics, Vec<Option<ColStats>>) {
        let null_utf8 = ScalarValue::Utf8(None);

        // DF Statistics for 3 columns: first with stats, second without stats, third with null stats
        let df_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                // column with stats
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    null_count: Precision::Exact(0),
                    distinct_count: Precision::Absent,
                },
                // column without stats
                ColumnStatistics {
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                // column with null stats
                ColumnStatistics {
                    min_value: Precision::Exact(null_utf8.clone()),
                    max_value: Precision::Exact(null_utf8),
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
            ],
        };

        // Corresponsinding ColStatsnof those 3 columns
        let cols_stats = vec![
            Some(ColStats {
                min: ScalarValue::Int64(Some(1)),
                max: ScalarValue::Int64(Some(10)),
            }),
            None,
            Some(ColStats {
                min: ScalarValue::Utf8(None),
                max: ScalarValue::Utf8(None),
            }),
        ];

        (df_stats, cols_stats)
    }

    // create DF Statistics and its respective ColStats for 4 columns
    fn create_df_stats_4_columns() -> (Statistics, Vec<Option<ColStats>>) {
        let null_utf8 = ScalarValue::Utf8(None);

        // DF Statistics for 4 columns: first with stats, second without stats, third with null stats, fourth with stats
        let df_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                // column with stats
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(110))),
                    null_count: Precision::Exact(0),
                    distinct_count: Precision::Absent,
                },
                // column without stats
                ColumnStatistics {
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                // column with null stats
                ColumnStatistics {
                    min_value: Precision::Exact(null_utf8.clone()),
                    max_value: Precision::Exact(null_utf8),
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                // column with stats
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("a".to_string()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_string()))),
                    null_count: Precision::Exact(0),
                    distinct_count: Precision::Absent,
                },
            ],
        };

        // Corresponsinding ColStatsnof those 4 columns
        let cols_stats = vec![
            Some(ColStats {
                min: ScalarValue::Int64(Some(100)),
                max: ScalarValue::Int64(Some(110)),
            }),
            None,
            Some(ColStats {
                min: ScalarValue::Utf8(None),
                max: ScalarValue::Utf8(None),
            }),
            Some(ColStats {
                min: ScalarValue::Utf8(Some("a".to_string())),
                max: ScalarValue::Utf8(Some("b".to_string())),
            }),
        ];

        (df_stats, cols_stats)
    }
}
