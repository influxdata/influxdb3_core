//! Metadata Index Cache Implementation.
#![warn(missing_docs)]

use tracing::warn;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{collections::HashSet, mem::size_of, num::NonZeroUsize, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, FixedSizeBinaryBuilder, RecordBatch},
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use data_types::ObjectStoreId;
use datafusion::{
    common::{Column, Statistics},
    datasource::file_format::parquet::statistics_from_parquet_meta_calc,
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::{Expr, utils::conjunction},
    parquet::{
        arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
        file::metadata::ParquetMetaData,
    },
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    scalar::ScalarValue,
};
use datafusion_util::create_physical_expr_from_schema;
use futures::FutureExt;
use metric::U64Counter;
use object_store_mem_cache::cache_system::{
    Cache, DynError, HasSize,
    hook::observer::ObserverHook,
    s3_fifo_cache::{S3Config, S3FifoCache},
};

const CACHE_NAME: &str = "parquet_metadata";

/// Parameters for [`MetaIndexCache`].
#[derive(Debug)]
pub struct MetaIndexCacheParams<'a> {
    /// Memory limit in bytes.
    pub memory_limit: NonZeroUsize,

    /// The relative size (in percentage) of the "small" S3-FIFO queue.
    pub s3fifo_main_threshold: usize,

    /// Size of S3-FIFO ghost set in bytes.
    pub s3_fifo_ghost_memory_limit: NonZeroUsize,

    /// Metric registry for metrics.
    pub metrics: &'a metric::Registry,

    /// Enable caching of column statistics from parquet metadata to optimize file pruning
    pub cache_column_stats: bool,
}

impl MetaIndexCacheParams<'_> {
    /// Build store from parameters.
    pub fn build(self) -> MetaIndexCache {
        let Self {
            memory_limit,
            s3_fifo_ghost_memory_limit,
            s3fifo_main_threshold,
            metrics,
            cache_column_stats,
        } = self;

        let cache = Arc::new(S3FifoCache::new(
            S3Config {
                max_memory_size: memory_limit.get(),
                max_ghost_memory_size: s3_fifo_ghost_memory_limit.get(),
                move_to_main_threshold: s3fifo_main_threshold as f64 / 100.0,
                hook: Arc::new(ObserverHook::new(
                    CACHE_NAME,
                    metrics,
                    Some(memory_limit.get() as u64),
                )) as _,
            },
            metrics,
        ));

        MetaIndexCache {
            file_index: cache,
            col_stats_metrics: Arc::new(StatsCachedMetrics::new(metrics)),
            cache_column_stats,
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
    // counter for the number of files pruned from using cache stats
    files_pruned: U64Counter,
}

impl StatsCachedMetrics {
    fn has_col_stats(&self) {
        self.col_cache_hit.inc(1);
    }

    fn has_no_col_stats(&self) {
        self.col_cache_miss.inc(1);
    }

    fn files_pruned(&self, num_files: u64) {
        self.files_pruned.inc(num_files);
    }

    // return the counters
    fn num_requests_with_stats(&self) -> U64Counter {
        self.col_cache_hit.clone()
    }
    fn num_requests_without_stats(&self) -> U64Counter {
        self.col_cache_miss.clone()
    }
    fn num_files_pruned(&self) -> U64Counter {
        self.files_pruned.clone()
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
            files_pruned: m.recorder(&[("status", "files_pruned")]),
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
    file_index: Arc<S3FifoCache<ObjectStoreId, Arc<FileMetas>, ()>>,

    // cache metrics
    col_stats_metrics: Arc<StatsCachedMetrics>,

    /// Cache column statistics for file pruning
    cache_column_stats: bool,
}

impl MetaIndexCache {
    /// add new file with its stats if not already added
    pub async fn add_metadata_for_file<R: AsyncFileReader + Send + 'static>(
        &self,
        file_uuid: &ObjectStoreId,
        table_schema: SchemaRef,
        mut reader: R,
        arrow_reader_options: Option<&ArrowReaderOptions>,
    ) -> Result<Arc<FileMetas>, DynError> {
        let cache_column_stats = self.cache_column_stats;
        let arrow_reader_options = arrow_reader_options.cloned();
        let (res, _state) = self
            .file_index
            .get_or_fetch(
                file_uuid,
                Box::new(move || {
                    async move {
                        let parquet_metadata = reader
                            .get_metadata(arrow_reader_options.as_ref())
                            .await
                            .map_err(|e| Arc::new(e) as DynError)?;
                        // get statistics from metadata
                        let col_metas = cache_column_stats
                            .then(|| {
                                statistics_from_parquet_meta_calc(&parquet_metadata, table_schema)
                                    .map(ColStats::from_statistics)
                                    .ok()
                            })
                            .flatten();
                        Ok(Arc::new(FileMetas {
                            col_metas,
                            parquet_metadata,
                        }))
                    }
                    .boxed()
                }),
            )
            .await;
        res
    }

    /// return number of cache hits
    pub async fn num_requests_with_stats(&self) -> U64Counter {
        self.col_stats_metrics.num_requests_with_stats()
    }

    /// return number of cache misses
    pub async fn num_requests_without_stats(&self) -> U64Counter {
        self.col_stats_metrics.num_requests_without_stats()
    }

    /// return number of files pruned
    pub async fn num_files_pruned(&self) -> U64Counter {
        self.col_stats_metrics.num_files_pruned()
    }

    /// record new pruned files
    pub async fn record_pruned_files(&self, num_files: u64) {
        self.col_stats_metrics.files_pruned(num_files);
    }

    /// get stats of a file
    pub fn get_file_stats(&self, file_uuid: &ObjectStoreId) -> Option<Arc<FileMetas>> {
        self.file_index.get(file_uuid).and_then(|res| res.ok())
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
    /// Return
    ///   . Some(Some(ColStats)) if the file has stats for the column index
    ///   . Some(None) if the file has absent stats for the column index
    ///   . None if the file does not have data for the columnn index (aka null column)
    pub async fn get_col_stats(
        &self,
        file_uuid: &ObjectStoreId,
        col_index: usize,
    ) -> Option<Option<ColStats>> {
        if let Some(file_meta) = self.get_file_stats(file_uuid)
            && let Some(ref col_metas) = file_meta.col_metas
            && col_index < col_metas.len()
        {
            if col_metas[col_index].is_some() {
                self.col_stats_metrics.has_col_stats();
            } else {
                self.col_stats_metrics.has_no_col_stats();
            }
            return Some(col_metas[col_index].clone());
        }
        self.col_stats_metrics.has_no_col_stats();
        None
    }

    /// Build array refs of the file min max for a given column index on a given list of files
    /// Return ColStatsArrayRef which is array refs of the file min max for the column index on the given list of files
    /// Note that it is possible that some of the files have no stats for the column
    pub async fn build_array_ref_for_a_column_on_files(
        &self,
        // files we want to get stats from
        file_uuids: &[ObjectStoreId],
        // column index of the table
        col_index: usize,
    ) -> Result<ColStatsArrayRef, ArrowError> {
        // collect files with stats
        let mut mins = Vec::with_capacity(file_uuids.len());
        let mut maxes = Vec::with_capacity(file_uuids.len());
        let mut null_counts = Vec::with_capacity(file_uuids.len());
        let mut row_counts = Vec::with_capacity(file_uuids.len());
        let mut data_type = ScalarValue::Null.data_type();
        let mut file_idx_with_null_col = Vec::new();
        for file_uuid in file_uuids {
            let col_stats = self.get_col_stats(file_uuid, col_index).await;
            match col_stats {
                Some(Some(col_stats)) => {
                    // Column has stats
                    // This includes null value of a known data type
                    mins.push(col_stats.min.clone());
                    maxes.push(col_stats.max.clone());
                    null_counts.push(col_stats.null_count);
                    row_counts.push(col_stats.row_count);

                    data_type = col_stats.min.data_type();
                }
                Some(None) => {
                    // Column has absent stats
                    // Absent stats for the column index, make min/max null scalar which mean unknown
                    add_scalar_null(&mut mins, &mut maxes, &mut null_counts, &mut row_counts);
                }
                None => {
                    // Column has no data (null column) & unknown data type
                    // --> its min and max are null
                    // Since we do not know the data type, we set its ScalarValue::Null but will convert it to Null of the right data type
                    add_scalar_null(&mut mins, &mut maxes, &mut null_counts, &mut row_counts);
                    file_idx_with_null_col.push(mins.len() - 1);
                }
            }
        }

        // Since there may be files without the columns (and leads to ScalarValue::Null used as unknown data type) and
        // files with the columns with known data type, we need to replace the null scalars of unknown type with the corresponding
        // null scalars of the known data type if avaialble.
        // This is to prevent hitting a bug while converting to array: converting data of every file into null while some actually have values
        if !data_type.is_null() && !file_idx_with_null_col.is_empty() {
            // make null values for the specific  data type
            let null_val = ScalarValue::try_from(data_type.clone())?;
            let null_uint64 = ScalarValue::try_from(DataType::UInt64)?;
            for i in file_idx_with_null_col {
                mins[i] = null_val.clone();
                maxes[i] = null_val.clone();
                null_counts[i] = null_uint64.clone();
                row_counts[i] = null_uint64.clone();
            }
        }

        // Convert to array refs
        let mins = ScalarValue::iter_to_array(mins.clone())?;
        let maxes = ScalarValue::iter_to_array(maxes.clone())?;
        let null_counts = ScalarValue::iter_to_array(null_counts.clone())?;
        let row_counts = ScalarValue::iter_to_array(row_counts.clone())?;

        Ok(ColStatsArrayRef {
            mins,
            maxes,
            null_counts,
            row_counts,
        })
    }
}

fn add_scalar_null(
    mins: &mut Vec<ScalarValue>,
    maxes: &mut Vec<ScalarValue>,
    null_counts: &mut Vec<ScalarValue>,
    row_counts: &mut Vec<ScalarValue>,
) {
    mins.push(ScalarValue::Null);
    maxes.push(ScalarValue::Null);
    null_counts.push(ScalarValue::Null);
    row_counts.push(ScalarValue::Null);
}

// Convert a list of file_uuids to a FixedSizeBinaryArray
fn build_file_uuids_array(file_uuids: &[ObjectStoreId]) -> Result<ArrayRef, ArrowError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(file_uuids.len(), 16);
    for file_uuid in file_uuids {
        let uuid = file_uuid.get_uuid();
        let uuid = uuid.as_bytes();
        builder.append_value(uuid)?
    }

    Ok(Arc::new(builder.finish()))
}

/// ArrayRefs of the file min max for a given column index on a given list of files
#[derive(Debug)]
pub struct ColStatsArrayRef {
    mins: ArrayRef,
    maxes: ArrayRef,
    null_counts: ArrayRef,
    row_counts: ArrayRef,
}

impl ColStatsArrayRef {
    /// Convert to a record batch for testing
    pub fn to_record_batch(&self, file_uuids: &[ObjectStoreId]) -> RecordBatch {
        const FILE_UUID: &str = "file_uuid";
        const MIN: &str = "min";
        const MAX: &str = "max";
        const NULL_COUNT: &str = "null_count";
        const ROW_COUNT: &str = "row_count";

        let file_uuids_array = build_file_uuids_array(file_uuids).unwrap();

        RecordBatch::try_from_iter(vec![
            (FILE_UUID, file_uuids_array),
            (MIN, Arc::clone(&self.mins)),
            (MAX, Arc::clone(&self.maxes)),
            (NULL_COUNT, Arc::clone(&self.null_counts)),
            (ROW_COUNT, Arc::clone(&self.row_counts)),
        ])
        .unwrap()
    }
}

/// Metadata of all columns of a file
#[derive(Debug, Clone, PartialEq)]
pub struct FileMetas {
    ///  The original parquet metadata of the file, so that we can cache it for faster loading in
    /// `AsyncFileReader::get_metadata` calls
    pub parquet_metadata: Arc<ParquetMetaData>,

    /// Metadata of all columns of the file's table
    /// The index of these columns must be in the same order of their appearance in the table schema.
    /// Note: Even though we allow new columns added to the table, we do not allow columns to be removed from the table.
    ///       Thus, we do not need to store table schema here to know the column index in the table. The query has
    ///       table schema at query time. The number of columns here are alway the first columns in the table schema
    pub col_metas: Option<Vec<Option<ColStats>>>,
}

impl HasSize for FileMetas {
    fn size(&self) -> usize {
        //destructure here to force compile error when new fields are added
        let Self {
            parquet_metadata,
            col_metas,
        } = self;
        let mut size = parquet_metadata.memory_size();
        if let Some(col_metas) = col_metas {
            size += col_metas
                .iter()
                .map(|col| {
                    let mut size = size_of::<Option<ColStats>>();
                    if let Some(col) = col {
                        size += col.size();
                    }
                    size
                })
                .sum::<usize>();
        }
        size
    }
}

/// Stastistics of a column in a file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColStats {
    /// Min value of a column
    pub min: ScalarValue,
    /// Max value of a column
    pub max: ScalarValue,
    /// Null count of a column
    pub null_count: ScalarValue,
    /// Row count of a column
    pub row_count: ScalarValue,
}

impl ColStats {
    // estimate the size of a column stats
    #[inline]
    fn size(&self) -> usize {
        self.min.size() + self.max.size()
    }

    /// Build column statistics from parquet file statistics
    #[inline]
    fn from_statistics(file_stats: Statistics) -> Vec<Option<Self>> {
        let row_count = file_stats.num_rows.get_value().map(|v| *v as u64);
        file_stats
            .column_statistics
            .into_iter()
            .map(move |col_stat| {
                if let (Some(min), Some(max)) = (
                    col_stat.min_value.get_value(),
                    col_stat.max_value.get_value(),
                ) {
                    let null_count =
                        ScalarValue::UInt64(col_stat.null_count.get_value().map(|v| *v as u64));
                    let row_count = ScalarValue::UInt64(row_count);
                    Some(Self {
                        min: min.clone(),
                        max: max.clone(),
                        null_count,
                        row_count,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}
/// Index of columns to its ArrayRef stats of given files
#[derive(Debug)]
pub struct FilesIndex {
    // Table schema of the files
    table_schema: SchemaRef,

    // UUIDs of files in the index
    file_uuids: ArrayRef,

    // Vec of ColsStatsArrayRef in table schema order,
    //   each contains the min/max values for the list of files
    // Since this struct is created based on the given filters and given set of file ids,
    //   . The index is only available (not None) for column index of columns in the filters
    //   . ColsStatsArrayRef contains stats min, max, null count and row coount of all the corresponding given files for that column.
    //      In other words, ColsStatsArrayRef of different column will have the same length and the length of file_uuids. Each element
    //      represents stats of the column on corresponding file of file_uuids
    col_stat_index: Vec<Option<ColStatsArrayRef>>,
}

impl FilesIndex {
    /// Create a new index for the given files
    pub async fn new(
        table_schema: SchemaRef,
        filters: &[Expr],
        file_uuids: &[ObjectStoreId],
        meta_cache: Arc<MetaIndexCache>,
    ) -> Result<Self, DataFusionError> {
        // Columns in the filters
        let mut columns = HashSet::new();
        filters
            .iter()
            .for_each(|expr| expr.add_column_refs(&mut columns));

        // Build array ref for file_uuids
        let file_uuids_array = build_file_uuids_array(file_uuids)?;

        // Index of columns to its ColStatsArrayRef of given files
        // initialize with None for all columns of the table schema
        let num_table_col = table_schema.fields().len();
        let mut col_stat_index: Vec<Option<ColStatsArrayRef>> = Vec::with_capacity(num_table_col);
        for _ in 0..num_table_col {
            col_stat_index.push(None);
        }
        for col in columns {
            let Ok(col_index) = table_schema.index_of(&col.name) else {
                // column not found in the table schema, skip getting its stats
                panic!("Column {col} not found in the table schema");
            };

            let col_stats = meta_cache
                .build_array_ref_for_a_column_on_files(file_uuids, col_index)
                .await;
            if let Ok(col_stats) = col_stats {
                col_stat_index[col_index] = Some(col_stats);
            }
        }

        Ok(Self {
            table_schema,
            col_stat_index,
            file_uuids: file_uuids_array,
        })
    }

    /// Prune the given files based on the given filters
    /// Return a mask of all files with trues are for files to keep.
    /// The returned mask is a mapping to self.file_uuids. Mask`\[`i``\]` is true if
    /// self.file_uuids`\[`i`\]` has column data covering the filters
    pub fn mark_prune_files(&self, filters: &[Expr]) -> Result<Vec<bool>, DataFusionError> {
        // convert filters like [`a = 1`, `b = 2`] to a single filter like `a = 1 AND b = 2`
        let filter_expr = conjunction(filters.to_vec());

        let Some(filter_expr) = filter_expr else {
            // no filters, return all files
            return Ok(vec![true; self.file_uuids.len()]);
        };

        // Build a pruning predicate from the filter expression
        let props = ExecutionProps::new();
        let predicate_expr =
            create_physical_expr_from_schema(&props, &filter_expr, &self.table_schema)?;
        let pruning_predicate =
            PruningPredicate::try_new(predicate_expr, Arc::clone(&self.table_schema))?;

        // Prune the files
        let mask = pruning_predicate.prune(self)?;

        Ok(mask)
    }
}

impl PruningStatistics for FilesIndex {
    /// Return min stats of the column
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let Ok(col_index) = self.table_schema.index_of(column.name()) else {
            return None;
        };

        self.col_stat_index[col_index]
            .as_ref()
            .map(|col_stats| Arc::clone(&col_stats.mins))
    }

    /// Return max stats of the column
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let Ok(col_index) = self.table_schema.index_of(column.name()) else {
            return None;
        };

        self.col_stat_index[col_index]
            .as_ref()
            .map(|col_stats| Arc::clone(&col_stats.maxes))
    }

    /// return the number of "containers". In this case,each "container" is
    /// a file (aka a row in the index)
    fn num_containers(&self) -> usize {
        self.file_uuids.len()
    }

    /// Return `None` to signal we don't have any information about null
    /// counts in the index,
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let Ok(col_index) = self.table_schema.index_of(column.name()) else {
            return None;
        };

        self.col_stat_index[col_index]
            .as_ref()
            .map(|col_stats| Arc::clone(&col_stats.null_counts))
    }

    /// Return `None` to signal we don't have any information about row counts in the index,
    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        let Ok(col_index) = self.table_schema.index_of(column.name()) else {
            return None;
        };

        self.col_stat_index[col_index]
            .as_ref()
            .map(|col_stats| Arc::clone(&col_stats.row_counts))
    }

    /// The `contained` API can be used with structures such as Bloom filters,
    /// but is not used in this case
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow_util::assert_batches_eq;
    use bytes::Bytes;
    use datafusion::{
        common::{ColumnStatistics, Statistics, stats::Precision},
        parquet::{
            arrow::arrow_reader::ArrowReaderOptions,
            errors::ParquetError,
            file::metadata::FileMetaData,
            schema::types::{SchemaDescriptor, Type},
        },
    };
    use futures::future::BoxFuture;
    use uuid::Uuid;

    struct MockFileReader(Arc<ParquetMetaData>);
    impl MockFileReader {
        fn new(metadata: ParquetMetaData) -> Self {
            Self(Arc::new(metadata))
        }
    }

    impl AsyncFileReader for MockFileReader {
        fn get_bytes(&mut self, _range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
            Box::pin(async move { unimplemented!() })
        }

        fn get_byte_ranges(
            &mut self,
            _ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
            Box::pin(async move { unimplemented!() })
        }

        fn get_metadata<'a>(
            &'a mut self,
            _options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
            Box::pin(async move { Ok(Arc::clone(&self.0)) })
        }
    }

    // A default ParquetMetaData for tests that do not care about the specific content of the
    // cached metadata.
    fn dummy_parquet_metadata() -> ParquetMetaData {
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("name").build().unwrap(),
        )));
        ParquetMetaData::new(
            FileMetaData::new(1234, 1234, None, None, schema_desc, None),
            Vec::new(),
        )
    }

    // Assert content of the cache hit and missed metrics
    async fn assert_metrics(
        meta_index: &MetaIndexCache,
        col_cache_hits: u64,
        col_cache_misses: u64,
    ) {
        assert_eq!(
            meta_index.num_requests_with_stats().await.fetch(),
            col_cache_hits
        );
        assert_eq!(
            meta_index.num_requests_without_stats().await.fetch(),
            col_cache_misses
        );
    }

    // Assert number of files pruned
    async fn assert_files_pruned_metrics(meta_index: &MetaIndexCache, num_files: u64) {
        assert_eq!(meta_index.num_files_pruned().await.fetch(), num_files);
    }

    // Caches the given file metadata for testing
    async fn cache_metadata(
        meta_index: &MetaIndexCache,
        file_uuid: &ObjectStoreId,
        parquet_1: impl Into<Arc<ParquetMetaData>> + Send + 'static,
        file_stats_1: Statistics,
    ) -> Arc<FileMetas> {
        let (res, _state) = meta_index
            .file_index
            .get_or_fetch(
                file_uuid,
                Box::new(|| {
                    async move {
                        Ok(Arc::new(FileMetas {
                            parquet_metadata: parquet_1.into(),
                            col_metas: Some(ColStats::from_statistics(file_stats_1)),
                        }))
                    }
                    .boxed()
                }),
            )
            .await;
        res.unwrap()
    }

    #[tokio::test]
    async fn test_file_meta_index() {
        // empty file meta index
        let meta_index = cache();

        // initial metric values
        let mut col_cache_hit = 0;
        let mut col_cache_missed = 0;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // get stats of a file that does not exist
        let file_1 = ObjectStoreId::from_uuid(Uuid::from_u128(1));
        assert_eq!(meta_index.get_file_stats(&file_1), None);
        let parquet_1 = Arc::new(dummy_parquet_metadata());
        // add stats for file 1 with 3 columns
        let (_schema_1, file_stats_1, expected_col_stats_1) = create_df_stats_3_columns();
        cache_metadata(
            &meta_index,
            &file_1,
            Arc::clone(&parquet_1),
            file_stats_1.clone(),
        )
        .await;
        // length of the file index should be 1
        assert_eq!(meta_index.len(), 1);
        // content of the index for file 1
        assert_eq!(
            meta_index.get_file_stats(&file_1).unwrap().col_metas,
            Some(expected_col_stats_1.clone())
        );

        // add the same file stats again and check if it is not added
        cache_metadata(&meta_index, &file_1, parquet_1, file_stats_1).await;
        assert_eq!(meta_index.len(), 1);
        // the stats of file 1 should be the same
        assert_eq!(
            meta_index.get_file_stats(&file_1).unwrap().col_metas,
            Some(expected_col_stats_1)
        );

        // add stats for file 2 with 4 columns (one more column from the previous file)
        let file_2 = ObjectStoreId::from_uuid(Uuid::from_u128(2));
        let parquet_2 = Arc::new(dummy_parquet_metadata());
        let (_schema_2, file_stats_2, expected_col_stats_2) = create_df_stats_4_columns();
        cache_metadata(&meta_index, &file_2, parquet_2, file_stats_2).await;
        // length of the file index should be 2
        assert_eq!(meta_index.len(), 2);
        // content of the index for file 2
        assert_eq!(
            meta_index.get_file_stats(&file_2).unwrap().col_metas,
            Some(expected_col_stats_2)
        );

        // check metrics of col cache hit and miss: still nothing because we have not asked for stats yet
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // check stats of file 1 & file_2 on the first column, c1
        let files = [file_1, file_2];
        let cold_idx = 0;
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000001 | 1   | 10  | 0          | 100       |",
                "| 00000000000000000000000000000002 | 100 | 110 | 0          | 100       |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;

        // check metrics: one column is asked for stats on 2 files with stats
        // --> . col_cache_hit increases by 2
        //     . col_cache_miss stays the same
        col_cache_hit += 2;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // check stats of second column, c2 of absent & unknonw data type, of both files
        let files = [file_1, file_2];
        let cold_idx = 1;
        // Both of them have no stats for the second column
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000001 |     |     |            |           |",
                "| 00000000000000000000000000000002 |     |     |            |           |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;
        //
        // check metrics: one column is asked for stats on 2 files without stats
        // --> num_requests_without_stats increases by 2
        col_cache_missed += 2;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // check stats of second column, c2 of absent & unknonw data type, of second file.
        // The second file has no stats for the second column
        let files = [file_2];
        let cold_idx = 1;
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000002 |     |     |            |           |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;
        //
        // check metrics: one column is asked for stats on 1 file without stats
        // --> num_requests_without_stats increases by 1
        col_cache_missed += 1;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // Check stats for third column, c3 of null utf8, of first file
        let files = [file_1];
        let cold_idx = 2;
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000001 |     |     |            | 100       |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;
        //
        // check metrics: one column is asked for stats on 1 file with stats
        // --> col_cache_hit increases by 1
        col_cache_hit += 1;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // Check stats for forth column, c4 of utf8, of both files
        let files = [file_1, file_2];
        let cold_idx = 3;
        // Only file 2 has stats for the forth column
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000001 |     |     |            |           |",
                "| 00000000000000000000000000000002 | a   | b   | 0          | 100       |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;
        //
        // check metrics: one column is asked for stats on 2 files one with stats and in without stats
        // --> both col_cache_hit and col_cache_miss increases by 1
        col_cache_hit += 1;
        col_cache_missed += 1;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // Check stats for second column of an unknown file
        let unknonw_file_uuid = ObjectStoreId::from_uuid(Uuid::from_u128(100));
        let files = [unknonw_file_uuid];
        let cold_idx = 1;
        // no stats for that file
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000064 |     |     |            |           |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;
        //
        // check metrics: one column is asked for stats on 1 file not exist in the index cache (=no stats)
        // --> col_cache_missed increases by 1
        col_cache_missed += 1;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;

        // Check stats for an unknown column of a known file
        let files = [unknonw_file_uuid];
        let cold_idx = 10;
        // no stats for that file
        build_array_refs_for_column(
            &meta_index,
            cold_idx,
            &files,
            vec![
                "+----------------------------------+-----+-----+------------+-----------+",
                "| file_uuid                        | min | max | null_count | row_count |",
                "+----------------------------------+-----+-----+------------+-----------+",
                "| 00000000000000000000000000000064 |     |     |            |           |",
                "+----------------------------------+-----+-----+------------+-----------+",
            ],
        )
        .await;

        //
        // check metrics
        // one column is asked for stats on 1 file not exist in the index cache (=no stats)
        // --> col_cache_miss increases by 1
        col_cache_missed += 1;
        assert_metrics(&meta_index, col_cache_hit, col_cache_missed).await;
    }
    #[test]
    fn test_file_meta_stats() {
        // stats for file 1 with 3 columns
        let (_schema_ref, file_stats_1, expected_col_stats_1) = create_df_stats_3_columns();
        let col_stats = ColStats::from_statistics(file_stats_1);
        // content of the index for file 1
        assert_eq!(col_stats, expected_col_stats_1);

        // add stats for file 2 with 4 columns (one more column from the previous file)
        let (_schema_ref, file_stats_2, expected_col_stats_2) = create_df_stats_4_columns();
        let col_stats = ColStats::from_statistics(file_stats_2);
        // content of the index for file 2
        assert_eq!(col_stats, expected_col_stats_2);
    }

    #[tokio::test]
    async fn test_prune_files() {
        // empty file meta index
        let meta_index = cache();

        // add stats for file 1 with 3 columns
        let file_1 = ObjectStoreId::from_uuid(Uuid::from_u128(1));
        // specific metadata content doesn't matter
        let parquet_1 = dummy_parquet_metadata();
        let (schema_1, _file_stats_1, _expected_col_stats_1) = create_df_stats_3_columns();
        let _ = meta_index
            .add_metadata_for_file(&file_1, schema_1, MockFileReader::new(parquet_1), None)
            .await;

        // add stats for file 2 with 4 columns (one more column from the previous file)
        let file_2 = ObjectStoreId::from_uuid(Uuid::from_u128(2));
        let (schema_2, _file_stats_2, _expected_col_stats_2) = create_df_stats_4_columns();
        // specific metadata content doesn't matter
        let parquet_2 = dummy_parquet_metadata();
        let _ = meta_index
            .add_metadata_for_file(&file_2, schema_2, MockFileReader::new(parquet_2), None)
            .await;
        // length of the file index should be 2
        assert_eq!(meta_index.len(), 2);

        // record one file pruned
        let mut total_files_pruned = 0;
        let pruned_files = 1;
        meta_index.record_pruned_files(pruned_files).await;
        // check the number of files pruned
        total_files_pruned += pruned_files;
        assert_files_pruned_metrics(&meta_index, total_files_pruned).await;

        // record 2 files pruned
        let pruned_files = 2;
        meta_index.record_pruned_files(pruned_files).await;
        // check the number of files pruned
        total_files_pruned += pruned_files;
        assert_files_pruned_metrics(&meta_index, total_files_pruned).await;
    }

    // ----------- Test helper

    // create DF Statistics and its respective ColStats (min & max) for 3 columns
    // c1: Int64[1, 10]
    // c2: [absent, absent] and unknown data type
    // c3: Utf8[null, null]
    fn create_df_stats_3_columns() -> (SchemaRef, Statistics, Vec<Option<ColStats>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
        ]));

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
                    sum_value: Precision::Absent,
                },
                // column without stats
                ColumnStatistics {
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                // column with null stats
                ColumnStatistics {
                    min_value: Precision::Exact(null_utf8.clone()),
                    max_value: Precision::Exact(null_utf8),
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };

        // Corresponsinding ColStatsnof those 3 columns
        let cols_stats = vec![
            Some(ColStats {
                min: ScalarValue::Int64(Some(1)),
                max: ScalarValue::Int64(Some(10)),
                null_count: ScalarValue::UInt64(Some(0)),
                row_count: ScalarValue::UInt64(Some(100)),
            }),
            None, // absent stats
            Some(ColStats {
                min: ScalarValue::Utf8(None),          // min is null
                max: ScalarValue::Utf8(None),          // max is null
                null_count: ScalarValue::UInt64(None), // null count is null
                row_count: ScalarValue::UInt64(Some(100)),
            }),
        ];

        (schema, df_stats, cols_stats)
    }

    // create DF Statistics and its respective ColStats for 4 columns
    // c1: Int64[100, 110]
    // c2: [absent, absent] and unknown data type
    // c3: Utf8[null, null]
    // c4: Utf8["a", "b"]
    fn create_df_stats_4_columns() -> (SchemaRef, Statistics, Vec<Option<ColStats>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Utf8, false),
        ]));

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
                    sum_value: Precision::Absent,
                },
                // column without stats
                ColumnStatistics {
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                // column with null stats
                ColumnStatistics {
                    min_value: Precision::Exact(null_utf8.clone()),
                    max_value: Precision::Exact(null_utf8),
                    null_count: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                // column with stats
                ColumnStatistics {
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("a".to_string()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_string()))),
                    null_count: Precision::Exact(0),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };

        // Corresponsinding ColStats of those 4 columns
        let cols_stats = vec![
            Some(ColStats {
                min: ScalarValue::Int64(Some(100)),
                max: ScalarValue::Int64(Some(110)),
                null_count: ScalarValue::UInt64(Some(0)),
                row_count: ScalarValue::UInt64(Some(100)),
            }),
            None, // absent stats
            Some(ColStats {
                min: ScalarValue::Utf8(None),          // min is null
                max: ScalarValue::Utf8(None),          // max is null
                null_count: ScalarValue::UInt64(None), // null count is null
                row_count: ScalarValue::UInt64(Some(100)),
            }),
            Some(ColStats {
                min: ScalarValue::Utf8(Some("a".to_string())),
                max: ScalarValue::Utf8(Some("b".to_string())),
                null_count: ScalarValue::UInt64(Some(0)),
                row_count: ScalarValue::UInt64(Some(100)),
            }),
        ];

        (schema, df_stats, cols_stats)
    }

    async fn build_array_refs_for_column(
        meta_index: &MetaIndexCache,
        col_idx: usize,
        files: &[ObjectStoreId],
        expected: Vec<&str>,
    ) {
        let record_batch = meta_index
            .build_array_ref_for_a_column_on_files(files, col_idx)
            .await
            .unwrap()
            .to_record_batch(files);
        assert_batches_eq!(expected, &[record_batch]);
    }

    fn cache() -> MetaIndexCache {
        MetaIndexCacheParams {
            memory_limit: NonZeroUsize::new(100_000).unwrap(),
            s3_fifo_ghost_memory_limit: NonZeroUsize::new(100_000).unwrap(),
            s3fifo_main_threshold: 20,
            metrics: &metric::Registry::new(),
            cache_column_stats: true,
        }
        .build()
    }
}
