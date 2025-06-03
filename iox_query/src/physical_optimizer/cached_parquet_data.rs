use std::{ops::Range, sync::Arc};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::physical_plan::{
        FileMeta, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
    },
    error::DataFusionError,
    parquet::{
        arrow::async_reader::AsyncFileReader, errors::ParquetError, file::metadata::ParquetMetaData,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{metrics::ExecutionPlanMetricsSet, ExecutionPlan},
};
use executor::spawn_io;
use futures::{future::Shared, prelude::future::BoxFuture, FutureExt};
use meta_data_cache::MetaIndexCache;
use object_store::{DynObjectStore, Error as ObjectStoreError, ObjectMeta};
use object_store_size_hinting::hint_size;
use observability_deps::tracing::warn;
use parquet_file::ParquetFilePath;

use crate::{
    config::{IoxCacheExt, IoxConfigExt},
    provider::PartitionedFileExt,
};

#[derive(Debug, Default)]
pub struct CachedParquetData;

impl PhysicalOptimizerRule for CachedParquetData {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let config_ext = config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default();
        if !config_ext.use_cached_parquet_loader {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() else {
                return Ok(Transformed::no(plan));
            };
            let base_config = parquet_exec.base_config();
            let mut files = base_config.file_groups.iter().flatten().peekable();

            if files.peek().is_none() {
                // no files
                return Ok(Transformed::no(plan));
            }

            // find object store
            let Some(ext) = files
                .next()
                .and_then(|f| f.extensions.as_ref())
                .and_then(|ext| ext.downcast_ref::<PartitionedFileExt>())
            else {
                return Err(DataFusionError::Plan("lost PartitionFileExt".to_owned()));
            };

            // Only querier has cache ext. The compactor does not have it.
            // It is useful for querier to cache the file metadat cache for furture file pruning
            // based on query predicates. The compactor reads file without predicates
            // and hence won't benefit from caching the file metadata.
            let iox_cache_ext = config.extensions.get::<IoxCacheExt>();
            let meta_cache = iox_cache_ext.and_then(|e| e.meta_cache.clone());

            let parquet_exec = parquet_exec
                .clone()
                .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                    Arc::clone(&ext.object_store),
                    Arc::clone(&ext.table_schema),
                    meta_cache,
                    config_ext.hint_known_object_size_to_object_store,
                )));
            Ok(Transformed::yes(Arc::new(parquet_exec)))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "cached_parquet_data"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A [`ParquetFileReaderFactory`] that fetches file data only onces.
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// Also supports the DataFusion [`ParquetFileMetrics`].
#[derive(Debug)]
struct CachedParquetFileReaderFactory {
    object_store: Arc<DynObjectStore>,
    table_schema: SchemaRef,
    meta_cache: Option<Arc<MetaIndexCache>>,
    hint_size_to_object_store: bool,
}

impl CachedParquetFileReaderFactory {
    /// Create new factory based on the given object store.
    pub(crate) fn new(
        object_store: Arc<DynObjectStore>,
        table_schema: SchemaRef,
        meta_cache: Option<Arc<MetaIndexCache>>,
        hint_size_to_object_store: bool,
    ) -> Self {
        Self {
            object_store,
            table_schema,
            meta_cache,
            hint_size_to_object_store,
        }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        let object_store = Arc::clone(&self.object_store);
        let location = file_meta.object_meta.location.clone();
        let size = file_meta.object_meta.size;
        let hint_size_to_object_store = self.hint_size_to_object_store;
        let data = spawn_io(async move {
            let options = if hint_size_to_object_store {
                hint_size(size)
            } else {
                Default::default()
            };
            let res = object_store
                .get_opts(&location, options)
                .await
                .map_err(Arc::new)?;
            res.bytes().await.map_err(Arc::new)
        })
        .boxed()
        .shared();
        let meta = Arc::new(file_meta.object_meta);

        let file_reader = ParquetFileReader {
            meta: Arc::clone(&meta),
            file_metrics: Some(file_metrics),
            metadata_size_hint,
            data,
        };

        // no meta cache available when this is executed by the compactor
        if let Some(ref meta_cache) = self.meta_cache {
            Ok(Box::new(CachedParquetFileReader {
                inner: file_reader,
                table_schema: Arc::clone(&self.table_schema),
                meta_cache: Arc::clone(meta_cache),
            }))
        } else {
            Ok(Box::new(file_reader))
        }
    }
}

/// A [`AsyncFileReader`] that fetches file data only onces.
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// This is an implementation detail of [`CachedParquetFileReaderFactory`]
#[derive(Debug)]
struct CachedParquetFileReader {
    inner: ParquetFileReader,
    table_schema: SchemaRef, // todo(nga): may want to replace this with tableCache
    meta_cache: Arc<MetaIndexCache>,
}

impl AsyncFileReader for CachedParquetFileReader {
    #[inline]
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        self.inner.get_bytes(range)
    }

    #[inline]
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        Box::pin(async move {
            if let Some(file_uuid) = ParquetFilePath::uuid_from_path(&self.inner.meta.location) {
                let file_reader = self.inner.clone_with_no_metrics();
                let file_metas = self
                    .meta_cache
                    .add_metadata_for_file(&file_uuid, Arc::clone(&self.table_schema), file_reader)
                    .await
                    // unfortunately there doesn't seem to be a way to downcast the Arc<DynError> into the
                    // `ParquetError` that DataFusion accepts, so just wrap it as an external error
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                if file_metas.col_metas.is_none() {
                    warn!(
                        "Failed to collect statistics for file: {:?}",
                        self.inner.meta.location
                    );
                }
                Ok(Arc::clone(&file_metas.parquet_metadata))
            } else {
                warn!(
                    "Unable to cache parquet metadata: Failed to find UUID from path: {:?}",
                    self.inner.meta.location
                );
                // no available UUID, try to fetch metadata without caching
                // NOTE(adam): does this make sense? maybe should throw an error instead?
                self.inner.get_metadata().await
            }
        })
    }
}

/// A [`AsyncFileReader`] that fetches file data each time it is invoked (no cache).
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// This is used as the inner implementation of [`CachedParquetFileReader`]
#[derive(Debug)]
struct ParquetFileReader {
    meta: Arc<ObjectMeta>,
    file_metrics: Option<ParquetFileMetrics>,
    metadata_size_hint: Option<usize>,
    data: Shared<BoxFuture<'static, Result<Bytes, Arc<ObjectStoreError>>>>,
}

impl ParquetFileReader {
    /// Creates a new [`ParquetFileReader`] for loading metadata.
    ///
    /// This is a "partial" clone, but omits `file_metrics` because Datafusion excludes metadata
    /// loads from the "bytes scanned" metrics
    #[inline]
    fn clone_with_no_metrics(&self) -> Self {
        Self {
            meta: Arc::clone(&self.meta),
            file_metrics: None,
            metadata_size_hint: self.metadata_size_hint,
            data: self.data.clone(),
        }
    }
    /// Loads [`ParquetMetaData`] from file.
    #[inline]
    async fn load_metadata(&mut self) -> Result<ParquetMetaData, ParquetError> {
        let prefetch = self.metadata_size_hint;
        let file_size = self.meta.size;
        ParquetMetaDataReader::new()
            .with_prefetch_hint(prefetch)
            .with_page_indexes(true)
            .load_and_finish(self, file_size)
            .await
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        Box::pin(async move {
            Ok(self
                .get_byte_ranges(vec![range])
                .await?
                .into_iter()
                .next()
                .expect("requested one range"))
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        Box::pin(async move {
            let data = self
                .data
                .clone()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            ranges
                .into_iter()
                .map(|range| {
                    if range.end > data.len() {
                        return Err(ParquetError::IndexOutOfBound(range.end, data.len()));
                    }
                    if range.start > range.end {
                        return Err(ParquetError::IndexOutOfBound(range.start, range.end));
                    }
                    if let Some(file_metrics) = &self.file_metrics {
                        file_metrics.bytes_scanned.add(range.len());
                    }
                    Ok(data.slice(range))
                })
                .collect()
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        Box::pin(async move {
            Ok(Arc::new(
                self.clone_with_no_metrics().load_metadata().await?,
            ))
        })
    }
}
