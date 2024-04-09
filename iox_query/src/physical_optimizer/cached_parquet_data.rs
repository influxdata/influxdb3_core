use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::physical_plan::{
        FileMeta, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
    },
    error::DataFusionError,
    parquet::{
        arrow::async_reader::{AsyncFileReader, MetadataLoader},
        errors::ParquetError,
        file::metadata::ParquetMetaData,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{metrics::ExecutionPlanMetricsSet, ExecutionPlan},
};
use futures::{future::Shared, prelude::future::BoxFuture, FutureExt};
use object_store::{DynObjectStore, Error as ObjectStoreError, ObjectMeta};
use once_cell::sync::Lazy;

use crate::provider::PartitionedFileExt;

#[derive(Debug, Default)]
pub struct CachedParquetData;

impl PhysicalOptimizerRule for CachedParquetData {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_up(&|plan| {
            // feature enabled?
            if !*USE_CACHED_PARQUET_LOADING {
                return Ok(Transformed::no(plan));
            }

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

            let parquet_exec = parquet_exec
                .clone()
                .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                    Arc::clone(&ext.object_store),
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
}

impl CachedParquetFileReaderFactory {
    /// Create new factory based on the given object store.
    pub(crate) fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self { object_store }
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
        let data = async move {
            let res = object_store.get(&location).await.map_err(Arc::new)?;
            res.bytes().await.map_err(Arc::new)
        }
        .boxed()
        .shared();

        Ok(Box::new(CachedFileReader {
            meta: file_meta.object_meta,
            file_metrics,
            metadata_size_hint,
            data,
        }))
    }
}

/// A [`AsyncFileReader`] that fetches file data only onces.
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// This is an implementation detail of [`CachedParquetFileReaderFactory`]
struct CachedFileReader {
    meta: ObjectMeta,
    file_metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
    data: Shared<BoxFuture<'static, Result<Bytes, Arc<ObjectStoreError>>>>,
}

impl AsyncFileReader for CachedFileReader {
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
                    self.file_metrics.bytes_scanned.add(range.len());
                    Ok(data.slice(range.clone()))
                })
                .collect()
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        Box::pin(async move {
            let preload_column_index = true;
            let preload_offset_index = true;
            let file_size = self.meta.size;
            let prefetch = self.metadata_size_hint;
            let mut loader = MetadataLoader::load(self, file_size, prefetch).await?;
            loader
                .load_page_index(preload_column_index, preload_offset_index)
                .await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}

/// Hackish feature switch for easier experiments.
///
/// TODO(marco): do this properly.
static USE_CACHED_PARQUET_LOADING: Lazy<bool> = Lazy::new(|| {
    std::env::var("INFLUXDB_IOX_CACHED_PARQUET_LOADER")
        .map(|s| s.to_lowercase() == "true" || s == "1")
        .unwrap_or_default()
});
