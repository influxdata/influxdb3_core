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
use tokio::{runtime::Handle, task::JoinSet};

use crate::{config::IoxConfigExt, provider::PartitionedFileExt};

#[derive(Debug, Default)]
pub struct CachedParquetData;

impl PhysicalOptimizerRule for CachedParquetData {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let enabled = config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default()
            .use_cached_parquet_loader;
        if !enabled {
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

            let parquet_exec = parquet_exec
                .clone()
                .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                    Arc::clone(&ext.object_store),
                    // use the same tokio runtime as planning for IO during execution
                    // (at the time of writing, this was the main/IO runtime
                    Handle::current(),
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
    runtime_handle: Handle,
}

impl CachedParquetFileReaderFactory {
    /// Create new factory based on the given object store.
    pub(crate) fn new(object_store: Arc<DynObjectStore>, runtime_handle: Handle) -> Self {
        Self {
            object_store,
            runtime_handle,
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
        let runtime_handle = self.runtime_handle.clone();
        let data = async move {
            let mut task = JoinSet::new();
            task.spawn_on(
                async move {
                    let res = object_store.get(&location).await.map_err(Arc::new)?;
                    res.bytes().await.map_err(Arc::new)
                },
                &runtime_handle,
            );
            task.join_next()
                .await
                .expect("just added task")
                .map_err(|e| Arc::new(ObjectStoreError::JoinError { source: e }))?
        }
        .boxed()
        .shared();

        Ok(Box::new(CachedFileReader {
            meta: Arc::new(file_meta.object_meta),
            file_metrics: Some(file_metrics),
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
    meta: Arc<ObjectMeta>,
    file_metrics: Option<ParquetFileMetrics>,
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
            // TODO(marco): preload metadata aggressively to make actual data loads cheaper
            //              DataFusion currently sets both to `false`
            let preload_column_index = false;
            let preload_offset_index = false;

            let file_size = self.meta.size;
            let prefetch = self.metadata_size_hint;

            // DataFusion excludes metadata loads from "bytes scanned"
            let mut this = Self {
                meta: Arc::clone(&self.meta),
                file_metrics: None,
                metadata_size_hint: self.metadata_size_hint,
                data: self.data.clone(),
            };

            let mut loader = MetadataLoader::load(&mut this, file_size, prefetch).await?;
            loader
                .load_page_index(preload_column_index, preload_offset_index)
                .await?;

            Ok(Arc::new(loader.finish()))
        })
    }
}
