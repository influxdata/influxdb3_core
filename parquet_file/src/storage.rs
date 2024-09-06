//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError, ParallelParquetWriterOptions},
    ParquetFilePath,
};
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use data_types::TransitionPartitionId;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{parquet::ParquetExecBuilder, FileScanConfig},
    },
    error::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics},
    prelude::SessionContext,
};
use datafusion_util::config::{
    iox_session_config, register_iox_object_store, table_parquet_options,
};
use object_store::{DynObjectStore, ObjectMeta, PutPayload};
use observability_deps::tracing::*;
use schema::Projection;
use std::{
    fmt::Display,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;

/// Errors returned during a Parquet "put" operation, covering [`RecordBatch`]
/// pull from the provided stream, encoding, and finally uploading the bytes to
/// the object store.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug, Error)]
pub enum UploadError {
    /// A codec failure during serialisation.
    #[error(transparent)]
    Serialise(#[from] CodecError),

    /// An error during Parquet metadata conversion when attempting to
    /// instantiate a valid [`IoxParquetMetaData`] instance.
    #[error("failed to construct IOx parquet metadata: {0}")]
    Metadata(crate::metadata::Error),

    /// Uploading the Parquet file to object store failed.
    #[error("failed to upload to object storage: {0}")]
    Upload(#[from] object_store::Error),

    /// Error in configuration.
    #[error("failed to properly configure parquet upload: {0}")]
    Config(#[from] DataFusionError),
}

impl From<UploadError> for DataFusionError {
    fn from(value: UploadError) -> Self {
        match value {
            UploadError::Serialise(e) => {
                Self::Context(String::from("serialize"), Box::new(e.into()))
            }
            UploadError::Metadata(e) => Self::External(Box::new(e)),
            UploadError::Upload(e) => Self::ObjectStore(e),
            UploadError::Config(e) => Self::External(e.into()),
        }
    }
}

/// ID for an object store hooked up into DataFusion.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StorageId(&'static str);

impl From<&'static str> for StorageId {
    fn from(id: &'static str) -> Self {
        Self(id)
    }
}

impl AsRef<str> for StorageId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl std::fmt::Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Inputs required to build a [`ParquetExec`] for one or multiple files.
///
/// The files shall be grouped by [`object_store_url`](Self::object_store_url). For each each object store, you shall
/// create one [`ParquetExec`] and put each file into its own "file group".
///
/// [`ParquetExec`]: datafusion::datasource::physical_plan::ParquetExec
#[derive(Debug, Clone)]
pub struct ParquetExecInput {
    /// Store where the file is located.
    pub object_store_url: ObjectStoreUrl,

    /// The actual store referenced by [`object_store_url`](Self::object_store_url).
    pub object_store: Arc<DynObjectStore>,

    /// Object metadata.
    pub object_meta: ObjectMeta,
}

impl ParquetExecInput {
    /// Read parquet file into [`RecordBatch`]es.
    ///
    /// This should only be used for testing purposes.
    pub async fn read_to_batches(
        &self,
        schema: SchemaRef,
        projection: Projection<'_>,
        session_ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        // Compute final (output) schema after selection
        let schema = Arc::new(projection.project_schema(&schema).as_ref().clone());
        let statistics = Statistics::new_unknown(&schema);
        let base_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: schema,
            file_groups: vec![vec![PartitionedFile {
                object_meta: self.object_meta.clone(),
                partition_values: vec![],
                range: None,
                extensions: None,
                statistics: None,
            }]],
            statistics,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            // Parquet files ARE actually sorted but we don't care here since we just construct a `collect` plan.
            output_ordering: vec![],
        };
        let builder = ParquetExecBuilder::new_with_options(base_config, table_parquet_options());
        let exec = builder.build();
        let exec_schema = exec.schema();
        datafusion::physical_plan::collect(Arc::new(exec), session_ctx.task_ctx())
            .await
            .inspect(|batches| {
                for batch in batches {
                    assert_eq!(batch.schema(), exec_schema);
                }
            })
    }
}

/// Inputs required for write upload.
///
/// Eventually these may be consumed separately when we
/// separate encoding from upload.
#[derive(Debug, Clone)]
pub struct ParquetUploadInput {
    // Store where the file is located.
    object_store_url: ObjectStoreUrl,

    // Path within the store.
    object_store_path: object_store::path::Path,

    /// The actual store referenced by [`object_store_url`](Self::object_store_url).
    pub object_store: Arc<DynObjectStore>,
}

impl ParquetUploadInput {
    fn try_new(
        partition_id: &TransitionPartitionId,
        meta: &IoxMetadata,
        id: &StorageId,
        object_store: Arc<DynObjectStore>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            object_store_url: ObjectStoreUrl::parse(format!("iox://{}/", id))?,
            object_store_path: ParquetFilePath::from((partition_id, meta)).object_store_path(),
            object_store,
        })
    }

    pub(crate) fn path(&self) -> &object_store::path::Path {
        &self.object_store_path
    }

    pub(crate) fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }
}

/// The [`ParquetStorage`] type encapsulates [`RecordBatch`] persistence to an
/// underlying [`ObjectStore`].
///
/// [`RecordBatch`] instances are serialized to Parquet files, with IOx specific
/// metadata ([`IoxParquetMetaData`]) attached.
///
/// Code that interacts with Parquet files in object storage should utilise this
/// type that encapsulates the storage & retrieval implementation.
///
/// [`ObjectStore`]: object_store::ObjectStore
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug, Clone)]
pub struct ParquetStorage {
    /// Underlying object store.
    object_store: Arc<DynObjectStore>,

    /// Storage ID to hook it into DataFusion.
    id: StorageId,

    /// If some, then parallelized parquet writes will occur.
    parquet_write_parallelization_settings: Option<ParallelParquetWriterOptions>,
}

impl Display for ParquetStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetStorage(id={:?}, object_store={}",
            self.id, self.object_store
        )
    }
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>, id: StorageId) -> Self {
        Self {
            object_store,
            id,
            parquet_write_parallelization_settings: None,
        }
    }

    /// Enable parquet write parallelism, and set the amount
    /// of parallelization per row group and per column.
    pub fn with_enabled_parallel_writes(
        self,
        num_row_group_writers: NonZeroUsize,
        num_column_writers_across_row_groups: NonZeroUsize,
    ) -> Self {
        Self {
            parquet_write_parallelization_settings: Some(ParallelParquetWriterOptions {
                maximum_parallel_row_group_writers: num_row_group_writers.into(),
                maximum_buffered_record_batches_per_stream: num_column_writers_across_row_groups
                    .into(),
            }),
            ..self
        }
    }

    /// Get underlying object store.
    pub fn object_store(&self) -> &Arc<DynObjectStore> {
        &self.object_store
    }

    /// Get ID.
    pub fn id(&self) -> StorageId {
        self.id
    }

    /// Fake DataFusion context for testing that contains this store
    pub fn test_df_context(&self) -> SessionContext {
        // set up "fake" DataFusion session
        let object_store = Arc::clone(&self.object_store);
        let session_ctx = SessionContext::new_with_config(iox_session_config());
        register_iox_object_store(session_ctx.runtime_env(), self.id, object_store);
        session_ctx
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// Any buffering needed is registered with the pool provided by the [`RuntimeEnv`]. The
    /// runtime itself is utilized if parallelized writes are enabled.
    ///
    /// # Retries
    ///
    /// This method retries forever in the presence of object store errors. All
    /// other errors are returned as they occur.
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    pub async fn upload(
        &self,
        batches: SendableRecordBatchStream,
        partition_id: &TransitionPartitionId,
        meta: &IoxMetadata,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<(IoxParquetMetaData, usize), UploadError> {
        if let Some(parallel_writer_options) = &self.parquet_write_parallelization_settings {
            let write_input = ParquetUploadInput::try_new(
                partition_id,
                meta,
                &self.id(),
                Arc::clone(&self.object_store),
            )?;

            return self
                .parallel_upload(
                    batches,
                    write_input,
                    meta,
                    runtime,
                    parallel_writer_options.to_owned(),
                )
                .await;
        };

        let start = Instant::now();

        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) =
            serialize::to_parquet_bytes(batches, meta, Arc::clone(&runtime.memory_pool)).await?;
        let num_rows = parquet_file_meta.num_rows;

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        trace!(
            ?parquet_meta,
            "IoxParquetMetaData converted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from((partition_id, meta)).object_store_path();

        let file_size = data.len();
        let data = Bytes::from(data);
        let playload = PutPayload::from_bytes(data);

        debug!(
            file_size,
            object_store_id=?meta.object_store_id,
            // includes the time to run the datafusion plan (that is the batches)
            total_time_to_create_parquet_bytes=?(Instant::now() - start),
            "Uploading parquet to object store"
        );

        // Retry uploading the file endlessly.
        //
        // This is abort-able by the user by dropping the upload() future.
        //
        // Cloning `data` is a ref count inc, rather than a data copy.
        let mut retried = false;
        while let Err(e) = self.object_store.put(&path, playload.clone()).await {
            warn!(error=%e, ?meta, "failed to upload parquet file to object storage, retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
            retried = true;
        }

        if retried {
            info!(
                ?meta,
                "Succeeded uploading files to object storage on retry"
            );
        }

        debug!(
            %num_rows,
            %file_size,
            %path,
            // includes the time to run the datafusion plan (that is the batches) & upload
            total_time_to_create_and_upload_parquet=?(Instant::now() - start),
            "Created and uploaded parquet file"
        );

        Ok((parquet_meta, file_size))
    }

    async fn parallel_upload(
        &self,
        batches: SendableRecordBatchStream,
        upload_input: ParquetUploadInput,
        meta: &IoxMetadata,
        runtime: Arc<RuntimeEnv>,
        parallel_writer_options: ParallelParquetWriterOptions,
    ) -> Result<(IoxParquetMetaData, usize), UploadError> {
        let start = Instant::now();

        let parquet_file_meta = serialize::to_parquet_upload(
            batches,
            meta,
            upload_input.clone(),
            runtime,
            parallel_writer_options,
        )
        .await?;
        let num_rows = parquet_file_meta.num_rows;

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        trace!(
            ?parquet_meta,
            "IoxParquetMetaData converted from Row Group Metadata (aka FileMetaData)"
        );

        let file_size = self.object_store.head(upload_input.path()).await?.size;

        debug!(
            %num_rows,
            %file_size,
            path=?upload_input.path(),
            // includes the time to run the datafusion plan (that is the batches) & upload
            total_time_to_create_and_upload_parquet=?(Instant::now() - start),
            "Created and uploaded parquet file (parallelized)"
        );

        Ok((parquet_meta, file_size))
    }

    /// Inputs for [`ParquetExec`].
    ///
    /// See [`ParquetExecInput`] for more information.
    ///
    /// [`ParquetExec`]: datafusion::datasource::physical_plan::ParquetExec
    pub fn parquet_exec_input(&self, path: &ParquetFilePath, file_size: usize) -> ParquetExecInput {
        ParquetExecInput {
            object_store_url: ObjectStoreUrl::parse(format!("iox://{}/", self.id))
                .expect("valid object store URL"),
            object_store: Arc::clone(&self.object_store),
            object_meta: ObjectMeta {
                location: path.object_store_path(),
                // we don't care about the "last modified" field
                last_modified: Default::default(),
                size: file_size,
                e_tag: None,
                version: None,
            },
        }
    }
}

/// Error during projecting parquet file data to an expected schema.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ProjectionError {
    /// Unknown field.
    #[error("Unknown field: {0}")]
    UnknownField(String),

    /// Field type mismatch
    #[error("Type mismatch, expected {expected:?} but got {actual:?}")]
    FieldTypeMismatch {
        /// Expected field.
        expected: Field,

        /// Actual field.
        actual: Field,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    };
    use data_types::{CompactionLevel, NamespaceId, ObjectStoreId, PartitionId, TableId};
    use datafusion::common::{DataFusionError, ScalarValue};
    use datafusion_util::{config::BATCH_SIZE, unbounded_memory_pool, MemoryStream};
    use iox_time::Time;
    use schema::{Error::InvalidInfluxColumnType, Schema as IoxSchema, SchemaBuilder};
    use tokio::runtime::{Handle, RuntimeFlavor};

    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    async fn test_upload_metadata(store: ParquetStorage) {
        let (transition_partition_id, meta) = meta();
        let mut batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"; 8192 * 2])),
            ("b", to_int_array(&[1; 8192 * 2])),
            ("c", to_string_array(&["foo"; 8192 * 2])),
            ("d", to_int_array(&[2; 8192 * 2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 8192 * 2);

        // schema on batch is not valid iox schema
        let iox_schema = IoxSchema::try_from(batch.schema());
        assert!(
            matches!(iox_schema, Err(InvalidInfluxColumnType { .. })),
            "should not have iox schema, yet"
        );

        // build & use iox_schema
        let iox_schema = SchemaBuilder::new()
            .influx_field("a", schema::InfluxFieldType::String)
            .influx_field("b", schema::InfluxFieldType::Integer)
            .influx_field("c", schema::InfluxFieldType::String)
            .influx_field("d", schema::InfluxFieldType::Integer)
            .build()
            .expect("valid iox schema");
        batch = batch
            .with_schema(iox_schema.clone().into())
            .expect("should update batch with iox schema");

        // Serialize & upload the record batches.
        let (encoded_file_meta, _file_size) =
            upload(&store, &transition_partition_id, &meta, batch.clone()).await;

        // Extract the various bits of metadata.
        let file_meta = encoded_file_meta
            .decode()
            .expect("should decode parquet metadata");

        let got_iox_meta = file_meta
            .read_iox_metadata_new()
            .expect("should read IOx metadata from parquet meta");

        // Ensure the metadata in the file decodes to the same IOx metadata we
        // provided when uploading.
        assert_eq!(got_iox_meta, meta);

        // Ensure gets the IOx Schema from the parquet metadata
        assert!(
            matches!(file_meta.read_schema(), Ok(encoded_schema) if encoded_schema == iox_schema),
            "should have the parquet encoded schema match the schema provided to upload"
        );
    }

    #[tokio::test]
    async fn test_normal_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        test_upload_metadata(store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn test_parallelized_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        // test with parallel column writers
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(6).unwrap(), // parallelized only column writing
            );
        test_upload_metadata(store).await;

        // test with parallel rowgroup writers
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(2).unwrap(), // parallelized only rowgroup writing
                NonZeroUsize::new(1).unwrap(),
            );
        test_upload_metadata(store).await;

        // test with both parallelized column and rowgroup writers
        let store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(6).unwrap(),
            );
        test_upload_metadata(store).await;
    }

    /// Perform parquet encoding and upload, while monitoring
    /// the currently active tokio task count.
    async fn run_upload_and_count_max_active_tasks(
        store: ParquetStorage,
        batch: RecordBatch,
    ) -> Arc<AtomicUsize> {
        // prepare schema
        let schema = batch.schema();

        // background watcher, counting the num of active tasks
        // this uses a tokio stable API in RuntimeMetrics
        let counter = Arc::new(AtomicUsize::new(0));
        let captured_counter = Arc::clone(&counter);
        let counter_handle = Handle::current().spawn(async move {
            loop {
                let num_active = Handle::current().metrics().num_alive_tasks(); // tokio stable api
                captured_counter.fetch_max(num_active, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        });

        // Test: start upload on current thread
        perform_assert_roundtrip(store, batch.clone(), Projection::All, schema, batch).await;
        counter_handle.abort();

        // provide counter for test assertions
        counter
    }

    /// Verify that using the ArrowWriter (our non-parallel writing
    /// implementation) uses a single thread, even when multiple threads
    /// are available in the tokio threadpool.
    ///
    /// The number of tasks is a proxy for the number of threads,
    /// since tokio distributes tasks across its threadpool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_default_upload_is_single_threaded() {
        // confirm test runtime is multi-threaded
        assert_eq!(
            RuntimeFlavor::MultiThread,
            Handle::current().runtime_flavor()
        );

        // store with default (single threaded) upload
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        // Test: single task, even when many columns
        let counter =
            run_upload_and_count_max_active_tasks(store.clone(), batch_with_many_cols_and_1_row())
                .await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "should be only 1 tasks, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );

        // Test: single task, even when many rows
        let counter =
            run_upload_and_count_max_active_tasks(store, batch_with_many_rows_and_1_col()).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "should be only 1 tasks, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );
    }

    /// Verify using datafusion writer is using more than a single task to
    /// encode parquet files. The number of tasks is used as a proxy for
    /// the speed of writing parquet files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_parallel_parquet_sink_uses_multiple_tasks() {
        use tokio::runtime::{Handle, RuntimeFlavor};

        // confirm test runtime is multi-threaded
        assert_eq!(
            RuntimeFlavor::MultiThread,
            Handle::current().runtime_flavor()
        );

        // store with multi-threaded upload
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(1).unwrap(), // only 1
                NonZeroUsize::new(1).unwrap(), // only 1
            );

        // Test: we should be using multiple tasks
        // w/ the many-cols-in-1-row
        let counter =
            run_upload_and_count_max_active_tasks(store.clone(), batch_with_many_cols_and_1_row())
                .await;
        assert!(
            counter.load(Ordering::SeqCst) > 1,
            "should have >1 task, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_simple_roundtrip() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();

        assert_roundtrip(batch.clone(), Projection::All, schema, batch).await;
    }

    #[tokio::test]
    async fn test_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Projection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_selection_unknown() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 1);
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([("b", to_int_array(&[1]))]).unwrap();
        assert_roundtrip(batch, Projection::Some(&["b", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        assert_eq!(file_batch.num_columns(), 2);
        assert_eq!(file_batch.num_rows(), 1);

        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        assert_roundtrip(file_batch, Projection::All, schema, schema_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order_with_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);

        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Projection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_types() {
        let batch = RecordBatch::try_from_iter([("a", to_interval_array(&[123456]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[123456]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Error during planning: Cannot cast file schema field a of type Int64 to table schema field of type Interval(MonthDayNano)",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_names() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("b", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: Invalid argument error: Column 'a' is declared as non-nullable but contains null values",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_unknown_column() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: Invalid argument error: Column 'b' is declared as non-nullable but contains null values",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(&store, &partition_id, &meta, batch).await;

        // add metadata to reference schema
        let schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );
        download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            schema,
            file_size,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        // add metadata to stored batch
        let batch = RecordBatch::try_new(
            Arc::new(
                schema
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
            ),
            batch.columns().to_vec(),
        )
        .unwrap();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(&store, &partition_id, &meta, batch).await;

        download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            schema,
            file_size,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_schema_arrow_metadata_preserved() {
        // Setup
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));
        let (partition_id, meta) = meta();

        // Create a basic record batch:
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();

        // Extend the created record batch with schema metadata:
        let schema = Arc::new(
            batch
                .schema()
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(
                    String::from("iox::measurement::name"),
                    String::from("foo"),
                )])),
        );

        // Recreate the batch with the added metadata in the schema:
        let upload_batch =
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()).unwrap();

        // Serialize & upload the record batches, then download it:
        let (_iox_md, file_size) = upload(&store, &partition_id, &meta, upload_batch.clone()).await;
        let downloaded_batch = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            schema,
            file_size,
        )
        .await
        .unwrap();

        // Check the uploaded and downloaded batches, including their schema, are equal.
        assert_eq!(upload_batch, downloaded_batch);
    }

    #[tokio::test]
    async fn test_schema_check_ignores_extra_column_in_file() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = expected_batch.schema();
        assert_roundtrip(file_batch, Projection::All, schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignores_type_for_unselected_column() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        assert_roundtrip(file_batch, Projection::Some(&["a"]), schema, expected_batch).await;
    }

    fn batch_with_many_cols_and_1_row() -> RecordBatch {
        let mut batch = Vec::with_capacity(10_000);
        for field in 0..10_000 {
            batch.push((field.to_string(), to_string_array(&["value"])));
        }
        let batch = RecordBatch::try_from_iter(batch).unwrap();

        assert_eq!(batch.num_columns(), 10_000);
        assert_eq!(batch.num_rows(), 1);
        batch
    }

    fn batch_with_many_rows_and_1_col() -> RecordBatch {
        let iox_session_batch_size = BATCH_SIZE;

        let col_1 = to_int_array(&[2; 1024 * 8]);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![col_1]).expect("created new record batch");

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), iox_session_batch_size);
        batch
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    /// Returns an IntervalMonthDayNano array with the given nanosecond values
    fn to_interval_array(vals: &[i64]) -> ArrayRef {
        ScalarValue::iter_to_array(vals.iter().map(|v| ScalarValue::new_interval_mdn(0, 0, *v)))
            .unwrap()
    }

    fn to_int_array(vals: &[i64]) -> ArrayRef {
        let array: Int64Array = vals.iter().map(|v| Some(*v)).collect();
        Arc::new(array)
    }

    fn meta() -> (TransitionPartitionId, IoxMetadata) {
        (
            TransitionPartitionId::Deprecated(PartitionId::new(4)),
            IoxMetadata {
                object_store_id: ObjectStoreId::new(),
                creation_timestamp: Time::from_timestamp_nanos(42),
                namespace_id: NamespaceId::new(1),
                namespace_name: "bananas".into(),
                table_id: TableId::new(3),
                table_name: "platanos".into(),
                partition_key: "potato".into(),
                compaction_level: CompactionLevel::FileNonOverlapped,
                sort_key: None,
                max_l0_created_at: Time::from_timestamp_nanos(42),
            },
        )
    }

    async fn upload(
        store: &ParquetStorage,
        partition_id: &TransitionPartitionId,
        meta: &IoxMetadata,
        batch: RecordBatch,
    ) -> (IoxParquetMetaData, usize) {
        let stream = Box::pin(MemoryStream::new(vec![batch]));
        let runtime = Arc::new(RuntimeEnv {
            memory_pool: unbounded_memory_pool(),
            ..Default::default()
        });
        datafusion_util::config::register_iox_object_store(
            &runtime,
            store.id(),
            Arc::clone(store.object_store()),
        );

        store
            .upload(stream, partition_id, meta, runtime)
            .await
            .expect("should serialize and store sucessfully")
    }

    async fn download<'a>(
        store: &ParquetStorage,
        partition_id: &TransitionPartitionId,
        meta: &IoxMetadata,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        file_size: usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let path: ParquetFilePath = (partition_id, meta).into();
        store
            .parquet_exec_input(&path, file_size)
            .read_to_batches(expected_schema, selection, &store.test_df_context())
            .await
            .map(|mut batches| {
                assert_eq!(batches.len(), 1);
                batches.remove(0)
            })
    }

    async fn assert_roundtrip(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        // test the single threaded path
        assert_roundtrip_single_thread(
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
        )
        .await;
        // test the multi threaded path
        assert_roundtrip_multi_thread(upload_batch, selection, expected_schema, expected_batch)
            .await;
    }

    async fn assert_roundtrip_single_thread(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"));

        perform_assert_roundtrip(
            store,
            upload_batch,
            selection,
            expected_schema,
            expected_batch,
        )
        .await;
    }

    async fn assert_roundtrip_multi_thread(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        // test with parallel column writer
        let parallel_store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(6).unwrap(), // parallelized only column writing
            );
        perform_assert_roundtrip(
            parallel_store,
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
        )
        .await;

        // test with parallel rowgroup writers
        let parallel_store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(6).unwrap(), // parallelized only rowgroup writing
                NonZeroUsize::new(1).unwrap(),
            );
        perform_assert_roundtrip(
            parallel_store,
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
        )
        .await;

        // test with both parallelized column and rowgroup writers
        let parallel_store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_enabled_parallel_writes(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::new(3).unwrap(),
            );
        perform_assert_roundtrip(
            parallel_store,
            upload_batch,
            selection,
            expected_schema,
            expected_batch,
        )
        .await;
    }

    async fn perform_assert_roundtrip(
        store: ParquetStorage,
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        // Serialize & upload the record batches.
        let (partition_id, meta) = meta();
        let (_iox_md, file_size) = upload(&store, &partition_id, &meta, upload_batch.clone()).await;

        // And compare to the original input
        let actual_batch = download(
            &store,
            &partition_id,
            &meta,
            selection,
            Arc::clone(&expected_schema),
            file_size,
        )
        .await
        .unwrap();
        assert_eq!(actual_batch, expected_batch);
    }

    async fn assert_schema_check_fail(
        persisted_batch: RecordBatch,
        expected_schema: SchemaRef,
        msg: &str,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();
        let (_iox_md, file_size) = upload(&store, &partition_id, &meta, persisted_batch).await;

        let err = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            expected_schema,
            file_size,
        )
        .await
        .unwrap_err();

        // And compare to the original input
        assert_eq!(err.to_string(), msg);
    }
}
