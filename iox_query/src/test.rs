//! This module provides a reference implementation of [`QueryNamespace`] for use in testing.
//!
//! AKA it is a Mock

use crate::{
    exec::{
        stringset::{StringSet, StringSetRef},
        Executor, IOxSessionContext, QueryConfig,
    },
    pruning::prune_chunks,
    query_log::{QueryLog, StateReceived},
    QueryChunk, QueryChunkData, QueryCompletedToken, QueryNamespace, QueryNamespaceProvider,
    QueryText,
};
use arrow::array::{BooleanArray, Float64Array};
use arrow::datatypes::SchemaRef;
use arrow::{
    array::{
        ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type, TimeUnit},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, NamespaceId, PartitionKey, TableId, TransitionPartitionId};
use datafusion::common::stats::Precision;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{catalog::schema::SchemaProvider, logical_expr::LogicalPlan};
use datafusion::{catalog::CatalogProvider, physical_plan::displayable};
use datafusion::{
    datasource::{object_store::ObjectStoreUrl, TableProvider, TableType},
    physical_plan::{ColumnStatistics, Statistics as DataFusionStatistics},
    scalar::ScalarValue,
};
use datafusion_util::{config::DEFAULT_SCHEMA, option_to_precision, timestamptz_nano};
use iox_query_params::StatementParams;
use iox_time::SystemProvider;
use itertools::Itertools;
use object_store::{path::Path, ObjectMeta};
use parking_lot::Mutex;
use parquet_file::storage::ParquetExecInput;
use schema::{
    builder::SchemaBuilder, merge::SchemaMerger, sort::SortKey, Schema, TIME_COLUMN_NAME,
};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt,
    num::NonZeroU64,
    sync::Arc,
};
use trace::{ctx::SpanContext, span::Span};
use tracker::{AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit};

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
    pub metric_registry: Arc<metric::Registry>,
    pub query_semaphore: Arc<tracker::InstrumentedAsyncSemaphore>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_semaphore_size(semaphore_size: usize) -> Self {
        let metric_registry = Arc::new(metric::Registry::default());
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metric_registry,
            &[("semaphore", "query_execution")],
        ));
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new_testing()),
            metric_registry,
            query_semaphore: Arc::new(semaphore_metrics.new_semaphore(semaphore_size)),
        }
    }

    pub async fn db_or_create(&self, name: &str) -> Arc<TestDatabase> {
        let mut databases = self.databases.lock();

        if let Some(db) = databases.get(name) {
            Arc::clone(db)
        } else {
            let new_db = Arc::new(TestDatabase::new(Arc::clone(&self.executor)));
            databases.insert(name.to_string(), Arc::clone(&new_db));
            new_db
        }
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self::new_with_semaphore_size(u16::MAX as usize)
    }
}

#[async_trait]
impl QueryNamespaceProvider for TestDatabaseStore {
    /// Retrieve the database specified name
    async fn db(
        &self,
        name: &str,
        _span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        let databases = self.databases.lock();

        Ok(databases.get(name).cloned().map(|ns| ns as _))
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_semaphore)
            .acquire_owned(span)
            .await
            .unwrap()
    }
}

#[derive(Debug)]
pub struct TestDatabase {
    executor: Arc<Executor>,
    /// Partitions which have been saved to this test database
    /// Key is partition name
    /// Value is map of chunk_id to chunk
    partitions: Mutex<BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>>,

    /// `column_names` to return upon next request
    column_names: Arc<Mutex<Option<StringSetRef>>>,

    /// The predicate passed to the most recent call to `chunks()`
    chunks_predicate: Mutex<Vec<Expr>>,

    /// Retention time ns.
    retention_time_ns: Option<i64>,
}

impl TestDatabase {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            executor,
            partitions: Default::default(),
            column_names: Default::default(),
            chunks_predicate: Default::default(),
            retention_time_ns: None,
        }
    }

    /// Add a test chunk to the database
    pub fn add_chunk(&self, partition_key: &str, chunk: Arc<TestChunk>) -> &Self {
        let mut partitions = self.partitions.lock();
        let chunks = partitions.entry(partition_key.to_string()).or_default();
        chunks.insert(chunk.id(), chunk);
        self
    }

    /// Add a test chunk to the database
    pub fn with_chunk(self, partition_key: &str, chunk: Arc<TestChunk>) -> Self {
        self.add_chunk(partition_key, chunk);
        self
    }

    /// Get the specified chunk
    pub fn get_chunk(&self, partition_key: &str, id: ChunkId) -> Option<Arc<TestChunk>> {
        self.partitions
            .lock()
            .get(partition_key)
            .and_then(|p| p.get(&id).cloned())
    }

    /// Return the most recent predicate passed to get_chunks()
    pub fn get_chunks_predicate(&self) -> Vec<Expr> {
        self.chunks_predicate.lock().clone()
    }

    /// Set the list of column names that will be returned on a call to
    /// column_names
    pub fn set_column_names(&self, column_names: Vec<String>) {
        let column_names = column_names.into_iter().collect::<StringSet>();
        let column_names = Arc::new(column_names);

        *Arc::clone(&self.column_names).lock() = Some(column_names)
    }

    /// Set retention time.
    pub fn with_retention_time_ns(mut self, retention_time_ns: Option<i64>) -> Self {
        self.retention_time_ns = retention_time_ns;
        self
    }
}

#[async_trait]
impl QueryNamespace for TestDatabase {
    async fn chunks(
        &self,
        table_name: &str,
        filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        // save last predicate
        *self.chunks_predicate.lock() = filters.to_vec();

        let partitions = self.partitions.lock().clone();
        Ok(partitions
            .values()
            .flat_map(|x| x.values())
            // filter by table
            .filter(|c| c.table_name == table_name)
            // only keep chunks if their statistics overlap
            .filter(|c| {
                prune_chunks(
                    c.schema(),
                    &[Arc::clone(*c) as Arc<dyn QueryChunk>],
                    filters,
                )
                .ok()
                .map(|res| res[0])
                .unwrap_or(true)
            })
            .map(|x| Arc::clone(x) as Arc<dyn QueryChunk>)
            .collect::<Vec<_>>())
    }

    fn retention_time_ns(&self) -> Option<i64> {
        self.retention_time_ns
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
    ) -> QueryCompletedToken<StateReceived> {
        QueryLog::new(0, Arc::new(SystemProvider::new())).push(
            NamespaceId::new(1),
            Arc::from("ns"),
            query_type,
            query_text,
            query_params,
            span_ctx.map(|s| s.trace_id),
        )
    }

    fn new_query_context(
        &self,
        span_ctx: Option<SpanContext>,
        query_config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        // Note: unlike Db this does not register a catalog provider
        let mut cmd = self
            .executor
            .new_execution_config()
            .with_default_catalog(Arc::new(TestDatabaseCatalogProvider::from_test_database(
                self,
            )))
            .with_span_context(span_ctx);
        if let Some(query_config) = query_config {
            cmd = cmd.with_query_config(query_config);
        }
        cmd.build()
    }
}

struct TestDatabaseCatalogProvider {
    partitions: BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>,
}

impl TestDatabaseCatalogProvider {
    fn from_test_database(db: &TestDatabase) -> Self {
        Self {
            partitions: db.partitions.lock().clone(),
        }
    }
}

impl CatalogProvider for TestDatabaseCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(TestDatabaseSchemaProvider {
                partitions: self.partitions.clone(),
            })),
            _ => None,
        }
    }
}

struct TestDatabaseSchemaProvider {
    partitions: BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>,
}

#[async_trait]
impl SchemaProvider for TestDatabaseSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.partitions
            .values()
            .flat_map(|c| c.values())
            .map(|c| c.table_name.to_owned())
            .unique()
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(Some(Arc::new(TestDatabaseTableProvider {
            partitions: self
                .partitions
                .values()
                .flat_map(|chunks| chunks.values().filter(|c| c.table_name() == name))
                .map(Clone::clone)
                .collect(),
        })))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }
}

struct TestDatabaseTableProvider {
    partitions: Vec<Arc<TestChunk>>,
}

#[async_trait]
impl TableProvider for TestDatabaseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.partitions
            .iter()
            .fold(SchemaMerger::new(), |merger, chunk| {
                merger.merge(chunk.schema()).expect("consistent schemas")
            })
            .build()
            .as_arrow()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> crate::exec::context::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
enum TestChunkData {
    RecordBatches(Vec<RecordBatch>),
    Parquet(ParquetExecInput),
}

#[derive(Debug, Clone)]
pub struct TestChunk {
    /// Table name
    table_name: String,

    /// Schema of the table
    schema: Schema,

    /// Values for stats()
    column_stats: HashMap<String, ColumnStatistics>,
    num_rows: Option<usize>,

    id: ChunkId,

    partition_id: TransitionPartitionId,

    /// Set the flag if this chunk might contain duplicates
    may_contain_pk_duplicates: bool,

    /// Data in this chunk.
    table_data: TestChunkData,

    /// A saved error that is returned instead of actual results
    saved_error: Option<String>,

    /// Order of this chunk relative to other overlapping chunks.
    order: ChunkOrder,

    /// The sort key of this chunk
    sort_key: Option<SortKey>,

    /// Suppress output
    quiet: bool,
}

/// Implements a method for adding a column with default stats
macro_rules! impl_with_column {
    ($NAME:ident, $DATA_TYPE:ident) => {
        pub fn $NAME(self, column_name: impl Into<String>) -> Self {
            let column_name = column_name.into();

            let new_column_schema = SchemaBuilder::new()
                .field(&column_name, DataType::$DATA_TYPE)
                .unwrap()
                .build()
                .unwrap();
            self.add_schema_to_table(new_column_schema, None)
        }
    };
}

/// Implements a method for adding a column with stats that have the specified min and max
macro_rules! impl_with_column_with_stats {
    ($NAME:ident, $DATA_TYPE:ident, $RUST_TYPE:ty, $STAT_TYPE:ident) => {
        pub fn $NAME(
            self,
            column_name: impl Into<String>,
            min: Option<$RUST_TYPE>,
            max: Option<$RUST_TYPE>,
        ) -> Self {
            let column_name = column_name.into();

            let new_column_schema = SchemaBuilder::new()
                .field(&column_name, DataType::$DATA_TYPE)
                .unwrap()
                .build()
                .unwrap();

            let stats = ColumnStatistics {
                null_count: Precision::Absent,
                max_value: option_to_precision(max.map(|s| ScalarValue::from(s))),
                min_value: option_to_precision(min.map(|s| ScalarValue::from(s))),
                distinct_count: Precision::Absent,
            };

            self.add_schema_to_table(new_column_schema, Some(stats))
        }
    };
}

impl TestChunk {
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        Self {
            table_name,
            schema: SchemaBuilder::new().build().unwrap(),
            column_stats: Default::default(),
            num_rows: None,
            id: ChunkId::new_test(0),
            may_contain_pk_duplicates: Default::default(),
            table_data: TestChunkData::RecordBatches(vec![]),
            saved_error: Default::default(),
            order: ChunkOrder::MIN,
            sort_key: None,
            partition_id: TransitionPartitionId::arbitrary_for_testing(),
            quiet: false,
        }
    }

    fn push_record_batch(&mut self, batch: RecordBatch) {
        match &mut self.table_data {
            TestChunkData::RecordBatches(batches) => {
                batches.push(batch);
            }
            TestChunkData::Parquet(_) => panic!("chunk is parquet-based"),
        }
    }

    pub fn with_order(self, order: i64) -> Self {
        Self {
            order: ChunkOrder::new(order),
            ..self
        }
    }

    pub fn with_dummy_parquet_file(self) -> Self {
        self.with_dummy_parquet_file_and_store("iox://store")
    }

    pub fn with_dummy_parquet_file_and_size(self, size: usize) -> Self {
        self.with_dummy_parquet_file_and_store_and_size("iox://store", size)
    }

    pub fn with_dummy_parquet_file_and_store(self, store: &str) -> Self {
        self.with_dummy_parquet_file_and_store_and_size(store, 1)
    }

    pub fn with_dummy_parquet_file_and_store_and_size(self, store: &str, size: usize) -> Self {
        match self.table_data {
            TestChunkData::RecordBatches(batches) => {
                assert!(batches.is_empty(), "chunk already has record batches");
            }
            TestChunkData::Parquet(_) => panic!("chunk already has a file"),
        }

        Self {
            table_data: TestChunkData::Parquet(ParquetExecInput {
                object_store_url: ObjectStoreUrl::parse(store).unwrap(),
                object_store: Arc::new(object_store::memory::InMemory::new()),
                object_meta: ObjectMeta {
                    location: Self::parquet_location(self.id),
                    last_modified: Default::default(),
                    size,
                    e_tag: None,
                    version: None,
                },
            }),
            ..self
        }
    }

    fn parquet_location(chunk_id: ChunkId) -> Path {
        Path::parse(format!("{}.parquet", chunk_id.get().as_u128())).unwrap()
    }

    /// Returns the receiver configured to suppress any output to STDOUT.
    pub fn with_quiet(mut self) -> Self {
        self.quiet = true;
        self
    }

    pub fn with_id(mut self, id: u128) -> Self {
        self.id = ChunkId::new_test(id);

        if let TestChunkData::Parquet(parquet_input) = &mut self.table_data {
            parquet_input.object_meta.location = Self::parquet_location(self.id);
        }

        self
    }

    pub fn with_partition(mut self, id: i64) -> Self {
        self.partition_id =
            TransitionPartitionId::new(TableId::new(id), &PartitionKey::from("arbitrary"));
        self
    }

    pub fn with_partition_id(mut self, id: TransitionPartitionId) -> Self {
        self.partition_id = id;
        self
    }

    /// specify that any call should result in an error with the message
    /// specified
    pub fn with_error(mut self, error_message: impl Into<String>) -> Self {
        self.saved_error = Some(error_message.into());
        self
    }

    /// Checks the saved error, and returns it if any, otherwise returns OK
    fn check_error(&self) -> Result<(), DataFusionError> {
        if let Some(message) = self.saved_error.as_ref() {
            Err(DataFusionError::External(message.clone().into()))
        } else {
            Ok(())
        }
    }

    /// Set the `may_contain_pk_duplicates` flag
    pub fn with_may_contain_pk_duplicates(mut self, v: bool) -> Self {
        self.may_contain_pk_duplicates = v;
        self
    }

    /// Register a tag column with the test chunk with default stats
    pub fn with_tag_column(self, column_name: impl Into<String>) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
    ) -> Self {
        self.with_tag_column_with_full_stats(column_name, min, max, 0, None)
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_full_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
        count: u64,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        let null_count = 0;
        self.with_tag_column_with_nulls_and_full_stats(
            column_name,
            min,
            max,
            count,
            distinct_count,
            null_count,
        )
    }

    fn update_count(&mut self, count: usize) {
        match self.num_rows {
            Some(existing) => assert_eq!(existing, count),
            None => self.num_rows = Some(count),
        }
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_nulls_and_full_stats(
        mut self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
        count: u64,
        distinct_count: Option<NonZeroU64>,
        null_count: u64,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Exact(null_count as usize),
            max_value: option_to_precision(max.map(ScalarValue::from)),
            min_value: option_to_precision(min.map(ScalarValue::from)),
            distinct_count: option_to_precision(distinct_count.map(|c| c.get() as usize)),
        };

        self.update_count(count as usize);
        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Register a timestamp column with the test chunk with default stats
    pub fn with_time_column(self) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register a timestamp column with the test chunk
    pub fn with_time_column_with_stats(self, min: Option<i64>, max: Option<i64>) -> Self {
        self.with_time_column_with_full_stats(min, max, 0, None)
    }

    /// Register a timestamp column with full stats with the test chunk
    pub fn with_time_column_with_full_stats(
        mut self,
        min: Option<i64>,
        max: Option<i64>,
        count: u64,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();
        let null_count = 0;

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Exact(null_count as usize),
            max_value: option_to_precision(max.map(timestamptz_nano)),
            min_value: option_to_precision(min.map(timestamptz_nano)),
            distinct_count: option_to_precision(distinct_count.map(|c| c.get() as usize)),
        };

        self.update_count(count as usize);
        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    pub fn with_timestamp_min_max(mut self, min: i64, max: i64) -> Self {
        let stats = self
            .column_stats
            .get_mut(TIME_COLUMN_NAME)
            .expect("stats in sync w/ columns");

        stats.min_value = Precision::Exact(timestamptz_nano(min));
        stats.max_value = Precision::Exact(timestamptz_nano(max));

        self
    }

    impl_with_column!(with_i64_field_column, Int64);
    impl_with_column_with_stats!(with_i64_field_column_with_stats, Int64, i64, I64);

    impl_with_column!(with_u64_column, UInt64);
    impl_with_column_with_stats!(with_u64_field_column_with_stats, UInt64, u64, U64);

    impl_with_column!(with_f64_field_column, Float64);
    impl_with_column_with_stats!(with_f64_field_column_with_stats, Float64, f64, F64);

    impl_with_column!(with_bool_field_column, Boolean);
    impl_with_column_with_stats!(with_bool_field_column_with_stats, Boolean, bool, Bool);

    /// Register a string field column with the test chunk
    pub fn with_string_field_column_with_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new()
            .field(&column_name, DataType::Utf8)
            .unwrap()
            .build()
            .unwrap();

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: option_to_precision(max.map(ScalarValue::from)),
            min_value: option_to_precision(min.map(ScalarValue::from)),
            distinct_count: Precision::Absent,
        };

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Adds the specified schema and optionally a column summary containing optional stats.
    /// If `add_column_summary` is false, `stats` is ignored. If `add_column_summary` is true but
    /// `stats` is `None`, default stats will be added to the column summary.
    fn add_schema_to_table(
        mut self,
        new_column_schema: Schema,
        input_stats: Option<ColumnStatistics>,
    ) -> Self {
        let mut merger = SchemaMerger::new();
        merger = merger.merge(&new_column_schema).unwrap();
        merger = merger.merge(&self.schema).expect("merging was successful");
        self.schema = merger.build();

        for f in new_column_schema.inner().fields() {
            self.column_stats.insert(
                f.name().clone(),
                input_stats.as_ref().cloned().unwrap_or_default(),
            );
        }

        self
    }

    /// Prepares this chunk to return a specific record batch with one
    /// row of non null data.
    /// tag: MA
    pub fn with_one_row_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![1000])) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(vec!["MA"])) as ArrayRef,
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![1000]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dict: DictionaryArray<Int32Type> = vec!["MA"].into_iter().collect();
                    Arc::new(dict) as ArrayRef
                }
                DataType::Float64 => Arc::new(Float64Array::from(vec![99.5])) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");
        if !self.quiet {
            println!("TestChunk batch data: {batch:#?}");
        }

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with a single tag, field and timestamp like
    pub fn with_one_row_of_specific_data(
        mut self,
        tag_val: impl AsRef<str>,
        field_val: i64,
        ts_val: i64,
    ) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![field_val])) as ArrayRef,
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![ts_val]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dict: DictionaryArray<Int32Type> =
                        vec![tag_val.as_ref()].into_iter().collect();
                    Arc::new(dict) as ArrayRef
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");
        if !self.quiet {
            println!("TestChunk batch data: {batch:#?}");
        }

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with three
    /// rows of non null data that look like, no duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| WA   | SC   | 1000      | 1970-01-01 00:00:00.000008    |",
    ///   "| VT   | NC   | 10        | 1970-01-01 00:00:00.000010    |",
    ///   "| UT   | RI   | 70        | 1970-01-01 00:00:00.000020    |",
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(UT, WA), tag2(RI, SC), time(8000, 20000)
    pub fn with_three_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000, 10, 70])) as ArrayRef,
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![1000, 10, 70])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec!["WA", "VT", "UT"])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec!["SC", "NC", "RI"])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec!["TX", "PR", "OR"])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![8000, 10000, 20000])
                        .with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["WA", "VT", "UT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["SC", "NC", "RI"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["TX", "PR", "OR"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with four
    /// rows of non null data that look like, duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| WA   | SC   | 1000      | 1970-01-01 00:00:00.000028    |",
    ///   "| VT   | NC   | 10        | 1970-01-01 00:00:00.000210    |", (1)
    ///   "| UT   | RI   | 70        | 1970-01-01 00:00:00.000220    |",
    ///   "| VT   | NC   | 50        | 1970-01-01 00:00:00.000210    |", // duplicate of (1)
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(UT, WA), tag2(RI, SC), time(28000, 220000)
    pub fn with_four_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000, 10, 70, 50])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec!["WA", "VT", "UT", "VT"])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec!["SC", "NC", "RI", "NC"])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec!["TX", "PR", "OR", "AL"])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![28000, 210000, 220000, 210000])
                        .with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["WA", "VT", "UT", "VT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["SC", "NC", "RI", "NC"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["TX", "PR", "OR", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with five
    /// rows of non null data that look like, no duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
    ///   "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |",
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
    ///   "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |",
    ///   "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |",
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(AL, MT), tag2(AL, MA), time(5, 7000)
    pub fn with_five_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => {
                    Arc::new(Int64Array::from(vec![1000, 10, 70, 100, 5])) as ArrayRef
                }
                DataType::Utf8 => {
                    match field.name().as_str() {
                        "tag1" => Arc::new(StringArray::from(vec!["MT", "MT", "CT", "AL", "MT"]))
                            as ArrayRef,
                        "tag2" => Arc::new(StringArray::from(vec!["CT", "AL", "CT", "MA", "AL"]))
                            as ArrayRef,
                        _ => Arc::new(StringArray::from(vec!["CT", "MT", "AL", "AL", "MT"]))
                            as ArrayRef,
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![1000, 7000, 100, 50, 5000])
                        .with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["MT", "MT", "CT", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["CT", "AL", "CT", "MA", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["CT", "MT", "AL", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with ten
    /// rows of non null data that look like, duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
    ///   "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |", (1)
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
    ///   "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |", (2)
    ///   "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |", (3)
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000002    |",
    ///   "| MT   | AL   | 20        | 1970-01-01 00:00:00.000007    |",  // Duplicate with (1)
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000500 |",
    ///   "| AL   | MA   | 10        | 1970-01-01 00:00:00.000000050 |",  // Duplicate with (2)
    ///   "| MT   | AL   | 30        | 1970-01-01 00:00:00.000005    |",  // Duplicate with (3)
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(AL, MT), tag2(AL, MA), time(5, 7000)
    pub fn with_ten_rows_of_data_some_duplicates(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![
                    1000, 10, 70, 100, 5, 1000, 20, 70, 10, 30,
                ])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec![
                        "MT", "MT", "CT", "AL", "MT", "MT", "MT", "CT", "AL", "MT",
                    ])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec![
                        "CT", "AL", "CT", "MA", "AL", "CT", "AL", "CT", "MA", "AL",
                    ])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec![
                        "CT", "MT", "AL", "AL", "MT", "CT", "MT", "AL", "AL", "MT",
                    ])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![
                        1000, 7000, 100, 50, 5, 2000, 7000, 500, 50, 5,
                    ])
                    .with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["MT", "MT", "CT", "AL", "MT", "MT", "MT", "CT", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["CT", "AL", "CT", "MA", "AL", "CT", "AL", "CT", "MA", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["CT", "MT", "AL", "AL", "MT", "CT", "MT", "AL", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
        self
    }

    /// Set the sort key for this chunk
    pub fn with_sort_key(self, sort_key: SortKey) -> Self {
        Self {
            sort_key: Some(sort_key),
            ..self
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl fmt::Display for TestChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}

impl QueryChunk for TestChunk {
    fn stats(&self) -> Arc<DataFusionStatistics> {
        self.check_error().unwrap();

        Arc::new(DataFusionStatistics {
            num_rows: option_to_precision(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: self
                .schema
                .inner()
                .fields()
                .iter()
                .map(|f| self.column_stats.get(f.name()).cloned().unwrap_or_default())
                .collect(),
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.may_contain_pk_duplicates
    }

    fn data(&self) -> QueryChunkData {
        self.check_error().unwrap();

        match &self.table_data {
            TestChunkData::RecordBatches(batches) => {
                QueryChunkData::in_mem(batches.clone(), Arc::clone(self.schema.inner()))
            }
            TestChunkData::Parquet(input) => QueryChunkData::Parquet(input.clone()),
        }
    }

    fn chunk_type(&self) -> &str {
        "Test Chunk"
    }

    fn order(&self) -> ChunkOrder {
        self.order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Return the raw data from the list of chunks
pub async fn raw_data(chunks: &[Arc<dyn QueryChunk>]) -> Vec<RecordBatch> {
    let ctx = IOxSessionContext::with_testing();
    let mut batches = vec![];
    for c in chunks {
        batches.append(&mut c.data().read_to_batches(c.schema(), ctx.inner()).await);
    }
    batches
}

pub fn format_logical_plan(plan: &LogicalPlan) -> Vec<String> {
    format_lines(&plan.display_indent().to_string())
}

pub fn format_execution_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    format_lines(&displayable(plan.as_ref()).indent(false).to_string())
}

fn format_lines(s: &str) -> Vec<String> {
    s.trim()
        .split('\n')
        .map(|s| {
            // Always add a leading space to ensure tha all lines in the YAML insta snapshots are quoted, otherwise the
            // alignment gets messed up and the snapshot would be hard to read.
            format!(" {s}")
        })
        .collect()
}
