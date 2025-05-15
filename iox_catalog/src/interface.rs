//! Traits and data types for the IOx Catalog API.

use async_trait::async_trait;
use data_types::{
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, NamespaceWithStorage, ObjectStoreId,
    ParquetFile, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, TableWithStorage, Timestamp,
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::{
        namespace::NamespaceSnapshot, partition::PartitionSnapshot, root::RootSnapshot,
        table::TableSnapshot,
    },
};
use generated_types::influxdata::iox::common::v1::SoftDeleted as ProtoSoftDeleted;
use iox_time::{AsyncTimeProvider, TimeProvider};
use snafu::Snafu;
use std::{
    any::Any,
    collections::HashSet,
    num::{NonZero, NonZeroUsize},
    time::Duration,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use trace::ctx::SpanContext;

/// An error wrapper detailing the reason for a compare-and-swap failure.
#[derive(Debug)]
pub enum CasFailure<T> {
    /// The compare-and-swap failed because the current value differers from the
    /// comparator.
    ///
    /// Contains the new current value.
    ValueMismatch(T),
    /// A query error occurred.
    QueryError(Error),
}

impl<T> std::fmt::Display for CasFailure<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueMismatch(_) => write!(f, "value mismatch"),
            Self::QueryError(e) => write!(f, "query error: {e}"),
        }
    }
}

/// Errors returned to the caller of catalog. All variants other than `Unhandled` may be something
/// the caller can address; an `Unhandled` error likely indiciates a bug in some component called
/// by the catalog.
#[derive(Clone, Debug, Snafu)]
#[expect(missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("unhandled: {source}"))]
    Unhandled { source: UnhandledError },

    #[snafu(display("already exists: {descr}"))]
    AlreadyExists { descr: String },

    #[snafu(display("limit exceeded: {descr}"))]
    LimitExceeded { descr: String },

    #[snafu(display("not found: {descr}"))]
    NotFound { descr: String },

    #[snafu(display("malformed request: {descr}"))]
    Malformed { descr: String },

    #[snafu(display("not implemented: {descr}"))]
    NotImplemented { descr: String },
}

/// Errors the catalog can't handle that get logged and propagated to the caller.
#[derive(Clone, Debug, Snafu)]
#[expect(missing_docs)]
pub enum UnhandledError {
    #[snafu(display("sqlx error: {source}"))]
    Sqlx { source: Arc<sqlx::Error> },

    #[snafu(display("prost decode error: {source}"))]
    ProstDecode {
        source: generated_types::prost::DecodeError,
    },

    #[snafu(display("gRPC serialization error: {source}"))]
    GrpcSerialization {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC request error: {source}"))]
    GrpcRequest {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("snapshot error: {source}"))]
    CatalogServiceSnapshot {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("quorum error: {source}"))]
    Quorum {
        source: Arc<catalog_cache::api::quorum::Error>,
    },

    #[snafu(display("cache handler error: {source}"))]
    CacheHandler {
        source: Arc<catalog_cache::api::quorum::Error>,
    },

    #[snafu(display("cache loader error: {source}"))]
    CacheLoader {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
}

impl From<UnhandledError> for Error {
    fn from(source: UnhandledError) -> Self {
        Self::Unhandled { source }
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        UnhandledError::Sqlx {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<sqlx::migrate::MigrateError> for Error {
    fn from(e: sqlx::migrate::MigrateError) -> Self {
        Self::from(sqlx::Error::from(e))
    }
}

impl From<data_types::snapshot::partition::Error> for Error {
    fn from(e: data_types::snapshot::partition::Error) -> Self {
        UnhandledError::CatalogServiceSnapshot {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<data_types::snapshot::table::Error> for Error {
    fn from(e: data_types::snapshot::table::Error) -> Self {
        UnhandledError::CatalogServiceSnapshot {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<data_types::snapshot::namespace::Error> for Error {
    fn from(e: data_types::snapshot::namespace::Error) -> Self {
        UnhandledError::CatalogServiceSnapshot {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<data_types::snapshot::root::Error> for Error {
    fn from(e: data_types::snapshot::root::Error) -> Self {
        UnhandledError::CatalogServiceSnapshot {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<data_types::ProtoV1AnyWithStorageError> for Error {
    fn from(e: data_types::ProtoV1AnyWithStorageError) -> Self {
        UnhandledError::GrpcSerialization {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<catalog_cache::api::quorum::Error> for Error {
    fn from(e: catalog_cache::api::quorum::Error) -> Self {
        UnhandledError::Quorum {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<generated_types::prost::DecodeError> for Error {
    fn from(e: generated_types::prost::DecodeError) -> Self {
        UnhandledError::ProstDecode { source: e }.into()
    }
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Specify how soft-deleted entities should affect query results.
///
/// ```text
///
///                ExcludeDeleted          OnlyDeleted
///
///                       ┃                     ┃
///                 .─────╋─────.         .─────╋─────.
///              ,─'      ┃      '─.   ,─'      ┃      '─.
///            ,'         ●         `,'         ●         `.
///          ,'                    ,' `.                    `.
///         ;                     ;     :                     :
///         │      No deleted     │     │   Only deleted      │
///         │         rows        │  ●  │       rows          │
///         :                     :  ┃  ;                     ;
///          ╲                     ╲ ┃ ╱                     ╱
///           `.                    `┃'                    ,'
///             `.                 ,'┃`.                 ,'
///               '─.           ,─'  ┃  '─.           ,─'
///                  `─────────'     ┃     `─────────'
///                                  ┃
///
///                               AllRows
///
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SoftDeletedRows {
    /// Return all rows.
    AllRows,

    /// Return all rows, except soft deleted rows.
    #[default]
    ExcludeDeleted,

    /// Return only soft deleted rows.
    OnlyDeleted,
}

impl SoftDeletedRows {
    /// Convert SoftDeletedRows to SQL predicate:
    /// - `ExcludeDeleted` -> `(alias.)deleted_at IS NULL`
    /// - `OnlyDeleted` -> `(alias.)deleted_at IS NOT NULL`
    /// - `AllRows` -> `1=1`
    pub(crate) fn as_sql_predicate(&self, column: Option<&str>) -> String {
        let prefix = column.map_or("".to_owned(), |c| format!("{c}."));
        match self {
            Self::ExcludeDeleted => format!("{prefix}deleted_at IS NULL"),
            Self::OnlyDeleted => format!("{prefix}deleted_at IS NOT NULL"),
            Self::AllRows => "1=1".to_owned(),
        }
    }
}

impl From<Option<i32>> for SoftDeletedRows {
    fn from(v: Option<i32>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::default(),
        }
    }
}

impl From<SoftDeletedRows> for i32 {
    fn from(val: SoftDeletedRows) -> Self {
        val as Self
    }
}

/// Convert from [`generated_types::influxdata::iox::common::v1::SoftDeleted`] to [`SoftDeletedRows`]
/// Note that SOFT_DELETED_UNSPECIFIED (0) defaults to [`SoftDeletedRows::ExcludeDeleted`]
impl From<i32> for SoftDeletedRows {
    fn from(v: i32) -> Self {
        match v {
            v if v == ProtoSoftDeleted::ListAll as i32 => Self::AllRows,
            v if v == ProtoSoftDeleted::OnlyActive as i32 => Self::ExcludeDeleted,
            v if v == ProtoSoftDeleted::OnlyDeleted as i32 => Self::OnlyDeleted,
            _ => Self::default(),
        }
    }
}

/// Possible ways that namespaces can be sorted. Used in the API.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NamespaceSortField {
    /// Sort by the namespace ID
    Id = 1,

    /// Sort by the namespace name
    Name = 2,

    /// Sort by the namespace retention period
    RetentionPeriod = 3,

    // Sort by total size of all non-deleted files in the namespace:
    // this is removed because of performance issues: all namespaces
    // need to be joined to get the size of all files in the namespace
    // and this is not efficient.
    // Storage = 4,
    /// Sort by the number of tables in the namespace
    TableCount = 5,
}

impl Default for NamespaceSortField {
    fn default() -> Self {
        Self::Id
    }
}

impl NamespaceSortField {
    /// Convert NamespaceSortField to SQL ORDER BY clause
    pub(crate) fn as_sql_column(&self, namespace_table_alias: Option<&str>) -> String {
        let namespace_table_alias_dot = namespace_table_alias.map(|alias| format!("{}.", alias));
        match self {
            Self::Id => format!("{}id", namespace_table_alias_dot.unwrap_or_default()),
            Self::Name => format!("{}name", namespace_table_alias_dot.unwrap_or_default()),
            Self::RetentionPeriod => format!(
                "{}retention_period_ns",
                namespace_table_alias_dot.unwrap_or_default()
            ),
            // table_count is a computed field, so we do not use the alias
            Self::TableCount => "table_count".to_string(),
        }
    }
}

impl TryFrom<Option<i32>> for NamespaceSortField {
    type Error = Error;
    fn try_from(v: Option<i32>) -> Result<Self, Error> {
        match v {
            Some(v) => Self::try_from(v),
            None => Err(Error::Malformed {
                descr: "missing sort field: will map to unsorted behavior".to_string(),
            }),
        }
    }
}

impl From<NamespaceSortField> for i32 {
    fn from(val: NamespaceSortField) -> Self {
        val as Self
    }
}

impl TryFrom<i32> for NamespaceSortField {
    type Error = Error;
    fn try_from(v: i32) -> Result<Self, Error> {
        Ok(match v {
            v if v == Self::Id as i32 => Self::Id,
            v if v == Self::Name as i32 => Self::Name,
            v if v == Self::RetentionPeriod as i32 => Self::RetentionPeriod,
            v if v == Self::TableCount as i32 => Self::TableCount,
            _ => {
                return Err(Error::Malformed {
                    descr: "invalid sort field".to_string(),
                });
            }
        })
    }
}

/// Possible directions for sorting
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortDirection {
    /// Sort ascending (default)
    Ascending = 1,
    /// Sort descending
    Descending = 2,
}

impl Default for SortDirection {
    fn default() -> Self {
        Self::Ascending
    }
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => write!(f, "ASC"),
            Self::Descending => write!(f, "DESC"),
        }
    }
}

impl From<SortDirection> for i32 {
    fn from(val: SortDirection) -> Self {
        val as Self
    }
}

impl TryFrom<i32> for SortDirection {
    type Error = Error;
    fn try_from(v: i32) -> Result<Self, Error> {
        Ok(match v {
            v if v == Self::Ascending as i32 => Self::Ascending,
            v if v == Self::Descending as i32 => Self::Descending,
            _ => {
                return Err(Error::Malformed {
                    descr: "invalid sort direction".to_string(),
                });
            }
        })
    }
}

/// A struct to hold paginated data. It is important to maintain information about the total number
/// of items in the unpaged data set and the total number of pages. This is useful for clients to know
/// when constructing front-end pagination controls.
#[derive(Debug, Clone, PartialEq)]
pub struct Paginated<T> {
    /// The data for the current page
    pub items: Vec<T>,
    /// The total number of items in the unpaged data set
    pub total: i64,
    /// The total number of pages
    pub pages: i64,
}

impl<T> Paginated<T> {
    /// Create a new instance of [`Paginated`]
    pub fn new(items: Vec<T>, total: i64, pages: i64) -> Self {
        Self {
            items,
            total,
            pages,
        }
    }

    /// Create a new instance of [`Paginated`] from a (paged) vector of items, the (unpaged) total
    /// number of items, and an optional [`PaginationOptions`].
    /// If a [`PaginationOptions`] is not provided, the default options will be used.
    pub fn new_from_options(items: Vec<T>, total: i64, options: Option<PaginationOptions>) -> Self {
        let options = options.unwrap_or_default();
        let pages = (total as usize).div_ceil(options.page_size.get()) as i64;
        Self::new(items, total, pages)
    }
}

/// A struct to hold pagination information.
#[derive(Debug, Clone, Copy)]
pub struct PaginationOptions {
    /// Page number to fetch
    pub page_number: NonZeroUsize,
    /// Number of items per page
    pub page_size: NonZeroUsize,
}

impl Default for PaginationOptions {
    fn default() -> Self {
        Self {
            page_number: NonZero::new(1usize).unwrap(),
            page_size: NonZero::new(25usize).unwrap(),
        }
    }
}

impl From<Option<(i32, i32)>> for PaginationOptions {
    fn from(v: Option<(i32, i32)>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::default(),
        }
    }
}

impl From<(Option<i32>, Option<i32>)> for PaginationOptions {
    fn from((page_number, page_size): (Option<i32>, Option<i32>)) -> Self {
        let page_number = page_number.unwrap_or_default();
        let page_size = page_size.unwrap_or_default();
        Self::from((page_number, page_size))
    }
}

impl From<(i32, i32)> for PaginationOptions {
    fn from((page_number, page_size): (i32, i32)) -> Self {
        let mut v = Self::default();
        // it is safe to unwrap from NonZero because we are sure that n > 0
        if page_number > 0 {
            v.page_number = NonZero::new(page_number as usize).unwrap();
        }
        if page_size > 0 {
            v.page_size = NonZero::new(page_size as usize).unwrap();
        }
        v
    }
}

impl PaginationOptions {
    /// Convert PaginationOptions to SQL LIMIT clause
    pub(crate) fn as_sql_limit_offset(&self) -> String {
        format!(
            "LIMIT {} OFFSET {}",
            self.page_size.get(),
            (self.page_number.get() - 1) * self.page_size.get()
        )
    }
}

/// A struct to hold sorting information for namespaces.
#[derive(Debug, Clone, Copy, Default)]
pub struct NamespaceSorting {
    /// Field to sort by
    pub field: NamespaceSortField,
    /// Direction to sort in
    pub direction: SortDirection,
}

impl From<Option<(i32, i32)>> for NamespaceSorting {
    fn from(v: Option<(i32, i32)>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::default(),
        }
    }
}

impl From<(Option<i32>, Option<i32>)> for NamespaceSorting {
    fn from((field, direction): (Option<i32>, Option<i32>)) -> Self {
        let field = field.unwrap_or_default();
        let direction = direction.unwrap_or_default();
        Self::from((field, direction))
    }
}

impl From<(i32, i32)> for NamespaceSorting {
    fn from((field, direction): (i32, i32)) -> Self {
        Self {
            field: NamespaceSortField::try_from(field).unwrap_or_default(),
            direction: SortDirection::try_from(direction).unwrap_or_default(),
        }
    }
}

impl NamespaceSorting {
    /// Convert NamespaceSorting to SQL ORDER BY clause
    pub(crate) fn as_sql_order_by(&self, namespace_table_alias: Option<&str>) -> String {
        let field = self.field.as_sql_column(namespace_table_alias);
        format!("ORDER BY {} {}", field, self.direction)
    }
}

/// Possible ways that tables can be sorted. Used in the API.
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub enum TableSortField {
    /// Sort by the table name
    #[default]
    Name = 1,
    // Sort by storage size is removed because of performance issues:
    // all tables need to be joined to get the size of all files in the table
    // and this is not efficient.
}

impl TableSortField {
    /// Convert TableSortField to SQL ORDER BY clause
    pub(crate) fn as_sql_column(&self, table_alias: Option<&str>) -> String {
        let table_alias_dot = table_alias.map(|alias| format!("{}.", alias));
        match self {
            Self::Name => format!("{}name", table_alias_dot.unwrap_or_default()),
        }
    }
}

impl TryFrom<Option<i32>> for TableSortField {
    type Error = Error;
    fn try_from(v: Option<i32>) -> Result<Self, Error> {
        match v {
            Some(v) => Self::try_from(v),
            None => Err(Error::Malformed {
                descr: "missing sort field: will map to unsorted behavior".to_string(),
            }),
        }
    }
}

impl From<TableSortField> for i32 {
    fn from(val: TableSortField) -> Self {
        val as Self
    }
}

impl TryFrom<i32> for TableSortField {
    type Error = Error;
    fn try_from(v: i32) -> Result<Self, Error> {
        Ok(match v {
            v if v == Self::Name as i32 => Self::Name,
            v => {
                return Err(Error::Malformed {
                    descr: format!("invalid sort field: {v}").to_string(),
                });
            }
        })
    }
}

/// A struct to hold sorting information for namespaces.
#[derive(Debug, Clone, Copy, Default)]
pub struct TableSorting {
    /// Field to sort by
    pub field: TableSortField,
    /// Direction to sort in
    pub direction: SortDirection,
}

impl From<Option<(i32, i32)>> for TableSorting {
    fn from(v: Option<(i32, i32)>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::default(),
        }
    }
}

impl From<(Option<i32>, Option<i32>)> for TableSorting {
    fn from((field, direction): (Option<i32>, Option<i32>)) -> Self {
        let field = field.unwrap_or_default();
        let direction = direction.unwrap_or_default();
        Self::from((field, direction))
    }
}

impl From<(i32, i32)> for TableSorting {
    fn from((field, direction): (i32, i32)) -> Self {
        Self {
            field: TableSortField::try_from(field).unwrap_or_default(),
            direction: SortDirection::try_from(direction).unwrap_or_default(),
        }
    }
}

impl TableSorting {
    /// Convert TableSorting to SQL ORDER BY clause
    pub(crate) fn as_sql_order_by(&self, table_alias: Option<&str>) -> String {
        let field = self.field.as_sql_column(table_alias);
        format!("ORDER BY {} {}", field, self.direction)
    }
}

/// Methods for working with the catalog storage API.
#[async_trait]
pub trait CatalogStorage: Send + Sync + Debug {
    /// Get a namespace with storage information by [`NamespaceId`].
    async fn get_namespace_with_storage(
        &self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<NamespaceWithStorage>>;

    /// Get all namespaces with storage information.
    /// `sorting` is optional and can be used to sort the results.
    async fn get_namespaces_with_storage(
        &self,
        sorting: Option<NamespaceSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<NamespaceWithStorage>>;

    /// Get a table with storage information by [`TableId`].
    async fn get_table_with_storage(
        &self,
        id: TableId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<TableWithStorage>>;

    /// Get all tables with storage information in a given namespace by [`NamespaceId`].
    async fn get_tables_with_storage(
        &self,
        namespace_id: NamespaceId,
        sorting: Option<TableSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<TableWithStorage>>;
}

/// Methods for working with the catalog.
#[async_trait]
pub trait Catalog: Send + Sync + Debug + Any {
    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Setup catalog for usage and apply possible migrations.
    async fn setup(&self) -> Result<(), Error>;

    /// Accesses the repositories without a transaction scope.
    fn repositories(&self) -> Box<dyn RepoCollection>;

    /// Gets metric registry associated with this catalog for testing purposes.
    fn metrics(&self) -> Arc<metric::Registry>;

    /// Get the current time from the catalog's perspective. This function is
    /// distinct from `Catalog::time_provider.now()`. They **may** return different times.
    async fn get_time(&self) -> Result<iox_time::Time>;

    /// Gets the time provider associated with this catalog.
    /// This function is distinct from `get_time()`. They may return different times.
    fn time_provider(&self) -> Arc<dyn TimeProvider>;

    /// Detect active applications running on this catalog instance.
    ///
    /// This includes this very catalog as well.
    ///
    /// This is only implemented for some backends, others may return [`NotImplemented`](Error::NotImplemented).
    async fn active_applications(&self) -> Result<HashSet<String>, Error>;

    /// Machine-readable name.
    fn name(&self) -> &'static str;
}

/// A time provider that uses the catalog's time.
#[derive(Debug)]
pub struct CatalogTimeProvider(Arc<dyn Catalog>);

/// Implementations for [`CatalogTimeProvider`]
impl CatalogTimeProvider {
    /// Create a new instance of [`CatalogTimeProvider`]
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self(catalog)
    }
}

impl std::fmt::Display for CatalogTimeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CatalogTimeProvider")
    }
}

impl AsyncTimeProvider for CatalogTimeProvider {
    type Error = crate::interface::Error;

    async fn now(&self) -> Result<iox_time::Time> {
        self.0.get_time().await
    }
}
/// Methods for working with the catalog's various repositories (collections of entities).
///
/// # Repositories
///
/// The methods (e.g. `create_*` or `get_by_*`) for handling entities (namespaces, partitions,
/// etc.) are grouped into *repositories* with one repository per entity. A repository can be
/// thought of a collection of a single kind of entity. Getting repositories from the transaction
/// is cheap.
///
/// A repository might internally map to a wide range of different storage abstractions, ranging
/// from one or more SQL tables over key-value key spaces to simple in-memory vectors. The user
/// should and must not care how these are implemented.
pub trait RepoCollection: Send + Sync + Debug {
    /// Repository for root information.
    fn root(&mut self) -> &mut dyn RootRepo;

    /// Repository for [namespaces](data_types::Namespace).
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo;

    /// Repository for [tables](data_types::Table).
    fn tables(&mut self) -> &mut dyn TableRepo;

    /// Repository for [columns](data_types::Column).
    fn columns(&mut self) -> &mut dyn ColumnRepo;

    /// Repository for [partitions](data_types::Partition).
    fn partitions(&mut self) -> &mut dyn PartitionRepo;

    /// Repository for [Parquet files](data_types::ParquetFile).
    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo;

    /// Set span context for all operations performed.
    fn set_span_context(&mut self, span_ctx: Option<SpanContext>);
}

/// Functions for working with root of the catalog
#[async_trait]
pub trait RootRepo: Send + Sync {
    /// Obtain a root snapshot
    async fn snapshot(&mut self) -> Result<RootSnapshot>;
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo: Send + Sync {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    /// Specify `None` for `retention_period_ns` to get infinite retention.
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace>;

    /// Update retention period for a namespace
    async fn update_retention_period(
        &mut self,
        id: NamespaceId,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace>;

    /// List all namespaces.
    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;

    /// List all namespaces with storage.
    /// Note: This excludes any namespaces that start with "_influx"
    async fn list_storage(
        &mut self,
        sorting: Option<NamespaceSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<NamespaceWithStorage>>;

    /// Gets the namespace by its ID.
    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>>;

    /// Gets an active namespace by its unique name. This will not consider any soft-deleted namespaces.
    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>>;

    /// Soft-delete a namespace by ID.
    async fn soft_delete(&mut self, id: NamespaceId) -> Result<Namespace>;

    /// Update the limit on the number of tables that can exist per namespace.
    async fn update_table_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxTables,
    ) -> Result<Namespace>;

    /// Update the limit on the number of columns that can exist per table in a given namespace.
    async fn update_column_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace>;

    /// Obtain a namespace snapshot
    async fn snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot>;

    /// Obtain a namespace snapshot by name
    async fn snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot>;

    /// Gets the namespace with storage information by its ID.
    async fn get_storage_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<NamespaceWithStorage>>;

    /// Rename the namespace corresponding to `id`.
    async fn rename(&mut self, id: NamespaceId, new_name: NamespaceName<'_>) -> Result<Namespace>;

    /// Undelete the soft deleted namespace corresponding to `id`.
    ///
    /// There must be no active, undeleted namespace using the target's name.
    async fn undelete(&mut self, id: NamespaceId) -> Result<Namespace>;
}

/// Fallback logic for [`NamespaceRepo::snapshot_by_name`]
///
/// This is not a default implementation to avoid accidental fallback behaviour
/// and to allow the gRPC client to use this based on the server response
pub(crate) async fn namespace_snapshot_by_name(
    repo: &mut impl NamespaceRepo,
    name: &str,
) -> Result<NamespaceSnapshot> {
    let ns = repo
        .get_by_name(name)
        .await?
        .ok_or_else(|| Error::NotFound {
            descr: name.to_string(),
        })?;
    repo.snapshot(ns.id).await
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo: Send + Sync {
    /// Creates the table in the catalog. If one in the same namespace with the same name already
    /// exists, an error is returned.
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table>;

    /// get table by ID
    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;

    /// get table with storage information by ID
    async fn get_storage_by_id(
        &mut self,
        table_id: TableId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<TableWithStorage>>;

    /// get table by namespace ID and name
    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;

    /// List all tables with storage in the catalog for the given namespace id.
    async fn list_storage_by_namespace_id(
        &mut self,
        namespace_id: NamespaceId,
        sorting: Option<TableSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<TableWithStorage>>;

    /// List all tables.
    async fn list(&mut self) -> Result<Vec<Table>>;

    /// Obtain a table snapshot
    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot>;

    /// List all tables which have iceberg enabled for the given namespace.
    async fn list_by_iceberg_enabled(&mut self, namespace_id: NamespaceId) -> Result<Vec<TableId>>;

    /// Enable iceberg exporting for the specified table.
    ///
    /// NOTE: this does not provision the necessary infrastructure to handle
    /// exports, this resides with the platform team.
    ///
    /// Enabling iceberg here means that the exporter can be aware of
    /// which tables are expected to be exported.
    async fn enable_iceberg(&mut self, table_id: TableId) -> Result<()>;

    /// Disable iceberg exporting for the specified table.
    ///
    /// This means that the exporter will no longer consider this table within
    /// its next export run.
    async fn disable_iceberg(&mut self, table_id: TableId) -> Result<()>;

    /// Soft delete the table corresponding to `id` - this returns a `Table`
    /// to allow the public API to return the "deleted_at" field without a
    /// follow-up query.
    async fn soft_delete(&mut self, id: TableId) -> Result<Table>;

    /// Rename the table corresponding to `id`.
    async fn rename(&mut self, id: TableId, new_name: &str) -> Result<Table>;

    /// Undelete the soft deleted table corresponding to `id`.
    ///
    /// There must be no active, undeleted table using the target's name
    /// in the namespace the target belongs to.
    async fn undelete(&mut self, id: TableId) -> Result<Table>;
}

/// Functions for working with columns in the catalog
#[async_trait]
pub trait ColumnRepo: Send + Sync {
    /// Creates the column in the catalog or returns the existing column. Will return a
    /// `Error::ColumnTypeMismatch` if the existing column type doesn't match the type
    /// the caller is attempting to create.
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column>;

    /// Perform a bulk upsert of columns specified by a map of column name to column type.
    ///
    /// Implementations make no guarantees as to the ordering or atomicity of
    /// the batch of column upsert operations - a batch upsert may partially
    /// commit, in which case an error MUST be returned by the implementation.
    ///
    /// Per-namespace limits on the number of columns allowed per table are explicitly NOT checked
    /// by this function, hence the name containing `unchecked`. It is expected that the caller
    /// will check this first-- and yes, this is racy.
    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;

    /// List all columns for the given table ID.
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;

    /// List columns based on their deleted status.
    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Column>>;
}

/// Extension trait for [`ParquetFileRepo`]
#[async_trait]
pub trait PartitionRepoExt {
    /// create the parquet file
    async fn get_by_id(self, partition_id: PartitionId) -> Result<Option<Partition>>;
}

#[async_trait]
impl PartitionRepoExt for &mut dyn PartitionRepo {
    async fn get_by_id(self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let iter = self.get_by_id_batch(&[partition_id]).await?;
        Ok(iter.into_iter().next())
    }
}

/// Functions for working with IOx partitions in the catalog. These are how IOx splits up
/// data within a namespace.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key and table
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;

    /// For test use only, update the new_file_at time on a partition
    async fn set_new_file_at(
        &mut self,
        partition_id: PartitionId,
        new_file_at: Timestamp,
    ) -> Result<()>;

    /// get multiple partitions by ID.
    ///
    /// the output order is undefined, non-existing partitions are not part of the output.
    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>>;

    /// return the partitions by table id
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;

    /// return all partitions IDs
    async fn list_ids(&mut self) -> Result<Vec<PartitionId>>;

    /// Update the sort key for the partition, setting it to `new_sort_key_ids` iff
    /// the current value matches `old_sort_key_ids`.
    ///
    /// NOTE: it is expected that ONLY the ingesters update sort keys for
    /// existing partitions.
    ///
    /// # Spurious failure
    ///
    /// Implementations are allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons in the presence of
    /// concurrent writers.
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>>;

    /// Record an instance of a partition being selected for compaction but compaction was not
    /// completed for the specified reason.
    #[expect(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()>;

    /// Get the record of partitions being skipped.
    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>>;

    /// List the records of compacting a partition being skipped. This is mostly useful for testing.
    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;

    /// Delete the records of skipping a partition being compacted.
    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>>;

    /// Return the N most recently created partitions.
    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;

    /// Select partitions with a `new_file_at` value greater than the minimum time value and, if specified, less than
    /// the maximum time value. Both range ends are exclusive; a timestamp exactly equal to either end will _not_ be
    /// included in the results.
    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>>;

    /// Select next batch of partitions needing cold compaction up through `maximum_time`
    /// A partition needs cold compaction if its `new_file_at` is less than or equal to the maximum time
    /// and its either never been cold compacted (`cold_compact_at`` == 0) or the last cold compaction
    /// has been invalided by a new file (`new_file_at` > `cold_compact_at`).
    async fn partitions_needing_cold_compact(
        &mut self,
        maximum_time: Timestamp,
        n: usize,
    ) -> Result<Vec<PartitionId>>;

    /// Update the time of the last cold compaction for the specified partition.
    async fn update_cold_compact(
        &mut self,
        partition_id: PartitionId,
        cold_compact_at: Timestamp,
    ) -> Result<()>;

    /// Return all partitions that do not have hash IDs in the catalog. Used in
    /// the ingester's `OldPartitionBloomFilter` to determine whether a catalog query is necessary.
    /// Can be removed when all partitions have hash IDs and support for old-style partitions is no
    /// longer needed.
    async fn list_old_style(&mut self) -> Result<Vec<Partition>>;

    /// Delete empty partitions more than a day outside the retention interval (as determined by
    /// their partition key) that were created at least `partition_cutoff` ago (to avoid
    /// immediately deleting backfill partitions added via bulk ingest).
    ///
    /// This deletion is limited to a certain (backend-specific) number of files to avoid overlarge
    /// changes. The caller MAY call this method again if the result was NOT empty.
    async fn delete_by_retention(
        &mut self,
        partition_cutoff: Duration,
    ) -> Result<Vec<(TableId, PartitionId)>>;

    /// Obtain a partition snapshot
    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot>;

    /// Obtain a partition snapshot generation number. If possible, this
    /// method will return the generation number using a lighter-weight
    /// implementation that would be required for a full snapshot.
    async fn snapshot_generation(&mut self, partition_id: PartitionId) -> Result<u64>;
}

/// Extension trait for [`ParquetFileRepo`]
#[async_trait]
pub trait ParquetFileRepoExt {
    /// Create the parquet file, returning nothing. If you need the catalog-assigned fields
    /// after calling this, do another lookup. This is mostly used in tests, so the performance of
    /// a lookup shouldn't be significant. Revisit this assumption if there's a need for the
    /// created fields in production.
    async fn create(self, parquet_file_params: ParquetFileParams) -> Result<()>;

    /// Attempt to upsert the parquet file
    ///
    /// Unlike [`Self::create`] this will not return an error if an entry already exists
    /// with the same parameters. However, [`Error::AlreadyExists`] will still be returned
    /// if an entry already exists with different parameters
    async fn upsert(self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;
}

#[async_trait]
impl ParquetFileRepoExt for &mut dyn ParquetFileRepo {
    async fn create(self, params: ParquetFileParams) -> Result<()> {
        self.create_upgrade_delete(
            params.partition_id,
            &[],
            &[],
            &[params],
            CompactionLevel::Initial,
        )
        .await
        .map(|_| ())
    }

    async fn upsert(mut self, params: ParquetFileParams) -> Result<ParquetFile> {
        // We can't use `self.create` here as the lifetime bounds cause issues
        let r = self
            .create_upgrade_delete(
                params.partition_id,
                &[],
                &[],
                &[params.clone()],
                CompactionLevel::Initial,
            )
            .await;

        match r {
            Ok(files) => Ok(files.into_iter().next().unwrap()),
            Err(Error::AlreadyExists { .. }) => {
                let id = params.object_store_id;
                if let Some(existing) = self.get_by_object_store_id(id).await? {
                    if existing.could_have_been_created_from(&params)
                        && existing.to_delete.is_none()
                    {
                        return Ok(existing);
                    }
                }
                Err(Error::AlreadyExists {
                    descr: format!("conflicting parquet file with object store id {id}"),
                })
            }
            Err(e) => Err(e),
        }
    }
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// Flag all parquet files for deletion that are older than their namespace's retention period.
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>>;

    /// Delete parquet files that were marked to be deleted earlier than the specified time.
    ///
    /// Returns the deleted IDs only.
    ///
    /// This deletion is limited to a certain (backend-specific) number of files to avoid overlarge
    /// changes. The caller MAY call this method again if the result was NOT empty.
    #[deprecated(
        note = "use delete_old_ids_count instead - it performs better and has more customization"
    )]
    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>>;

    /// This does the same thing as calling `self.delete_old_ids_only` and then counting the
    /// result, but is implemented as a separate method to allow for some performance optimizations
    /// where possible (e.g. in the postgres catalog, we can avoid allocating the intermediate Vec)
    /// and allows for configuration of the maximum amount of files to delete at a time.
    async fn delete_old_ids_count(
        &mut self,
        older_than: Timestamp,
        limit: u32,
    ) -> Result<(u64, Option<Timestamp>)>;

    /// List parquet files for given partitions that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    ///
    /// The output order is undefined, non-existing partitions are not part of the output.
    /// The number of L1 and L2 file counts are unlimited, but a maximum of
    /// `MAX_PARQUET_L0_FILES_PER_PARTITION` L0 files are returned per partition, with the returned
    /// files being the oldest L0s.
    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>>;

    /// List Parquet files that are active as of the specified timestamp. Active means the file was
    /// created before the specified time, and was not deleted before the specified time.
    #[deprecated]
    async fn active_as_of(&mut self, as_of: Timestamp) -> Result<Vec<ParquetFile>>;

    /// Return the parquet file with the given object store id
    // used heavily in tests for verification of catalog state.
    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>>;

    /// Test a batch of parquet files exist by object store ids
    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>>;

    /// Test a batch of parquet files exist by partition and object store IDs
    async fn exists_by_partition_and_object_store_id_batch(
        &mut self,
        ids: Vec<(PartitionId, ObjectStoreId)>,
    ) -> Result<Vec<(PartitionId, ObjectStoreId)>>;

    /// Commit deletions, upgrades and creations in a single transaction.
    ///
    /// Returns created files.
    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a particular table (via [`TableId`]) and
    /// optionally at a specific [`CompactionLevel`].
    ///
    /// NOTE: implementations of this interface implicitly produce files
    /// that are NOT marked for deletion.
    async fn list_by_table_id(
        &mut self,
        table_id: TableId,
        compaction_level: Option<CompactionLevel>,
    ) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a particular Namespace (via [`NamespaceId`]) and
    /// deletion status
    async fn list_by_namespace_id(
        &mut self,
        namespace_id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Vec<ParquetFile>>;
}
