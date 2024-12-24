//! Cache layer.

mod batcher;
mod handler;
mod loader;
mod snapshot;

#[cfg(test)]
mod test_utils;

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use backoff::BackoffConfig;
use catalog_cache::api::quorum::QuorumCatalogCache;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::{
        namespace::NamespaceSnapshot, partition::PartitionSnapshot, root::RootSnapshot,
        table::TableSnapshot,
    },
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, NamespaceWithStorage, ObjectStoreId,
    ParquetFile, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, TableWithStorage, Timestamp,
};
use futures::{Future, StreamExt, TryFutureExt, TryStreamExt};
use iox_time::TimeProvider;
use trace::ctx::SpanContext;

use crate::constants::MAX_PARQUET_L0_FILES_PER_PARTITION;
use crate::interface::{
    CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
    RepoCollection, Result, RootRepo, SoftDeletedRows, TableRepo,
};
use crate::metrics::CatalogMetrics;

use self::handler::{CacheHandlerCatalog, CacheHandlerRepos};
use self::snapshot::RootKey;

/// Parameters for [`CachingCatalog`].
#[derive(Debug)]
pub struct CachingCatalogParams {
    /// quorum-based cache
    pub cache: Arc<QuorumCatalogCache>,

    /// underlying backing catalog
    pub backing: Arc<dyn Catalog>,

    /// metrics registry
    pub metrics: Arc<metric::Registry>,

    /// time provider, used for metrics
    pub time_provider: Arc<dyn TimeProvider>,

    /// number of concurrent quorum operations that a single request can trigger
    pub quorum_fanout: usize,

    /// linger timeout for writes impacting the partition snapshot
    pub partition_linger: Duration,

    /// linger timeout for writes impacting the table snapshot
    pub table_linger: Duration,

    /// Do not eagerly update a partition snapshot when parquet files
    /// are added, upgraded, or removed. Instead clear the partition
    /// snapshot in a quorum of the cache so that the partition snapshot
    /// will be read from the backing store the next time it is read.
    pub parquet_file_updates_delete_partition_snapshots: bool,
}

/// Caching catalog.
#[derive(Debug)]
pub struct CachingCatalog {
    backing: Arc<dyn Catalog>,
    metrics: CatalogMetrics,
    time_provider: Arc<dyn TimeProvider>,
    quorum_fanout: usize,
    parquet_file_updates_delete_partition_snapshots: bool,
    partitions: CacheHandlerCatalog<PartitionSnapshot>,
    tables: CacheHandlerCatalog<TableSnapshot>,
    namespaces: CacheHandlerCatalog<NamespaceSnapshot>,
    root: CacheHandlerCatalog<RootSnapshot>,
}

impl CachingCatalog {
    const NAME: &'static str = "cache";

    /// Create new caching catalog.
    pub fn new(params: CachingCatalogParams) -> Self {
        let CachingCatalogParams {
            cache,
            backing,
            metrics,
            time_provider,
            quorum_fanout,
            partition_linger,
            table_linger,
            parquet_file_updates_delete_partition_snapshots,
        } = params;

        // We set a more aggressive backoff configuration than normal as we expect latencies to be low
        let backoff_config = Arc::new(BackoffConfig {
            init_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            base: 5.0,
            deadline: Some(Duration::from_millis(750)),
        });

        let partitions = CacheHandlerCatalog::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
            Arc::clone(&time_provider),
            partition_linger,
        );
        let tables = CacheHandlerCatalog::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
            Arc::clone(&time_provider),
            table_linger,
        );
        let namespaces = CacheHandlerCatalog::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
            Arc::clone(&time_provider),
            Duration::from_secs(0),
        );
        let root = CacheHandlerCatalog::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
            Arc::clone(&time_provider),
            Duration::from_secs(0),
        );

        Self {
            backing,
            metrics: CatalogMetrics::new(metrics, Arc::clone(&time_provider), Self::NAME),
            time_provider,
            quorum_fanout,
            parquet_file_updates_delete_partition_snapshots,
            partitions,
            tables,
            namespaces,
            root,
        }
    }
}

#[async_trait]
impl Catalog for CachingCatalog {
    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(self.metrics.repos(Box::new(Repos {
            backing: self.backing.repositories(),
            quorum_fanout: self.quorum_fanout,
            tables: self.tables.repos(),
            partitions: self.partitions.repos(),
            namespaces: self.namespaces.repos(),
            root: self.root.repos(),
            parquet_file_updates_delete_partition_snapshots:
                self.parquet_file_updates_delete_partition_snapshots,
        })))
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        self.metrics.registry()
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    async fn get_time(&self) -> Result<iox_time::Time, Error> {
        self.backing.get_time().await
    }

    async fn active_applications(&self) -> Result<HashSet<String>, Error> {
        Err(Error::NotImplemented {
            descr: "active applications".to_owned(),
        })
    }

    fn name(&self) -> &'static str {
        Self::NAME
    }
}

#[derive(Debug)]
struct Repos {
    partitions: CacheHandlerRepos<PartitionSnapshot>,
    tables: CacheHandlerRepos<TableSnapshot>,
    namespaces: CacheHandlerRepos<NamespaceSnapshot>,
    root: CacheHandlerRepos<RootSnapshot>,
    backing: Box<dyn RepoCollection>,
    quorum_fanout: usize,
    parquet_file_updates_delete_partition_snapshots: bool,
}

impl Repos {
    async fn namespace_id_by_name(&self, name: &str) -> Result<NamespaceId, Error> {
        let root = self.root.get(RootKey).await?;
        let ns_id = root
            .lookup_namespace_by_name(name)?
            .ok_or_else(|| Error::NotFound {
                descr: format!("namespace: {name}"),
            })?
            .id();
        Ok(ns_id)
    }
}

impl RepoCollection for Repos {
    fn root(&mut self) -> &mut dyn RootRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }

    fn set_span_context(&mut self, span_ctx: Option<SpanContext>) {
        self.partitions.set_span_context(span_ctx.clone());
        self.tables.set_span_context(span_ctx.clone());
        self.namespaces.set_span_context(span_ctx.clone());
        self.root.set_span_context(span_ctx.clone());
        self.backing.set_span_context(span_ctx);
    }
}

#[async_trait]
impl RootRepo for Repos {
    async fn snapshot(&mut self) -> Result<RootSnapshot> {
        self.root.get(RootKey).await
    }
}

#[async_trait]
impl NamespaceRepo for Repos {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let ns = self
            .backing
            .namespaces()
            .create(
                name,
                partition_template,
                retention_period_ns,
                service_protection_limits,
            )
            .with_refresh(self.root.refresh(RootKey))
            .await?;

        // Warm cache
        self.namespaces.warm_up(ns.id).await?;

        Ok(ns)
    }

    async fn update_retention_period(
        &mut self,
        id: NamespaceId,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let ns = self
            .backing
            .namespaces()
            .update_retention_period(id, retention_period_ns)
            .with_refresh(self.namespaces.refresh(id))
            .await?;

        assert_eq!(ns.id, id);

        Ok(ns)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        // This method is called infrequently by the routers to warm their caches
        // and will load data on all namespaces including a number that aren't
        // in the working set.
        //
        // Rather than polluting our caches we just read through for this data
        //
        // There are potential future consistency issues with this approach, see
        // https://github.com/influxdata/influxdb_iox/pull/10407#discussion_r1526046175,
        // but if/when such data dependencies are introduced into namespace data
        // we can potentially revisit this design
        self.backing.namespaces().list(deleted).await
    }

    async fn list_storage(&mut self) -> Result<Vec<NamespaceWithStorage>> {
        let root = self.root.get(RootKey).await?;

        let mut namespaces_with_storage: Vec<NamespaceWithStorage> = Vec::new();

        for ns in root.namespaces() {
            let ns = ns?;
            let ns_snapshot = match self.namespaces.get(ns.id()).await {
                Ok(ns_s) => ns_s,
                Err(Error::NotFound { .. }) => {
                    // This error indicates that a namespace exists in the root snapshot
                    // but not in the namespace snapshot. In general, this should not happen.
                    // However, it might occur if a namespace is deleted, and the namespace snapshot
                    // is updated to remove the namespace, but the root snapshot has not
                    // been updated, leaving the namespace still present in the root snapshot.
                    // In this case, do not calculate the storage information for this namespace.
                    // Instead, skip to the next iteration of the loop.
                    continue;
                }
                Err(e) => return Err(e),
            };

            if ns_snapshot.deleted_at().is_some() {
                // If the namespace is deleted, no need to calculate the storage information.
                // Instead, skip to the next namespace
                continue;
            }

            let n = ns_snapshot.namespace()?;

            // read-through: not latency sensitive
            let namespace_size_bytes = self
                .backing
                .parquet_files()
                .list_by_namespace_id(n.id, SoftDeletedRows::ExcludeDeleted)
                .await?
                .iter()
                .map(|f| f.file_size_bytes)
                .sum();
            let table_count = self
                .backing
                .tables()
                .list_by_namespace_id(n.id)
                .await?
                .len() as i64;

            namespaces_with_storage.push(NamespaceWithStorage {
                id: n.id,
                name: n.name.clone(),
                retention_period_ns: n.retention_period_ns,
                max_tables: n.max_tables,
                max_columns_per_table: n.max_columns_per_table,
                partition_template: n.partition_template.clone(),
                size_bytes: namespace_size_bytes,
                table_count,
            })
        }

        Ok(namespaces_with_storage)
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        match self.namespaces.get(id).await {
            Ok(n) => Ok(filter_namespace(n.namespace()?, deleted)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let root = self.root.get(RootKey).await?;
        let n = match root.lookup_namespace_by_name(name)? {
            Some(n) => n,
            None => {
                return Ok(None);
            }
        };

        Ok(filter_namespace(
            self.namespaces.get(n.id()).await?.namespace()?,
            deleted,
        ))
    }

    async fn soft_delete(&mut self, id: NamespaceId) -> Result<NamespaceId> {
        let deleted_id = self
            .backing
            .namespaces()
            .soft_delete(id)
            .with_refresh(self.namespaces.refresh(id))
            .await?;

        assert_eq!(id, deleted_id);

        Ok(id)
    }

    async fn update_table_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxTables,
    ) -> Result<Namespace> {
        let ns = self
            .backing
            .namespaces()
            .update_table_limit(id, new_max)
            .with_refresh(self.namespaces.refresh(id))
            .await?;

        assert_eq!(ns.id, id);

        Ok(ns)
    }

    async fn update_column_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        let ns = self
            .backing
            .namespaces()
            .update_column_limit(id, new_max)
            .with_refresh(self.namespaces.refresh(id))
            .await?;

        assert_eq!(ns.id, id);

        Ok(ns)
    }

    async fn snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot> {
        self.namespaces.get(namespace_id).await
    }

    async fn snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot> {
        let id = self.namespace_id_by_name(name).await?;
        self.namespaces.get(id).await
    }

    async fn get_storage_by_id(&mut self, id: NamespaceId) -> Result<Option<NamespaceWithStorage>> {
        // read-through: not latency sensitive
        let namespace_size_bytes = self
            .backing
            .parquet_files()
            .list_by_namespace_id(id, SoftDeletedRows::ExcludeDeleted)
            .await?
            .iter()
            .map(|f| f.file_size_bytes)
            .sum();
        let table_count = self.backing.tables().list_by_namespace_id(id).await?.len() as i64;

        match self.namespaces.get(id).await {
            Ok(n) => {
                let ns = n.namespace()?;
                Ok(Some(NamespaceWithStorage {
                    id: ns.id,
                    name: ns.name,
                    retention_period_ns: ns.retention_period_ns,
                    max_tables: ns.max_tables,
                    max_columns_per_table: ns.max_columns_per_table,
                    partition_template: ns.partition_template,
                    size_bytes: namespace_size_bytes,
                    table_count,
                }))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl TableRepo for Repos {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let table = self
            .backing
            .tables()
            .create(name, partition_template, namespace_id)
            .with_refresh(self.namespaces.refresh(namespace_id))
            .await?;

        // Warm cache
        self.tables.warm_up(table.id).await?;

        Ok(table)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        match self.tables.get(table_id).await {
            Ok(s) => Ok(Some(s.table()?)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_storage_by_id(&mut self, table_id: TableId) -> Result<Option<TableWithStorage>> {
        match self.tables.get(table_id).await {
            Ok(table_snapshot) => {
                let mut table_size_bytes: i64 = 0;
                for partition_snapshot in table_snapshot.partitions() {
                    for file in self.partitions.get(partition_snapshot?.id()).await?.files() {
                        let file = file?;
                        if file.to_delete.is_none() {
                            table_size_bytes += file.file_size_bytes;
                        }
                    }
                }

                let table = table_snapshot.table()?;
                Ok(Some(TableWithStorage {
                    id: table.id,
                    name: table.name,
                    namespace_id: table.namespace_id,
                    partition_template: table.partition_template,
                    size_bytes: table_size_bytes,
                }))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let namespace = match self.namespaces.get(namespace_id).await {
            Ok(ns) => ns,
            Err(Error::NotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        };
        let t = match namespace.lookup_table_by_name(name)? {
            Some(t) => t,
            None => {
                return Ok(None);
            }
        };

        Ok(Some(self.tables.get(t.id()).await?.table()?))
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let namespace = match self.namespaces.get(namespace_id).await {
            Ok(ns) => ns,
            Err(Error::NotFound { .. }) => {
                return Ok(vec![]);
            }
            Err(e) => {
                return Err(e);
            }
        };
        let tables = namespace.tables().collect::<Result<Vec<_>, _>>()?;

        futures::stream::iter(tables)
            .map(|t| {
                let this = &self;
                async move {
                    let snapshot = match this.tables.get(t.id()).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.table() {
                        Ok(p) => Ok(futures::stream::once(async move { Ok(p) }).boxed()),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_storage_by_namespace_id(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<TableWithStorage>> {
        let namespace_snapshot = match self.namespaces.get(namespace_id).await {
            Ok(ns) => ns,
            Err(Error::NotFound { .. }) => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        if namespace_snapshot.deleted_at().is_some() {
            return Ok(vec![]);
        }

        let mut tables_with_storage: Vec<TableWithStorage> = Vec::new();

        for table in namespace_snapshot.tables() {
            let table = table?;
            let table_snapshot = match self.tables.get(table.id()).await {
                Ok(ts) => ts,
                Err(Error::NotFound { .. }) => {
                    // This error indicates that a table exists in the namespace snapshot
                    // but not in the table snapshot. In general, this should not happen.
                    // However, it might occur if a table is deleted, and the table snapshot
                    // is updated to remove the table, but the namespace snapshot has not
                    // been updated, leaving the table still present in the namespace snapshot.
                    // In this case, do not calculate the size for this table.
                    // Instead, skip to the next iteration of the loop.
                    continue;
                }
                Err(e) => return Err(e),
            };

            let mut table_size: i64 = 0;
            for partition_snapshot in table_snapshot.partitions() {
                for file in self.partitions.get(partition_snapshot?.id()).await?.files() {
                    let file = file?;
                    if file.to_delete.is_none() {
                        table_size += file.file_size_bytes;
                    }
                }
            }

            let t = table_snapshot.table()?;

            tables_with_storage.push(TableWithStorage {
                id: t.id,
                name: t.name.clone(),
                namespace_id: t.namespace_id,
                partition_template: t.partition_template.clone(),
                size_bytes: table_size,
            });
        }

        Ok(tables_with_storage)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        // read-through: global-scoped, only used by parquet cache during startup
        self.backing.tables().list().await
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        self.tables.get(table_id).await
    }
}

#[async_trait]
impl ColumnRepo for Repos {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let table = self.tables.get(table_id).await?;
        for col in table.columns() {
            let col = col?;
            if col.name == name {
                if column_type != col.column_type {
                    return Err(Error::AlreadyExists {
                        descr: format!(
                            "column {} is type {} but schema update has type {}",
                            name, col.column_type, column_type
                        ),
                    });
                }
                return Ok(col);
            }
        }

        let c = self
            .backing
            .columns()
            .create_or_get(name, table_id, column_type)
            .with_refresh(self.tables.refresh(table_id))
            .await?;

        Ok(c)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let table = self.tables.get(table_id).await?;
        let mut found = Vec::with_capacity(columns.len());
        for col in table.columns() {
            let col = col?;
            if let Some(expected) = columns.get(col.name.as_str()) {
                if expected != &col.column_type {
                    return Err(Error::AlreadyExists {
                        descr: format!(
                            "column {} is type {} but schema update has type {}",
                            col.name, col.column_type, expected
                        ),
                    });
                }
                found.push(col);
            }
        }

        if found.len() == columns.len() {
            return Ok(found);
        }

        let r = self
            .backing
            .columns()
            .create_or_get_many_unchecked(table_id, columns)
            .with_refresh(self.tables.refresh(table_id))
            .await?;

        Ok(r)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        // read-through: users should use `list_by_table_id`, see https://github.com/influxdata/influxdb_iox/issues/10146
        self.backing
            .columns()
            .list_by_namespace_id(namespace_id)
            .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let table = self.tables.get(table_id).await?;
        Ok(table.columns().collect::<Result<_, _>>()?)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        // Read-through: should we retain this method?
        self.backing.columns().list().await
    }
}

#[async_trait]
impl PartitionRepo for Repos {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let table = self.tables.get(table_id).await?;

        // If we can find the snapshot, use it
        if let Some(p) = table.lookup_partition_by_key(&key)? {
            let partition = self.partitions.get(p.id()).await?;
            return Ok(partition.partition()?);
        }

        let p = self
            .backing
            .partitions()
            .create_or_get(key, table_id)
            .with_refresh(self.tables.refresh_batched(table_id))
            .await?;

        Ok(p)
    }

    async fn set_new_file_at(
        &mut self,
        _partition_id: PartitionId,
        _new_file_at: Timestamp,
    ) -> Result<()> {
        Err(Error::NotImplemented {
            descr: "set_new_file_at is for test use only, not implemented for cache".to_string(),
        })
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        futures::stream::iter(prepare_set(partition_ids.iter().cloned()))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.partitions.get(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.partition() {
                        Ok(p) => Ok(futures::stream::once(async move { Ok(p) }).boxed()),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let table = self.tables.get(table_id).await?;
        let partitions = table.partitions().collect::<Result<Vec<_>, _>>()?;

        futures::stream::iter(partitions)
            .map(|p| {
                let this = &self;
                async move {
                    let snapshot = match this.partitions.get(p.id()).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.partition() {
                        Ok(p) => Ok(futures::stream::once(async move { Ok(p) }).boxed()),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        // read-through: only used for testing, we should eventually remove this interface
        self.backing.partitions().list_ids().await
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        let res = self
            .backing
            .partitions()
            .cas_sort_key(partition_id, old_sort_key_ids, new_sort_key_ids)
            .with_refresh(
                self.partitions
                    .refresh(partition_id)
                    .map_err(CasFailure::QueryError),
            )
            .await?;

        Ok(res)
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        self.backing
            .partitions()
            .record_skipped_compaction(
                partition_id,
                reason,
                num_files,
                limit_num_files,
                limit_num_files_first_in_partition,
                estimated_bytes,
                limit_bytes,
            )
            .with_refresh(self.partitions.refresh(partition_id))
            .await?;

        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        futures::stream::iter(prepare_set(partition_id.iter().cloned()))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.partitions.get(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.skipped_compaction() {
                        Some(sc) => Ok(futures::stream::once(async move { Ok(sc) }).boxed()),
                        None => Ok(futures::stream::empty().boxed()),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        // read-through: used for debugging, this should be replaced w/ proper hierarchy-traversal
        self.backing.partitions().list_skipped_compactions().await
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let res = self
            .backing
            .partitions()
            .delete_skipped_compactions(partition_id)
            .with_refresh(self.partitions.refresh(partition_id))
            .await?;

        Ok(res)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        // read-through: used for ingester warm-up at the moment
        self.backing.partitions().most_recent_n(n).await
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        // read-through: used by the compactor for scheduling, we should eventually find a better interface
        self.backing
            .partitions()
            .partitions_new_file_between(minimum_time, maximum_time)
            .await
    }

    async fn partitions_needing_cold_compact(
        &mut self,
        maximum_time: Timestamp,
        n: usize,
    ) -> Result<Vec<PartitionId>> {
        // read-through: used by the compactor for scheduling, we should eventually find a better interface
        self.backing
            .partitions()
            .partitions_needing_cold_compact(maximum_time, n)
            .await
    }

    async fn update_cold_compact(
        &mut self,
        partition_id: PartitionId,
        cold_compact_at: Timestamp,
    ) -> Result<()> {
        // read-through: used by the compactor for scheduling, we should eventually find a better interface
        self.backing
            .partitions()
            .update_cold_compact(partition_id, cold_compact_at)
            .await
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        // read-through: used by the ingester due to hash-id stuff
        self.backing.partitions().list_old_style().await
    }

    async fn delete_by_retention(&mut self) -> Result<Vec<(TableId, PartitionId)>> {
        let res = self.backing.partitions().delete_by_retention().await?;

        let affected_tables: HashSet<_> = res.iter().map(|(t, _)| *t).collect();

        // Refresh all impacted tables to avoid lost updates
        futures::stream::iter(affected_tables)
            .map(|t_id| {
                let this = &self;
                async move {
                    this.tables.refresh(t_id).await?;
                    Ok::<(), Error>(())
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_collect::<()>()
            .await?;

        Ok(res)
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        self.partitions.get(partition_id).await
    }

    async fn snapshot_generation(&mut self, partition_id: PartitionId) -> Result<u64> {
        (self as &mut dyn PartitionRepo)
            .snapshot(partition_id)
            .await
            .map(|s| s.generation())
    }
}

#[async_trait]
impl ParquetFileRepo for Repos {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let res = self
            .backing
            .parquet_files()
            .flag_for_delete_by_retention()
            .await?;

        let affected_partitions = res
            .iter()
            .map(|(p_id, _os_id)| *p_id)
            .collect::<HashSet<_>>();

        // ensure deterministic order
        let mut affected_partitions = affected_partitions.into_iter().collect::<Vec<_>>();
        affected_partitions.sort_unstable();

        // refresh ALL partitons that are affected, NOT just only the ones that were cached. This should avoid the
        // following "lost update" race condition:
        //
        // This scenario assumes that the partition in question is NOT cached yet.
        //
        // | T | Thread 1                              | Thread 2                                           |
        // | - | ------------------------------------- | -------------------------------------------------- |
        // | 1 | receive `create_update_delete`        |                                                    |
        // | 2 | execute change within backing catalog |                                                    |
        // | 3 | takes snapshot from backing catalog   |                                                    |
        // | 4 |                                       | receive `flag_for_delete_by_retention`             |
        // | 5 |                                       | execute change within backing catalog              |
        // | 6 |                                       | affected partition not cached => no snapshot taken |
        // | 7 |                                       | return                                             |
        // | 8 | quorum-write snapshot                 |                                                    |
        // | 9 | return                                |                                                    |
        //
        // The partition is now cached by does NOT contain the `flag_for_delete_by_retention` change and will not
        // automatically converge.
        futures::stream::iter(affected_partitions)
            .map(|p_id| {
                let this = &self;
                async move {
                    if this.parquet_file_updates_delete_partition_snapshots {
                        this.partitions.delete(p_id).await?;
                    } else {
                        this.partitions.refresh(p_id).await?;
                    };
                    Ok::<(), Error>(())
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_collect::<()>()
            .await?;

        Ok(res)
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        // deleted files are NOT part of the snapshot, so this bypasses the cache
        self.backing
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
    }

    async fn delete_old_ids_count(&mut self, older_than: Timestamp) -> Result<u64> {
        self.backing
            .parquet_files()
            .delete_old_ids_count(older_than)
            .await
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        let res = futures::stream::iter(prepare_set(partition_ids))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.partitions.get(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    // Decode files so we can drop the snapshot early.
                    //
                    // Need to collect the file results into a vec though because we cannot return borrowed data and
                    // "owned iterators" aren't a thing.
                    let files = snapshot
                        .files()
                        .map(|res| res.map_err(Error::from))
                        .collect::<Vec<_>>();
                    Ok::<_, Error>(futures::stream::iter(files).boxed())
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await?;

        // Apply the MAX_PARQUET_L0_FILES_PER_PARTITION limit on L0 files, taking the first N files sorted by max_l0_created_at
        let (mut initial_level, mut files): (Vec<ParquetFile>, Vec<ParquetFile>) = res
            .into_iter()
            .partition(|file| file.compaction_level == CompactionLevel::Initial);
        initial_level.sort_by(|a, b| a.max_l0_created_at.cmp(&b.max_l0_created_at));
        initial_level.truncate(MAX_PARQUET_L0_FILES_PER_PARTITION as usize);
        files.append(&mut initial_level);
        Ok(files)
    }

    #[allow(deprecated)]
    async fn active_as_of(&mut self, as_of: Timestamp) -> Result<Vec<ParquetFile>> {
        // read-through: this is used by the GC, so this is not overall latency-critical
        self.backing.parquet_files().active_as_of(as_of).await
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        // read-through: see https://github.com/influxdata/influxdb_iox/issues/9719
        self.backing
            .parquet_files()
            .get_by_object_store_id(object_store_id)
            .await
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        // read-through: this is used by the GC, so this is not overall latency-critical
        self.backing
            .parquet_files()
            .exists_by_object_store_id_batch(object_store_ids)
            .await
    }

    async fn exists_by_partition_and_object_store_id_batch(
        &mut self,
        ids: Vec<(PartitionId, ObjectStoreId)>,
    ) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        // We use two sources for this implementation and use the union of the them:
        //
        // 1. backing catalog:
        //    That one knows all files, no matter if they are marked for deletion or not.
        // 2. cache layer:
        //    Under certain conditions (e.g. pod dies between "backing change" and "quorum write" in
        //    `ParquetFileRepo::flag_for_delete_by_retention`) it is possible that we still cache a file even when it is
        //    marked for deletion. Since `ParquetFileRepo::delete_old_ids_only` doesn't affect the cache (we only cache
        //    non-soft-deleted files), we'll subsequently miss the hard deletion as well. To prevent the GC from
        //    deleting the file finally, we combine the two results.
        //
        // An alternative would to detect the inconsistency and force-refresh the respective cache entries. However the
        // check between the backing catalog and the cache is racy and a new file COULD be created in-between. This may
        // lead to many force-refreshs when a GC scans a partition that is under heavy write load. This may degrede
        // system performance. On the other hand, NOT deleting a few files isn't the worst.
        //
        // Also see https://github.com/influxdata/influxdb_iox/issues/12807
        let backing_result = self
            .backing
            .parquet_files()
            .exists_by_partition_and_object_store_id_batch(ids.clone())
            .await?;

        let mut cache_result = futures::stream::iter(prepare_set(tuples_to_hash(ids)))
            .map(|(p_id, os_ids)| {
                let this = &self;
                async move {
                    let snapshot = match this.partitions.get(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty::<
                                Result<(PartitionId, ObjectStoreId), Error>,
                            >()
                            .boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    // decode object store IDs
                    let files = snapshot
                        .files()
                        .map(|res| res.map(|f| f.object_store_id).map_err(Error::from))
                        .collect::<Result<HashSet<_>>>()?;

                    Ok::<_, Error>(
                        futures::stream::iter(
                            os_ids
                                .into_iter()
                                .filter(move |os_id| files.contains(os_id))
                                .map(move |os_id| Ok((p_id, os_id))),
                        )
                        .boxed(),
                    )
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<HashSet<_>>()
            .await?;

        cache_result.extend(backing_result);

        Ok(prepare_set(cache_result))
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFile>> {
        let refresh: Pin<Box<dyn Future<Output = Result<()>> + Send>> =
            if self.parquet_file_updates_delete_partition_snapshots {
                Box::pin(self.partitions.delete(partition_id))
            } else {
                Box::pin(self.partitions.refresh_batched(partition_id))
            };

        let res = self
            .backing
            .parquet_files()
            .create_upgrade_delete(partition_id, delete, upgrade, create, target_level)
            .with_refresh(refresh)
            .await?;

        Ok(res)
    }

    async fn list_by_table_id(
        &mut self,
        table_id: TableId,
        compaction_level: Option<CompactionLevel>,
    ) -> Result<Vec<ParquetFile>> {
        self.backing
            .parquet_files()
            .list_by_table_id(table_id, compaction_level)
            .await
    }

    async fn list_by_namespace_id(
        &mut self,
        namespace_id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Vec<ParquetFile>> {
        // read-through: not latency sensitive
        self.backing
            .parquet_files()
            .list_by_namespace_id(namespace_id, deleted)
            .await
    }
}

/// Prepare set of elements in deterministic order.
fn prepare_set<S, T>(set: S) -> Vec<T>
where
    S: IntoIterator<Item = T>,
    T: Eq + Ord,
{
    // ensure deterministic order (also required for de-dup)
    let mut set = set.into_iter().collect::<Vec<_>>();
    set.sort_unstable();

    // de-dup
    set.dedup();

    set
}

/// Convert an iterator of tuples into a [`HashMap`].
fn tuples_to_hash<I, K, V>(it: I) -> HashMap<K, Vec<V>>
where
    I: IntoIterator<Item = (K, V)>,
    K: Eq + Hash,
{
    let mut map = HashMap::<K, Vec<V>>::new();
    for (k, v) in it {
        map.entry(k).or_default().push(v);
    }
    map
}

/// Filter namespace according to retrieval policy.
fn filter_namespace(ns: Namespace, deleted: SoftDeletedRows) -> Option<Namespace> {
    match deleted {
        SoftDeletedRows::AllRows => Some(ns),
        SoftDeletedRows::ExcludeDeleted => ns.deleted_at.is_none().then_some(ns),
        SoftDeletedRows::OnlyDeleted => ns.deleted_at.is_some().then_some(ns),
    }
}

/// Determines if that error should lead to a refresh, see [`RefreshExt`].
trait ShouldRefresh {
    /// Should we refresh the cache state?
    fn should_refresh(&self) -> bool;
}

impl ShouldRefresh for Error {
    fn should_refresh(&self) -> bool {
        match self {
            Self::Unhandled { .. } | Self::Malformed { .. } | Self::NotImplemented { .. } => false,
            Self::AlreadyExists { .. } | Self::LimitExceeded { .. } | Self::NotFound { .. } => true,
        }
    }
}

impl<T> ShouldRefresh for CasFailure<T> {
    fn should_refresh(&self) -> bool {
        match self {
            Self::ValueMismatch(_) => true,
            Self::QueryError(e) => e.should_refresh(),
        }
    }
}

/// Add a refresh callback to a [`Future`].
///
/// This is used to refresh cache state after a future is called but BEFORE a potential error is evaluated. This is
/// important because an error can be seen as a read operation as well, e.g. in case of a [`Error::AlreadyExists`].
///
/// Since updates are optimized for the "happy path", it is OK to always refresh the cache state, even when they error.
/// This "big hammer" approach simplifies code and makes it less likely that some error conditions slip through.
///
/// # Cancellation
/// Cancellation is not handled here. That is Ok because in case of cancelled request, no information was returned to
/// the user.[^1]
///
///
/// [^1]: We assume no side-channels are (ab)used here.
trait RefreshExt {
    /// [OK](Result::Ok) output of the operation.
    type Out;

    /// [Err](Result::Err) output of the operation.
    type Err: ShouldRefresh;

    /// Add a refresh operation to the given future that will be called no matter what the result of the original future
    /// was.
    ///
    /// The return value of the resulting future is [Ok](Result::Ok) if both the update and the refresh succeeded, and
    /// [Err](Result::Err) otherwise (with the update error being returned first).
    async fn with_refresh<R>(self, refresh_fut: R) -> Result<Self::Out, Self::Err>
    where
        R: Future<Output = Result<(), Self::Err>> + Send;
}

impl<F, T, E> RefreshExt for F
where
    F: Future<Output = Result<T, E>> + Send,
    T: Send,
    E: ShouldRefresh + Send,
{
    type Out = T;
    type Err = E;

    async fn with_refresh<R>(self, refresh_fut: R) -> Result<Self::Out, Self::Err>
    where
        R: Future<Output = Result<(), Self::Err>> + Send,
    {
        let self_res = self.await;
        if let Err(e) = &self_res {
            if !e.should_refresh() {
                return self_res;
            }
        }

        let refresh_res = refresh_fut.await;

        let out = self_res?;
        refresh_res?;

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        cache::snapshot::Snapshot as _,
        mem::MemCatalog,
        test_helpers::{
            arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table,
            catalog_from_backing_and_times, CatalogAndCache, TestCatalog,
        },
    };

    use std::{
        fmt::Debug,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use catalog_cache::{CacheKey, CacheValue};
    use generated_types::{influxdata::iox::catalog_cache::v1 as proto, prost::Message};
    use iox_time::{MockProvider, SystemProvider, Time};
    use metric::{Attributes, DurationHistogram, Metric};
    use test_helpers::maybe_start_logging;
    use test_utils::AssertPendingExt;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_catalog() {
        crate::interface_tests::test_catalog(|| async { Arc::new(catalog().0) as _ }).await;
    }

    #[tokio::test]
    async fn test_catalog_metrics() {
        maybe_start_logging();

        let catalog = Arc::new(catalog().0);
        let metrics = catalog.metrics();

        let ns = catalog
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("ns").unwrap(), None, None, None)
            .await
            .unwrap();
        let table = catalog
            .repositories()
            .tables()
            .create("t", TablePartitionTemplateOverride::default(), ns.id)
            .await
            .unwrap();
        let partition = catalog
            .repositories()
            .partitions()
            .create_or_get(PartitionKey::from("k"), table.id)
            .await
            .unwrap();

        catalog
            .repositories()
            .partitions()
            .snapshot(partition.id)
            .await
            .unwrap();
        catalog
            .repositories()
            .partitions()
            .snapshot(partition.id)
            .await
            .unwrap();
        catalog
            .repositories()
            .partitions()
            .snapshot(partition.id)
            .await
            .unwrap();

        assert_metric_cache_get(&metrics, "partition", "miss", 1);
        assert_metric_cache_get(&metrics, "partition", "hit", 2);
        assert_metric_cache_put(&metrics, "partition", 1);

        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("dns_request_duration")
            .unwrap()
            .get_observer(&Attributes::from(&[("client", "catalog")]))
            .unwrap()
            .fetch();

        assert_ne!(histogram.sample_count(), 0);
    }

    #[tokio::test]
    async fn test_quorum_recovery() {
        maybe_start_logging();

        let (catalog, cache) = catalog();

        let mut repos = catalog.repositories();

        let ns = repos
            .namespaces()
            .create(&NamespaceName::new("ns").unwrap(), None, None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("t", Default::default(), ns.id)
            .await
            .unwrap();

        // Snapshot to load into cache
        let s1 = repos.tables().snapshot(table.id).await.unwrap();
        let s2 = repos.tables().snapshot(table.id).await.unwrap();
        assert_eq!(s1.generation(), s2.generation());

        // Obtain a new snapshot
        let s3 = catalog
            .inner
            .backing
            .repositories()
            .tables()
            .snapshot(table.id)
            .await
            .unwrap();
        assert_ne!(s2.generation(), s3.generation());

        // Replicate write to single node
        let encoded = proto::Table::from(s3.clone()).encode_to_vec();
        let value = CacheValue::new(encoded.into(), s3.generation());
        cache.peers()[0]
            .put(CacheKey::Table(table.id.get()), &value)
            .await
            .unwrap();

        // Remove from the local cache
        cache
            .local()
            .delete(CacheKey::Table(table.id.get()))
            .unwrap();

        // Should fail to establish quorum and obtain a new snapshot
        let s4 = repos.tables().snapshot(table.id).await.unwrap();
        assert_ne!(s4.generation(), s3.generation());
        assert_ne!(s4.generation(), s2.generation());
    }

    #[tokio::test]
    async fn test_etag() {
        let (catalog, cache) = catalog();

        let mut repos = catalog.repositories();
        repos
            .namespaces()
            .create(&NamespaceName::new("123456789").unwrap(), None, None, None)
            .await
            .unwrap();

        let key = CacheKey::Root;
        let s1 = repos.root().snapshot().await.unwrap();

        let val = cache.peers()[0].get(key).await.unwrap().unwrap();
        let etag = val.etag().unwrap();
        assert_eq!(s1.generation(), val.generation()); // Snapshot generation should match

        // Insert a new snapshot
        let next_gen = val.generation() + 1;
        let new_val =
            CacheValue::new(val.data().cloned().unwrap(), next_gen).with_etag(Arc::clone(etag));
        cache.local().insert(key, new_val).unwrap();

        // Should use local version even though remotes only have previous version
        let s2 = repos.root().snapshot().await.unwrap();
        assert_eq!(s2.generation(), next_gen);

        cache.local().delete(key).unwrap();

        // Note that the generation can travel backwards now but only for identical payloads
        let s3 = repos.root().snapshot().await.unwrap();
        assert_eq!(s1.generation(), s3.generation());
        assert_eq!(s1.to_bytes(), s3.to_bytes());
    }

    #[tokio::test]
    async fn test_refresh_ext() {
        RefreshExtTest {
            res_op: Ok("ok_op"),
            res_refresh: Ok(()),
            expected: Ok("ok_op"),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", true)),
            res_refresh: Ok(()),
            expected: Err(TestErr("err_op", true)),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", false)),
            res_refresh: Ok(()),
            expected: Err(TestErr("err_op", false)),
            refresh_called: false,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Ok("ok_op"),
            res_refresh: Err(TestErr("err_refresh", true)),
            expected: Err(TestErr("err_refresh", true)),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Ok("ok_op"),
            res_refresh: Err(TestErr("err_refresh", false)),
            expected: Err(TestErr("err_refresh", false)),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", true)),
            res_refresh: Err(TestErr("err_refresh", true)),
            expected: Err(TestErr("err_op", true)),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", false)),
            res_refresh: Err(TestErr("err_refresh", true)),
            expected: Err(TestErr("err_op", false)),
            refresh_called: false,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", true)),
            res_refresh: Err(TestErr("err_refresh", false)),
            expected: Err(TestErr("err_op", true)),
            refresh_called: true,
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err(TestErr("err_op", false)),
            res_refresh: Err(TestErr("err_refresh", false)),
            expected: Err(TestErr("err_op", false)),
            refresh_called: false,
        }
        .run()
        .await;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TestErr(&'static str, bool);

    impl ShouldRefresh for TestErr {
        fn should_refresh(&self) -> bool {
            self.1
        }
    }

    struct RefreshExtTest {
        res_op: Result<&'static str, TestErr>,
        res_refresh: Result<(), TestErr>,
        expected: Result<&'static str, TestErr>,
        refresh_called: bool,
    }

    impl RefreshExtTest {
        async fn run(self) {
            let Self {
                res_op,
                res_refresh,
                expected,
                refresh_called,
            } = self;

            let called_op = Arc::new(AtomicBool::new(false));
            let called_op_captured = Arc::clone(&called_op);
            let called_op_captured2 = Arc::clone(&called_op);
            let op = async move {
                called_op_captured.store(true, Ordering::SeqCst);
                res_op
            };

            let called_refresh = Arc::new(AtomicBool::new(false));
            let called_refresh_captured = Arc::clone(&called_refresh);
            let refresh = async move {
                assert!(called_op_captured2.load(Ordering::SeqCst));
                called_refresh_captured.store(true, Ordering::SeqCst);
                res_refresh
            };

            let actual = op.with_refresh(refresh).await;
            assert_eq!(actual, expected);

            assert!(called_op.load(Ordering::SeqCst));
            assert_eq!(called_refresh.load(Ordering::SeqCst), refresh_called);
        }
    }

    #[tokio::test]
    async fn test_write_batching() {
        // maybe_start_logging();

        let batch_delay = Duration::from_secs(1);
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));
        let (catalog, _cache) = catalog_with_params(Arc::clone(&time_provider) as _, batch_delay);

        let mut repos = catalog.repositories();

        let ns = arbitrary_namespace(&mut *repos, "ns").await;
        let table = arbitrary_table(&mut *repos, "t", &ns).await;

        // check PUT metrics:
        // - root:
        //   - created namespace
        // - namespace:
        //   - created namespace
        //   - created table
        // - table:
        //   - created table
        assert_metric_cache_put(&catalog.metrics(), "root", 1);
        assert_metric_cache_put(&catalog.metrics(), "namespace", 2);
        assert_metric_cache_put(&catalog.metrics(), "table", 1);

        let key = PartitionKey::from("p");

        let mut repos_1 = catalog.repositories();
        let mut fut_1 = repos_1.partitions().create_or_get(key.clone(), table.id);
        fut_1.assert_pending().await;

        let mut repos_2 = catalog.repositories();
        let mut fut_2 = repos_2.partitions().create_or_get(key.clone(), table.id);
        fut_2.assert_pending().await;

        time_provider.inc(batch_delay);

        let partition_1 = fut_1.await.unwrap();
        let partition_2 = fut_2.await.unwrap();
        assert_eq!(partition_1, partition_2);

        // We perform two ops to the backing catalog (to avoid combining failures) but only one additional quorum PUT
        // (for the table)
        //
        // Note that we don't perform a quorum PUT for the partition since we `create_or_get` is often used as a "get"
        // instead of "create" and we don't always want to issue a "load" for that.
        assert_metric_mem_catalog(&catalog.metrics(), "partition_create_or_get", 2);
        assert_metric_cache_put(&catalog.metrics(), "root", 1);
        assert_metric_cache_put(&catalog.metrics(), "namespace", 2);
        assert_metric_cache_put(&catalog.metrics(), "table", 2);
        assert_metric_cache_put(&catalog.metrics(), "partition", 0);

        let params_1 = arbitrary_parquet_file_params(&ns, &table, &partition_1);
        let params_2 = ParquetFileParams {
            object_store_id: ObjectStoreId::new(),
            ..params_1.clone()
        };

        let mut repos_1 = catalog.repositories();
        let create_1 = vec![params_1.clone()];
        let mut fut_1 = repos_1.parquet_files().create_upgrade_delete(
            partition_1.id,
            &[],
            &[],
            &create_1,
            CompactionLevel::Initial,
        );
        fut_1.assert_pending().await;

        let mut repos_2 = catalog.repositories();
        let create_2 = vec![params_2.clone()];
        let mut fut_2 = repos_2.parquet_files().create_upgrade_delete(
            partition_2.id,
            &[],
            &[],
            &create_2,
            CompactionLevel::Initial,
        );
        fut_2.assert_pending().await;

        time_provider.inc(batch_delay);

        fut_1.await.unwrap();
        fut_2.await.unwrap();

        assert_metric_mem_catalog(&catalog.metrics(), "parquet_create_upgrade_delete", 2);
        assert_metric_cache_put(&catalog.metrics(), "root", 1);
        assert_metric_cache_put(&catalog.metrics(), "namespace", 2);
        assert_metric_cache_put(&catalog.metrics(), "table", 2);
        assert_metric_cache_put(&catalog.metrics(), "partition", 1);

        let files = repos
            .parquet_files()
            .list_by_partition_not_to_delete_batch(vec![partition_1.id])
            .await
            .unwrap();
        let mut file_uuids_actual = files.iter().map(|f| f.object_store_id).collect::<Vec<_>>();
        file_uuids_actual.sort();
        let mut file_uuids_expected = vec![params_1.object_store_id, params_2.object_store_id];
        file_uuids_expected.sort();
        assert_eq!(file_uuids_actual, file_uuids_expected);
    }

    fn catalog() -> (TestCatalog<CachingCatalog>, Arc<QuorumCatalogCache>) {
        catalog_with_params(Arc::new(SystemProvider::new()), Duration::ZERO)
    }

    fn catalog_with_params(
        time_provider: Arc<dyn TimeProvider>,
        batch_delay: Duration,
    ) -> CatalogAndCache {
        catalog_from_backing_and_times(
            MemCatalog::new(Arc::default(), Arc::clone(&time_provider)),
            time_provider,
            batch_delay,
        )
    }

    #[track_caller]
    fn assert_metric_cache_get(
        metrics: &metric::Registry,
        variant: &'static str,
        result: &'static str,
        expected: u64,
    ) {
        let actual = metrics
            .get_instrument::<Metric<DurationHistogram>>("iox_catalog_cache_op")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("op", "get"),
                ("variant", variant),
                ("result", result),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(actual.sample_count(), expected);
    }

    #[track_caller]
    fn assert_metric_cache_put(metrics: &metric::Registry, variant: &'static str, expected: u64) {
        let actual = metrics
            .get_instrument::<Metric<DurationHistogram>>("iox_catalog_cache_op")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", "put"), ("variant", variant)]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(actual.sample_count(), expected);
    }

    #[track_caller]
    fn assert_metric_mem_catalog(metrics: &metric::Registry, op: &'static str, expected: u64) {
        let actual = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("type", "memory"),
                ("op", op),
                ("result", "success"),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(actual.sample_count(), expected);
    }
}
