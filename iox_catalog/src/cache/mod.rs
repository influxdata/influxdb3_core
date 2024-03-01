//! Cache layer.

mod handler;
mod loader;
mod snapshot;

use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use backoff::BackoffConfig;
use catalog_cache::api::quorum::QuorumCatalogCache;
use data_types::snapshot::namespace::NamespaceSnapshot;
use data_types::snapshot::root::RootSnapshot;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::partition::PartitionSnapshot,
    snapshot::table::TableSnapshot,
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, Timestamp,
};
use futures::{Future, StreamExt, TryFutureExt, TryStreamExt};

use iox_time::TimeProvider;

use trace::ctx::SpanContext;

use crate::interface::RootRepo;
use crate::{
    interface::{
        CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
        RepoCollection, Result, SoftDeletedRows, TableRepo,
    },
    metrics::MetricDecorator,
};

use self::handler::CacheHandler;
use self::snapshot::RootKey;

/// Caching catalog.
#[derive(Debug)]
pub struct CachingCatalog {
    backing: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
    time_provider: Arc<dyn TimeProvider>,
    quorum_fanout: usize,
    partitions: Arc<CacheHandler<PartitionSnapshot>>,
    tables: Arc<CacheHandler<TableSnapshot>>,
    namespaces: Arc<CacheHandler<NamespaceSnapshot>>,
    root: Arc<CacheHandler<RootSnapshot>>,
}

impl CachingCatalog {
    /// Create new caching catalog.
    ///
    /// Sets:
    /// - `cache`: quorum-based cache
    /// - `backing`: underlying backing catalog
    /// - `metrics`: metrics registry
    /// - `time_provider`: time provider, used for metrics
    /// - `quorum_fanout`: number of concurrent quorum operations that a single request can trigger
    pub fn new(
        cache: Arc<QuorumCatalogCache>,
        backing: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
        quorum_fanout: usize,
    ) -> Self {
        // We set a more aggressive backoff configuration than normal as we expect latencies to be low
        let backoff_config = Arc::new(BackoffConfig {
            init_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            base: 5.0,
            deadline: Some(Duration::from_millis(750)),
        });

        let partitions = Arc::new(CacheHandler::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
        ));
        let tables = Arc::new(CacheHandler::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
        ));
        let namespaces = Arc::new(CacheHandler::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
        ));
        let root = Arc::new(CacheHandler::new(
            Arc::clone(&backing),
            &metrics,
            Arc::clone(&cache),
            Arc::clone(&backoff_config),
        ));

        Self {
            backing,
            metrics,
            time_provider,
            quorum_fanout,
            partitions,
            tables,
            namespaces,
            root,
        }
    }
}

impl std::fmt::Display for CachingCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "caching")
    }
}

#[async_trait]
impl Catalog for CachingCatalog {
    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            Box::new(Repos {
                backing: Arc::clone(&self.backing),
                quorum_fanout: self.quorum_fanout,
                tables: Arc::clone(&self.tables),
                partitions: Arc::clone(&self.partitions),
                namespaces: Arc::clone(&self.namespaces),
                root: Arc::clone(&self.root),
            }),
            Arc::clone(&self.metrics),
            self.time_provider(),
            "cache",
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[derive(Debug)]
struct Repos {
    partitions: Arc<CacheHandler<PartitionSnapshot>>,
    tables: Arc<CacheHandler<TableSnapshot>>,
    namespaces: Arc<CacheHandler<NamespaceSnapshot>>,
    root: Arc<CacheHandler<RootSnapshot>>,
    backing: Arc<dyn Catalog>,
    quorum_fanout: usize,
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

    fn set_span_context(&mut self, _span_ctx: Option<SpanContext>) {}
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
            .repositories()
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
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let ns_id = self.namespace_id_by_name(name).await?;

        let ns = self
            .backing
            .repositories()
            .namespaces()
            .update_retention_period(name, retention_period_ns)
            .with_refresh(self.namespaces.refresh(ns_id))
            .await?;

        assert_eq!(ns.id, ns_id);

        Ok(ns)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let root = self.root.get(RootKey).await?;
        let namespaces = root.namespaces().collect::<Result<Vec<_>, _>>()?;

        futures::stream::iter(namespaces)
            .map(|n| {
                let this = &self;
                async move {
                    let snapshot = match this.namespaces.get(n.id()).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    let ns = snapshot.namespace()?;

                    match filter_namespace(ns, deleted) {
                        Some(ns) => Ok(futures::stream::once(async move { Ok(ns) }).boxed()),
                        None => Ok(futures::stream::empty().boxed()),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
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

    async fn soft_delete(&mut self, name: &str) -> Result<NamespaceId> {
        let ns_id = self.namespace_id_by_name(name).await?;

        let id = self
            .backing
            .repositories()
            .namespaces()
            .soft_delete(name)
            .with_refresh(self.namespaces.refresh(ns_id))
            .await?;

        assert_eq!(id, ns_id);

        Ok(id)
    }

    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace> {
        let ns_id = self.namespace_id_by_name(name).await?;

        let ns = self
            .backing
            .repositories()
            .namespaces()
            .update_table_limit(name, new_max)
            .with_refresh(self.namespaces.refresh(ns_id))
            .await?;

        assert_eq!(ns.id, ns_id);

        Ok(ns)
    }

    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        let ns_id = self.namespace_id_by_name(name).await?;

        let ns = self
            .backing
            .repositories()
            .namespaces()
            .update_column_limit(name, new_max)
            .with_refresh(self.namespaces.refresh(ns_id))
            .await?;

        assert_eq!(ns.id, ns_id);

        Ok(ns)
    }

    async fn snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot> {
        self.namespaces.get(namespace_id).await
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
            .repositories()
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

    async fn list(&mut self) -> Result<Vec<Table>> {
        // read-through: global-scoped, only used by parquet cache during startup
        self.backing.repositories().tables().list().await
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
            .repositories()
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
            .repositories()
            .columns()
            .create_or_get_many_unchecked(table_id, columns)
            .with_refresh(self.tables.refresh(table_id))
            .await?;

        Ok(r)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        // read-through: users should use `list_by_table_id`, see https://github.com/influxdata/influxdb_iox/issues/10146
        self.backing
            .repositories()
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
        self.backing.repositories().columns().list().await
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
            .repositories()
            .partitions()
            .create_or_get(key, table_id)
            .with_refresh(self.tables.refresh(table_id))
            .await?;

        Ok(p)
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
        self.backing.repositories().partitions().list_ids().await
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        let res = self
            .backing
            .repositories()
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
            .repositories()
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
        self.backing
            .repositories()
            .partitions()
            .list_skipped_compactions()
            .await
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let res = self
            .backing
            .repositories()
            .partitions()
            .delete_skipped_compactions(partition_id)
            .with_refresh(self.partitions.refresh(partition_id))
            .await?;

        Ok(res)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        // read-through: used for ingester warm-up at the moment
        self.backing
            .repositories()
            .partitions()
            .most_recent_n(n)
            .await
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        // read-through: used by the compactor for scheduling, we should eventually find a better interface
        self.backing
            .repositories()
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
            .repositories()
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
            .repositories()
            .partitions()
            .update_cold_compact(partition_id, cold_compact_at)
            .await
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        // read-through: used by the ingester due to hash-id stuff
        self.backing
            .repositories()
            .partitions()
            .list_old_style()
            .await
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        self.partitions.get(partition_id).await
    }
}

#[async_trait]
impl ParquetFileRepo for Repos {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let res = self
            .backing
            .repositories()
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
                    this.partitions.refresh(p_id).await?;
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
            .repositories()
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        futures::stream::iter(prepare_set(partition_ids))
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
            .await
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        // read-through: see https://github.com/influxdata/influxdb_iox/issues/9719
        self.backing
            .repositories()
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
            .repositories()
            .parquet_files()
            .exists_by_object_store_id_batch(object_store_ids)
            .await
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let res = self
            .backing
            .repositories()
            .parquet_files()
            .create_upgrade_delete(partition_id, delete, upgrade, create, target_level)
            .with_refresh(self.partitions.refresh(partition_id))
            .await?;

        Ok(res)
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

/// Filter namespace according to retrieval policy.
fn filter_namespace(ns: Namespace, deleted: SoftDeletedRows) -> Option<Namespace> {
    match deleted {
        SoftDeletedRows::AllRows => Some(ns),
        SoftDeletedRows::ExcludeDeleted => ns.deleted_at.is_none().then_some(ns),
        SoftDeletedRows::OnlyDeleted => ns.deleted_at.is_some().then_some(ns),
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
    type Err;

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
    E: Send,
{
    type Out = T;
    type Err = E;

    async fn with_refresh<R>(self, refresh_fut: R) -> Result<Self::Out, Self::Err>
    where
        R: Future<Output = Result<(), Self::Err>> + Send,
    {
        let self_res = self.await;
        let refresh_res = refresh_fut.await;

        let out = self_res?;
        refresh_res?;

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use catalog_cache::local::CatalogCache;
    use catalog_cache::CacheValue;
    use catalog_cache::{api::server::test_util::TestCacheServer, CacheKey};
    use generated_types::influxdata::iox::catalog_cache::v1 as proto;
    use generated_types::prost::Message;
    use iox_time::SystemProvider;
    use metric::{Attributes, DurationHistogram, Metric, U64Counter};
    use test_helpers::maybe_start_logging;

    use crate::{interface_tests::TestCatalog, mem::MemCatalog};

    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
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

        assert_metric_get(&metrics, "partition", "miss", 1);
        assert_metric_get(&metrics, "partition", "hit", 2);

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
            .inner()
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
    async fn test_refresh_ext() {
        RefreshExtTest {
            res_op: Ok("ok_op"),
            res_refresh: Ok(()),
            expected: Ok("ok_op"),
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err("err_op"),
            res_refresh: Ok(()),
            expected: Err("err_op"),
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Ok("ok_op"),
            res_refresh: Err("err_refresh"),
            expected: Err("err_refresh"),
        }
        .run()
        .await;

        RefreshExtTest {
            res_op: Err("err_op"),
            res_refresh: Err("err_refresh"),
            expected: Err("err_op"),
        }
        .run()
        .await;
    }

    struct RefreshExtTest {
        res_op: Result<&'static str, &'static str>,
        res_refresh: Result<(), &'static str>,
        expected: Result<&'static str, &'static str>,
    }

    impl RefreshExtTest {
        async fn run(self) {
            let Self {
                res_op,
                res_refresh,
                expected,
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
            assert!(called_refresh.load(Ordering::SeqCst));
        }
    }

    fn catalog() -> (TestCatalog<CachingCatalog>, Arc<QuorumCatalogCache>) {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::new()) as _;
        let backing = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));

        // use new metrics registry so the two layers don't double-count
        let metrics = Arc::new(metric::Registry::default());

        let peer0 = TestCacheServer::bind_ephemeral(&metrics);
        let peer1 = TestCacheServer::bind_ephemeral(&metrics);
        let cache = Arc::new(QuorumCatalogCache::new(
            Arc::new(CatalogCache::default()),
            Arc::new([peer0.client(), peer1.client()]),
        ));

        let caching_catalog =
            CachingCatalog::new(Arc::clone(&cache), backing, metrics, time_provider, 10);

        let test_catalog = TestCatalog::new(caching_catalog);
        test_catalog.hold_onto(peer0);
        test_catalog.hold_onto(peer1);
        (test_catalog, cache)
    }

    #[track_caller]
    fn assert_metric_get(
        metrics: &metric::Registry,
        variant: &'static str,
        result: &'static str,
        expected: u64,
    ) {
        let actual = metrics
            .get_instrument::<Metric<U64Counter>>("iox_catalog_cache_get")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("variant", variant),
                ("result", result),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(actual, expected);
    }
}
