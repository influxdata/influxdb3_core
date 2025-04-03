//! Fault injection for testing purposes.

#![expect(deprecated)] // injecting faults into deprecated trait methods is fine

use crate::interface::{
    CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, NamespaceSorting, Paginated,
    PaginationOptions, ParquetFileRepo, PartitionRepo, RepoCollection, Result, RootRepo,
    SoftDeletedRows, TableRepo, TableSorting,
};
use async_trait::async_trait;
use data_types::snapshot::namespace::NamespaceSnapshot;
use data_types::snapshot::root::RootSnapshot;
use data_types::snapshot::table::TableSnapshot;
use data_types::{
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction, SortKeyIds, Table,
    TableId, Timestamp,
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::partition::PartitionSnapshot,
};
use data_types::{NamespaceWithStorage, TableWithStorage};
use iox_time::TimeProvider;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};
use trace::ctx::SpanContext;

#[derive(Debug)]
struct Fault(Error);

impl From<Fault> for Error {
    fn from(fault: Fault) -> Self {
        fault.0
    }
}

impl<T> From<Fault> for CasFailure<T> {
    fn from(fault: Fault) -> Self {
        Self::QueryError(fault.0)
    }
}

type FaultResult = Result<(), Fault>;

#[derive(Debug, Clone, Default)]
struct State(Arc<Mutex<HashMap<FaultPoint, Vec<FaultResult>>>>);

impl State {
    fn check(&self, point: FaultPoint) -> FaultResult {
        self.0
            .lock()
            .get_mut(&point)
            .and_then(|faults| (!faults.is_empty()).then(|| faults.remove(0)))
            .unwrap_or(Ok(()))
    }
}

/// A wrapper around an existing [`Catalog`] that enables [Fault Injection]
///
/// Use [`set_result`](Self::set_result) to inject faults.
///
///
/// [Fault Injection]: https://en.wikipedia.org/wiki/Fault_injection
pub struct FaultCatalog {
    inner: Arc<dyn Catalog>,
    state: State,
}

impl FaultCatalog {
    /// Create new fault wrapper with no faults.
    pub fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            state: State::default(),
        }
    }

    /// Append result for a given fault point.
    ///
    /// Whenever a fault point is hit, a result is consumed.
    ///
    /// If a fault point has no results left, it just passes.
    pub fn set_result(&self, point: FaultPoint, res: Result<(), Error>) {
        self.state
            .0
            .lock()
            .entry(point)
            .or_default()
            .push(res.map_err(Fault));
    }
}

impl std::fmt::Debug for FaultCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fault({:?})", self.inner)
    }
}

#[async_trait]
impl Catalog for FaultCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn setup(&self) -> Result<()> {
        self.state.check(FaultPoint::CatalogSetupPre)?;
        let res = self.inner.setup().await;
        self.state.check(FaultPoint::CatalogSetupPost)?;
        res
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(Repos {
            inner: self.inner.repositories(),
            state: self.state.clone(),
        })
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        self.inner.metrics()
    }

    async fn get_time(&self) -> Result<iox_time::Time> {
        self.state.check(FaultPoint::CatalogGetTimePre)?;
        let res = self.inner.get_time().await;
        self.state.check(FaultPoint::CatalogGetTimePost)?;
        res
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        self.inner.time_provider()
    }

    async fn active_applications(&self) -> Result<HashSet<String>> {
        self.state.check(FaultPoint::CatalogActiveApplicationsPre)?;
        let res = self.inner.active_applications().await;
        self.state
            .check(FaultPoint::CatalogActiveApplicationsPost)?;
        res
    }

    fn name(&self) -> &'static str {
        // some metrics tests rely on this name, so just forward the inner catalog that actually emits the metrics
        self.inner.name()
    }
}

#[derive(Debug)]
struct Repos {
    inner: Box<dyn RepoCollection>,
    state: State,
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
        self.inner.set_span_context(span_ctx);
    }
}

macro_rules! generate {
    {
        $({
            impl_trait = $trait:ident,
            repo = $repo:ident,
            methods = [$(
                $method:ident(
                    &mut self $(,)?
                    $($arg:ident : $t:ty),*
                ) -> Result<$out:ty$(, $err:ty)?>;
            )+] $(,)?
        }),* $(,)?
    } => {
        paste::paste! {
            /// Describe where to insert the fault.
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
            pub enum FaultPoint {
                /// Fault before calling [`Catalog::setup`].
                CatalogSetupPre,

                /// Fault after calling [`Catalog::setup`].
                CatalogSetupPost,

                /// Fault before calling [`Catalog::get_time`].
                CatalogGetTimePre,

                /// Fault after calling [`Catalog::get_time`].
                CatalogGetTimePost,

                /// Fault before calling [`Catalog::active_applications`].
                CatalogActiveApplicationsPre,

                /// Fault after calling [`Catalog::active_applications`].
                CatalogActiveApplicationsPost,

                $(
                    $(
                        #[doc = "Fault before calling [`" $trait "::" $method "`]"]
                        [<$repo:camel $method:camel Pre>],

                        #[doc = "Fault after calling [`" $trait "::" $method "`]"]
                        [<$repo:camel $method:camel Post>],
                    )+
                )*
            }

            $(
                #[async_trait]
                impl $trait for Repos {
                    /// NOTE: if you're seeing an error here about "not all trait items
                    /// implemented" or something similar, one or more methods are
                    /// missing from / incorrectly defined in the decorate!() blocks
                    /// below.

                    $(
                        async fn $method(&mut self, $($arg : $t),*) -> Result<$out$(, $err)?> {
                            self.state.check(FaultPoint::[<$repo:camel $method:camel Pre>])?;
                            let res = self.inner.$repo()
                                .$method($($arg),*)
                                .await;
                            self.state.check(FaultPoint::[<$repo:camel $method:camel Post>])?;
                            res
                        }
                    )+
                }
            )*
        }
    };
}

generate! {
    {
        impl_trait = RootRepo,
        repo = root,
        methods = [
            snapshot(&mut self) -> Result<RootSnapshot>;
        ],
    },
    {
        impl_trait = NamespaceRepo,
        repo = namespaces,
        methods = [
            create(&mut self, name: &NamespaceName<'_>, partition_template: Option<NamespacePartitionTemplateOverride>, retention_period_ns: Option<i64>, service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>) -> Result<Namespace>;
            update_retention_period(&mut self, id: NamespaceId, retention_period_ns: Option<i64>) -> Result<Namespace>;
            list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;
            list_storage(&mut self, sorting: Option<NamespaceSorting>, pagination: Option<PaginationOptions>) -> Result<Paginated<NamespaceWithStorage>>;
            get_by_id(&mut self, id: NamespaceId, deleted: SoftDeletedRows) -> Result<Option<Namespace>>;
            get_by_name(&mut self, name: &str) -> Result<Option<Namespace>>;
            soft_delete(&mut self, id: NamespaceId) -> Result<Namespace>;
            update_table_limit(&mut self, id: NamespaceId, new_max: MaxTables) -> Result<Namespace>;
            update_column_limit(&mut self, id: NamespaceId, new_max: MaxColumnsPerTable) -> Result<Namespace>;
            snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot>;
            snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot>;
            get_storage_by_id(&mut self, id: NamespaceId) -> Result<Option<NamespaceWithStorage>>;
            rename(&mut self, id: NamespaceId, new_name: NamespaceName<'_>) -> Result<Namespace>;
            undelete(&mut self, id: NamespaceId) -> Result<Namespace>;
        ],
    },
    {
        impl_trait = TableRepo,
        repo = tables,
        methods = [
            create(&mut self, name: &str, partition_template: TablePartitionTemplateOverride, namespace_id: NamespaceId) -> Result<Table>;
            get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;
            get_storage_by_id(&mut self, table_id: TableId) -> Result<Option<TableWithStorage>>;
            get_by_namespace_and_name(&mut self, namespace_id: NamespaceId, name: &str) -> Result<Option<Table>>;
            list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;
            list_storage_by_namespace_id(&mut self, namespace_id: NamespaceId, sorting: Option<TableSorting>, pagination: Option<PaginationOptions>) -> Result<Paginated<TableWithStorage>>;
            list(&mut self) -> Result<Vec<Table>>;
            snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot>;
            list_by_iceberg_enabled(&mut self, _namespace_id: NamespaceId) -> Result<Vec<TableId>>;
            enable_iceberg(&mut self, table_id: TableId) -> Result<()>;
            disable_iceberg(&mut self, table_id: TableId) -> Result<()>;
            soft_delete(&mut self, id: TableId) -> Result<Table>;
            rename(&mut self, id: TableId, new_name: &str) -> Result<Table>;
            undelete(&mut self, id: TableId) -> Result<Table>;
        ],
    },
    {
        impl_trait = ColumnRepo,
        repo = columns,
        methods = [
            create_or_get(&mut self, name: &str, table_id: TableId, column_type: ColumnType) -> Result<Column>;
            list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
            list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;
            create_or_get_many_unchecked(&mut self, table_id: TableId, columns: HashMap<&str, ColumnType>) -> Result<Vec<Column>>;
            list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Column>>;
        ],
    },
    {
        impl_trait = PartitionRepo,
        repo = partitions,
        methods = [
            create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;
            set_new_file_at(&mut self, partition_id: PartitionId, new_file_at: Timestamp) -> Result<()>;
            get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>>;
            list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;
            list_ids(&mut self) -> Result<Vec<PartitionId>>;
            cas_sort_key(&mut self, partition_id: PartitionId, old_sort_key_ids: Option<&SortKeyIds>, new_sort_key_ids: &SortKeyIds) -> Result<Partition, CasFailure<SortKeyIds>>;
            record_skipped_compaction(&mut self, partition_id: PartitionId, reason: &str, num_files: usize, limit_num_files: usize, limit_num_files_first_in_partition: usize, estimated_bytes: u64, limit_bytes: u64) -> Result<()>;
            list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;
            delete_skipped_compactions(&mut self, partition_id: PartitionId) -> Result<Option<SkippedCompaction>>;
            most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;
            partitions_new_file_between(&mut self, minimum_time: Timestamp, maximum_time: Option<Timestamp>) -> Result<Vec<PartitionId>>;
            partitions_needing_cold_compact(&mut self, maximum_time: Timestamp, n: usize) -> Result<Vec<PartitionId>>;
            update_cold_compact(&mut self, partition_id: PartitionId, cold_compact_at: Timestamp) -> Result<()>;
            get_in_skipped_compactions(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<SkippedCompaction>>;
            list_old_style(&mut self) -> Result<Vec<Partition>>;
            delete_by_retention(&mut self, partition_cutoff: Duration) -> Result<Vec<(TableId, PartitionId)>>;
            snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot>;
            snapshot_generation(&mut self, partition_id: PartitionId) -> Result<u64>;
        ],
    },
    {
        impl_trait = ParquetFileRepo,
        repo = parquet_files,
        methods = [
            flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>>;
            delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>>;
            delete_old_ids_count(&mut self, older_than: Timestamp, limit: u32) -> Result<u64>;
            list_by_partition_not_to_delete_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<ParquetFile>>;
            active_as_of(&mut self, as_of: Timestamp) -> Result<Vec<ParquetFile>>;
            get_by_object_store_id(&mut self, object_store_id: ObjectStoreId) -> Result<Option<ParquetFile>>;
            exists_by_object_store_id_batch(&mut self, object_store_ids: Vec<ObjectStoreId>) -> Result<Vec<ObjectStoreId>>;
            exists_by_partition_and_object_store_id_batch(&mut self, ids: Vec<(PartitionId, ObjectStoreId)>) -> Result<Vec<(PartitionId, ObjectStoreId)>>;
            create_upgrade_delete(&mut self, partition_id: PartitionId, delete: &[ObjectStoreId], upgrade: &[ObjectStoreId], create: &[ParquetFileParams], target_level: CompactionLevel) -> Result<Vec<ParquetFile>>;
            list_by_table_id(&mut self, _table_id: TableId, _compaction_level: Option<CompactionLevel>) -> Result<Vec<ParquetFile>>;
            list_by_namespace_id(&mut self, _namespace_id: NamespaceId, deleted: SoftDeletedRows) -> Result<Vec<ParquetFile>>;
        ],
    },
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use iox_time::SystemProvider;

    use crate::mem::MemCatalog;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_catalog() {
        crate::interface_tests::test_catalog(async || {
            let metrics = Arc::new(metric::Registry::default());
            let time_provider = Arc::new(SystemProvider::new());

            let mem_catalog = Arc::new(MemCatalog::new(metrics, time_provider));
            Arc::new(FaultCatalog::new(mem_catalog)) as Arc<dyn Catalog>
        })
        .await;
    }

    #[tokio::test]
    async fn test_fault_injection_setup() {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::new());

        let mem_catalog = Arc::new(MemCatalog::new(metrics, time_provider));
        let catalog = Arc::new(FaultCatalog::new(mem_catalog));

        catalog.set_result(FaultPoint::CatalogSetupPre, Ok(()));
        catalog.set_result(
            FaultPoint::CatalogSetupPre,
            Err(Error::NotImplemented {
                descr: "foo".to_owned(),
            }),
        );

        catalog.setup().await.unwrap();
        assert_matches!(
            catalog.setup().await.unwrap_err(),
            Error::NotImplemented { .. }
        );

        // no results left
        catalog.setup().await.unwrap();
    }

    #[tokio::test]
    async fn test_fault_injection_root_snapshot() {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::new());

        let mem_catalog = Arc::new(MemCatalog::new(metrics, time_provider));
        let catalog = Arc::new(FaultCatalog::new(mem_catalog));

        catalog.set_result(FaultPoint::RootSnapshotPre, Ok(()));
        catalog.set_result(
            FaultPoint::RootSnapshotPre,
            Err(Error::NotImplemented {
                descr: "foo".to_owned(),
            }),
        );

        catalog.repositories().root().snapshot().await.unwrap();
        assert_matches!(
            catalog.repositories().root().snapshot().await.unwrap_err(),
            Error::NotImplemented { .. }
        );

        // no results left
        catalog.repositories().root().snapshot().await.unwrap();
    }

    #[tokio::test]
    async fn test_fault_injection_pre_post() {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::new());

        let mem_catalog = Arc::new(MemCatalog::new(metrics, time_provider));
        let catalog = Arc::new(FaultCatalog::new(mem_catalog));

        catalog.set_result(
            FaultPoint::NamespacesCreatePre,
            Err(Error::NotImplemented {
                descr: "foo".to_owned(),
            }),
        );
        catalog.set_result(
            FaultPoint::NamespacesCreatePost,
            Err(Error::NotImplemented {
                descr: "foo".to_owned(),
            }),
        );

        let ns = NamespaceName::new("ns").unwrap();

        // pre: does NOT create
        assert_matches!(
            catalog
                .repositories()
                .namespaces()
                .create(&ns, None, None, None)
                .await
                .unwrap_err(),
            Error::NotImplemented { .. }
        );
        assert!(
            catalog
                .repositories()
                .namespaces()
                .get_by_name(&ns)
                .await
                .unwrap()
                .is_none()
        );

        // post: DOES create
        assert_matches!(
            catalog
                .repositories()
                .namespaces()
                .create(&ns, None, None, None)
                .await
                .unwrap_err(),
            Error::NotImplemented { .. }
        );
        assert!(
            catalog
                .repositories()
                .namespaces()
                .get_by_name(&ns)
                .await
                .unwrap()
                .is_some()
        );
    }
}
