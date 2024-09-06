//! Metric instrumentation for catalog implementations.

use crate::interface::{
    CasFailure, ColumnRepo, NamespaceRepo, ParquetFileRepo, PartitionRepo, RepoCollection, Result,
    RootRepo, SoftDeletedRows, TableRepo,
};
use async_trait::async_trait;
use data_types::snapshot::namespace::NamespaceSnapshot;
use data_types::snapshot::root::RootSnapshot;
use data_types::snapshot::table::TableSnapshot;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::partition::PartitionSnapshot,
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, Timestamp,
};
use iox_time::TimeProvider;
use metric::{DurationHistogram, Metric};
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use trace::{
    ctx::SpanContext,
    span::{SpanExt, SpanRecorder},
};

/// Catalog metrics.
#[derive(Debug)]
pub struct CatalogMetrics {
    state: Arc<MetricState>,
    registry: Arc<metric::Registry>,
}

impl CatalogMetrics {
    /// Create new metrics object.
    ///
    /// This object should be cached/stored since creating it is potentially expensive.
    pub fn new(
        metrics: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
        catalog_type: &'static str,
    ) -> Self {
        Self {
            state: Arc::new(MetricState::new(time_provider, &metrics, catalog_type)),
            registry: metrics,
        }
    }

    /// Access to underlying metric registry.
    pub fn registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.registry)
    }

    /// Create new [`RepoCollection`].
    pub fn repos(&self, inner: Box<dyn RepoCollection>) -> MetricDecorator {
        MetricDecorator {
            inner,
            state: Arc::clone(&self.state),
            span_ctx: None,
        }
    }
}

/// Decorates a implementation of the catalog's [`RepoCollection`] (and the
/// transactional variant) with instrumentation that emits latency histograms
/// for each method.
///
/// Values are recorded under the `catalog_op_duration` metric, labelled by
/// operation name and result (success/error).
#[derive(Debug)]
pub struct MetricDecorator {
    inner: Box<dyn RepoCollection>,
    state: Arc<MetricState>,
    span_ctx: Option<SpanContext>,
}

impl RepoCollection for MetricDecorator {
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
        self.span_ctx = span_ctx;
    }
}

#[derive(Debug)]
struct MethodMetric {
    success: DurationHistogram,
    error: DurationHistogram,
}

impl MethodMetric {
    fn new(
        observer: &Metric<DurationHistogram>,
        catalog_type: &'static str,
        op: &'static str,
    ) -> Self {
        Self {
            success: observer.recorder(&[
                ("type", catalog_type),
                ("op", op),
                ("result", "success"),
            ]),
            error: observer.recorder(&[("type", catalog_type), ("op", op), ("result", "error")]),
        }
    }

    fn record(&self, d: Duration, ok: bool) {
        if ok {
            self.success.record(d);
        } else {
            self.error.record(d);
        }
    }
}

/// Emit a trait impl for `impl_trait` that delegates calls to the inner
/// implementation, recording the duration and result to the metrics registry.
///
/// Format:
///
/// ```ignore
///     decorate!(
///         impl_trait = <trait name>,
///         repo = <repo name>,
///         methods = [
///             "<metric name>" = <method signature>;
///             "<metric name>" = <method signature>;
///             // ... and so on
///         ]
///     );
/// ```
///
/// All methods of a given trait MUST be defined in the `decorate!()` call so
/// they are all instrumented or the decorator will not compile as it won't
/// fully implement the trait.
macro_rules! decorate {
    {
        $({
            impl_trait = $trait:ident,
            repo = $repo:ident,
            methods = [$(
                $metric:tt = $method:ident(
                    &mut self $(,)?
                    $($arg:ident : $t:ty),*
                ) -> Result<$out:ty$(, $err:ty)?>;
            )+] $(,)?
        }),* $(,)?
    } => {
        /// State passed to the individual [`MetricDecorator`]s.
        #[derive(Debug)]
        struct MetricState {
            time_provider: Arc<dyn TimeProvider>,
            catalog_type: &'static str,

            $(
                $(
                    $metric: MethodMetric,
                )+
            )*
        }

        impl MetricState {
            fn new(
                time_provider: Arc<dyn TimeProvider>,
                registry: &metric::Registry,
                catalog_type: &'static str,
            ) -> Self {
                let observer = registry.register_metric("catalog_op_duration", "catalog call duration");

                Self {
                    time_provider,
                    catalog_type,
                    $(
                        $(
                            $metric: MethodMetric::new(&observer, catalog_type, stringify!($metric)),
                        )+
                    )*
                }
            }
        }

        $(
            #[async_trait]
            impl $trait for MetricDecorator {
                /// NOTE: if you're seeing an error here about "not all trait items
                /// implemented" or something similar, one or more methods are
                /// missing from / incorrectly defined in the decorate!() blocks
                /// below.

                $(
                    async fn $method(&mut self, $($arg : $t),*) -> Result<$out$(, $err)?> {
                        let t = self.state.time_provider.now();

                        let mut span_recorder = SpanRecorder::new(self.span_ctx.child_span(stringify!($metric)));
                        span_recorder.set_metadata("catalog_type", self.state.catalog_type);
                        self.inner
                            .set_span_context(span_recorder.span().map(|s| s.ctx.clone()));

                        let res = self.inner.$repo()
                            .$method($($arg),*)
                            .await;

                        match &res {
                            Ok(_) => {
                                span_recorder.ok("done");
                            }
                            Err(e) => {
                                span_recorder.error(e.to_string());
                            }
                        }

                        // Avoid exploding if time goes backwards - simply drop the
                        // measurement if it happens.
                        if let Some(delta) = self.state.time_provider.now().checked_duration_since(t) {
                            self.state.$metric.record(delta, res.is_ok());
                        }

                        res
                    }
                )+
            }
        )*
    };
}

decorate! {
    {
        impl_trait = RootRepo,
        repo = root,
        methods = [
            root_snapshot = snapshot(&mut self) -> Result<RootSnapshot>;
        ],
    },
    {
        impl_trait = NamespaceRepo,
        repo = namespaces,
        methods = [
            namespace_create = create(&mut self, name: &NamespaceName<'_>, partition_template: Option<NamespacePartitionTemplateOverride>, retention_period_ns: Option<i64>, service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>) -> Result<Namespace>;
            namespace_update_retention_period = update_retention_period(&mut self, name: &str, retention_period_ns: Option<i64>) -> Result<Namespace>;
            namespace_list = list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;
            namespace_get_by_id = get_by_id(&mut self, id: NamespaceId, deleted: SoftDeletedRows) -> Result<Option<Namespace>>;
            namespace_get_by_name = get_by_name(&mut self, name: &str, deleted: SoftDeletedRows) -> Result<Option<Namespace>>;
            namespace_soft_delete = soft_delete(&mut self, name: &str) -> Result<NamespaceId>;
            namespace_update_table_limit = update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace>;
            namespace_update_column_limit = update_column_limit(&mut self, name: &str, new_max: MaxColumnsPerTable) -> Result<Namespace>;
            namespace_snapshot = snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot>;
            namespace_snapshot_by_name = snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot>;
        ],
    },
    {
        impl_trait = TableRepo,
        repo = tables,
        methods = [
            table_create = create(&mut self, name: &str, partition_template: TablePartitionTemplateOverride, namespace_id: NamespaceId) -> Result<Table>;
            table_get_by_id = get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;
            table_get_by_namespace_and_name = get_by_namespace_and_name(&mut self, namespace_id: NamespaceId, name: &str) -> Result<Option<Table>>;
            table_list_by_namespace_id = list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;
            table_list = list(&mut self) -> Result<Vec<Table>>;
            table_snapshot = snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot>;
        ],
    },
    {
        impl_trait = ColumnRepo,
        repo = columns,
        methods = [
            column_create_or_get = create_or_get(&mut self, name: &str, table_id: TableId, column_type: ColumnType) -> Result<Column>;
            column_list_by_namespace_id = list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
            column_list_by_table_id = list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;
            column_create_or_get_many_unchecked = create_or_get_many_unchecked(&mut self, table_id: TableId, columns: HashMap<&str, ColumnType>) -> Result<Vec<Column>>;
            column_list = list(&mut self) -> Result<Vec<Column>>;
        ],
    },
    {
        impl_trait = PartitionRepo,
        repo = partitions,
        methods = [
            partition_create_or_get = create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;
            partition_set_new_file_at = set_new_file_at(&mut self, partition_id: PartitionId, new_file_at: Timestamp) -> Result<()>;
            partition_get_by_id_batch = get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>>;
            partition_list_by_table_id = list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;
            partition_list_ids = list_ids(&mut self) -> Result<Vec<PartitionId>>;
            partition_update_sort_key = cas_sort_key(&mut self, partition_id: PartitionId, old_sort_key_ids: Option<&SortKeyIds>, new_sort_key_ids: &SortKeyIds) -> Result<Partition, CasFailure<SortKeyIds>>;
            partition_record_skipped_compaction = record_skipped_compaction(&mut self, partition_id: PartitionId, reason: &str, num_files: usize, limit_num_files: usize, limit_num_files_first_in_partition: usize, estimated_bytes: u64, limit_bytes: u64) -> Result<()>;
            partition_list_skipped_compactions = list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;
            partition_delete_skipped_compactions = delete_skipped_compactions(&mut self, partition_id: PartitionId) -> Result<Option<SkippedCompaction>>;
            partition_most_recent_n = most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;
            partition_partitions_new_file_between = partitions_new_file_between(&mut self, minimum_time: Timestamp, maximum_time: Option<Timestamp>) -> Result<Vec<PartitionId>>;
            partition_partitions_needing_cold_compact = partitions_needing_cold_compact(&mut self, maximum_time: Timestamp, n: usize) -> Result<Vec<PartitionId>>;
            partition_update_cold_compact = update_cold_compact(&mut self, partition_id: PartitionId, cold_compact_at: Timestamp) -> Result<()>;
            partition_get_in_skipped_compactions = get_in_skipped_compactions(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<SkippedCompaction>>;
            partition_list_old_style = list_old_style(&mut self) -> Result<Vec<Partition>>;
            partition_delete_by_retention = delete_by_retention(&mut self) -> Result<Vec<(TableId, PartitionId)>>;
            partition_snapshot = snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot>;
        ],
    },
    {
        impl_trait = ParquetFileRepo,
        repo = parquet_files,
        methods = [
            parquet_flag_for_delete_by_retention = flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>>;
            parquet_delete_old_ids_only = delete_old_ids_only(&mut self, older_than: Timestamp, cutoff: Duration) -> Result<Vec<ObjectStoreId>>;
            parquet_list_by_partition_not_to_delete_batch = list_by_partition_not_to_delete_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<ParquetFile>>;
            parquet_active_as_of = active_as_of(&mut self, as_of: Timestamp) -> Result<Vec<ParquetFile>>;
            parquet_get_by_object_store_id = get_by_object_store_id(&mut self, object_store_id: ObjectStoreId) -> Result<Option<ParquetFile>>;
            parquet_exists_by_object_store_id_batch = exists_by_object_store_id_batch(&mut self, object_store_ids: Vec<ObjectStoreId>) -> Result<Vec<ObjectStoreId>>;
            parquet_create_upgrade_delete = create_upgrade_delete(&mut self, partition_id: PartitionId, delete: &[ObjectStoreId], upgrade: &[ObjectStoreId], create: &[ParquetFileParams], target_level: CompactionLevel) -> Result<Vec<ParquetFileId>>;
        ],
    },
}
