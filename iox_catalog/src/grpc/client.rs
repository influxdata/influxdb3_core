//! gRPC client implementation.
use std::collections::HashSet;
use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use client_util::tower::SetRequestHeadersService;
use data_types::snapshot::namespace::NamespaceSnapshot;
use data_types::snapshot::root::RootSnapshot;
use data_types::{NamespaceWithStorage, TableWithStorage};
use futures::TryStreamExt;
use http::HeaderName;
use observability_deps::tracing::{debug, info, warn};
use tonic::transport::{Channel, Endpoint, Uri};
use trace::ctx::SpanContext;
use trace_http::ctx::format_jaeger_trace_context;

use crate::interface::{
    CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, NamespaceSorting, Paginated,
    ParquetFileRepo, PartitionRepo, RepoCollection, Result, SoftDeletedRows, TableRepo,
};
use crate::interface::{PaginationOptions, RootRepo, TableSorting, namespace_snapshot_by_name};
use crate::metrics::CatalogMetrics;
use backoff::{Backoff, BackoffError};
use data_types::snapshot::partition::PartitionSnapshot;
use data_types::{
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction, SortKeyIds, Table,
    TableId, Timestamp,
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::table::TableSnapshot,
};
use generated_types::influxdata::iox::catalog::v2 as proto;
use iox_time::TimeProvider;
use trace_http::metrics::{MetricFamily, RequestMetrics};
use trace_http::tower::TraceService;

use super::serialization::{
    deserialize_column, deserialize_namespace, deserialize_object_store_id,
    deserialize_parquet_file, deserialize_partition, deserialize_skipped_compaction,
    deserialize_sort_key_ids, deserialize_table, deserialize_timestamp, serialize_column_type,
    serialize_object_store_id, serialize_parquet_file_params, serialize_sort_key_ids,
};
use crate::util_serialization::{
    ContextExt, RequiredExt, convert_status, is_upstream_error, serialize_soft_deleted_rows,
};

type InstrumentedChannel = TraceService<Channel>;

/// Builder for [`GrpcCatalogClient`].
#[derive(Debug)]
pub struct GrpcCatalogClientBuilder {
    endpoints: Vec<Uri>,
    metrics: Arc<metric::Registry>,
    time_provider: Arc<dyn TimeProvider>,
    timeout: Duration,
    connect_timeout: Duration,
    trace_header_name: HeaderName,
    max_decoded_message_size: usize,
}

impl GrpcCatalogClientBuilder {
    /// Build client.
    pub fn build(self) -> GrpcCatalogClient {
        let Self {
            endpoints: uri,
            metrics,
            time_provider,
            timeout,
            connect_timeout,
            trace_header_name,
            max_decoded_message_size,
        } = self;

        let channel = match uri.len() {
            1 => Channel::builder(uri.into_iter().next().unwrap())
                .timeout(timeout)
                .connect_timeout(connect_timeout)
                .connect_lazy(),
            _ => {
                let endpoints = uri.into_iter().map(|uri| {
                    Endpoint::from(uri)
                        .timeout(timeout)
                        .connect_timeout(connect_timeout)
                });
                Channel::balance_list(endpoints)
            }
        };

        let req_metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&metrics),
            MetricFamily::GrpcClient,
        ));
        let channel = TraceService::new_client(channel, req_metrics, None, "catalog");

        GrpcCatalogClient {
            channel,
            metrics: CatalogMetrics::new(
                metrics,
                Arc::clone(&time_provider),
                GrpcCatalogClient::NAME,
            ),
            time_provider,
            trace_header_name,
            max_decoded_message_size,
        }
    }

    /// Apply a timeout to each request.
    pub fn timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }

    /// Apply a timeout to connecting to the uri.
    pub fn connect_timeout(self, connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            ..self
        }
    }

    /// Set header used to transmit tracing information.
    pub fn trace_header_name(self, trace_header_name: HeaderName) -> Self {
        Self {
            trace_header_name,
            ..self
        }
    }

    /// Set max decoded message size.
    ///
    /// Default: 4MB
    pub fn max_decoded_message_size(self, max_decoded_message_size: usize) -> Self {
        Self {
            max_decoded_message_size,
            ..self
        }
    }
}

/// Catalog that goes through a gRPC interface.
#[derive(Debug)]
pub struct GrpcCatalogClient {
    channel: InstrumentedChannel,
    metrics: CatalogMetrics,
    time_provider: Arc<dyn TimeProvider>,
    trace_header_name: HeaderName,
    max_decoded_message_size: usize,
}

impl GrpcCatalogClient {
    /// The [name](Catalog::name) of this catalog type.
    pub const NAME: &'static str = "grpc_client";

    /// Create builder for new client.
    pub fn builder(
        endpoints: Vec<Uri>,
        metrics: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> GrpcCatalogClientBuilder {
        GrpcCatalogClientBuilder {
            endpoints,
            metrics,
            time_provider,
            timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(2),
            trace_header_name: HeaderName::from_static("uber-trace-id"),
            // default from tonic: 4MB
            max_decoded_message_size: 4194304,
        }
    }
}

#[async_trait]
impl Catalog for GrpcCatalogClient {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(self.metrics.repos(Box::new(GrpcCatalogClientRepos {
            channel: self.channel.clone(),
            span_ctx: None,
            trace_header_name: self.trace_header_name.clone(),
            max_decoded_message_size: self.max_decoded_message_size,
            time_provider: Arc::clone(&self.time_provider),
        })))
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        self.metrics.registry()
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    async fn get_time(&self) -> Result<iox_time::Time, Error> {
        let client_repos = GrpcCatalogClientRepos {
            channel: self.channel.clone(),
            span_ctx: None,
            trace_header_name: self.trace_header_name.clone(),
            max_decoded_message_size: self.max_decoded_message_size,
            time_provider: Arc::clone(&self.time_provider),
        };

        let req = proto::GetTimeRequest {};

        let resp = client_repos
            .retry("get_time", req, async move |data, mut client| {
                client.get_time(data).await
            })
            .await?;

        Ok(deserialize_timestamp(resp.time.required().ctx("time")?)?)
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
struct GrpcCatalogClientRepos {
    channel: InstrumentedChannel,
    span_ctx: Option<SpanContext>,
    trace_header_name: HeaderName,
    max_decoded_message_size: usize,
    time_provider: Arc<dyn TimeProvider>,
}

type ServiceClient = proto::catalog_service_client::CatalogServiceClient<
    SetRequestHeadersService<InstrumentedChannel>,
>;

impl GrpcCatalogClientRepos {
    fn client(&self) -> ServiceClient {
        let headers = if let Some(span_ctx) = &self.span_ctx {
            vec![(
                self.trace_header_name.clone(),
                format_jaeger_trace_context(span_ctx)
                    .try_into()
                    .expect("format trace header"),
            )]
        } else {
            vec![]
        };

        proto::catalog_service_client::CatalogServiceClient::new(SetRequestHeadersService::new(
            self.channel.clone(),
            headers,
        ))
        .max_decoding_message_size(self.max_decoded_message_size)
    }

    async fn retry<U, FunIo, Fut, D>(
        &self,
        operation: &str,
        upload: U,
        fun_io: FunIo,
    ) -> Result<D, Error>
    where
        U: Clone + std::fmt::Debug + Send + Sync,
        FunIo: Fn(U, ServiceClient) -> Fut + Send + Sync,
        Fut: Future<Output = Result<tonic::Response<D>, tonic::Status>> + Send,
        D: std::fmt::Debug,
    {
        Backoff::new(&Default::default())
            .retry_with_backoff(operation, async || {
                let res = fun_io(upload.clone(), self.client()).await;
                match res {
                    Ok(r) => {
                        let return_value = r.into_inner();
                        debug!(operation, ?return_value, "successfully received");
                        ControlFlow::Break(Ok(return_value))
                    }
                    Err(e) if is_upstream_error(&e) => {
                        info!(operation, %e, "retriable error encountered");
                        ControlFlow::Continue(e)
                    }
                    Err(e) => {
                        warn!(
                            operation,
                            %e,
                            ?upload,
                            "NON-retriable error encountered",
                        );
                        ControlFlow::Break(Err(convert_status(e)))
                    }
                }
            })
            .await
            .map_err(|be| {
                let status = match be {
                    BackoffError::DeadlineExceeded { source, .. } => source,
                    BackoffError::RetryDisallowed { source, .. } => source,
                };
                convert_status(status)
            })?
    }
}

impl RepoCollection for GrpcCatalogClientRepos {
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

#[async_trait]
impl RootRepo for GrpcCatalogClientRepos {
    async fn snapshot(&mut self) -> Result<RootSnapshot> {
        let req = proto::RootSnapshotRequest {};
        let resp = self
            .retry("root_snapshot", req, async move |req, mut client| {
                client.root_snapshot(req).await
            })
            .await?;

        let root = resp.root.required().ctx("root")?;
        let snapshot = RootSnapshot::decode(root, resp.generation)?;
        Ok(snapshot)
    }
}

#[async_trait]
impl NamespaceRepo for GrpcCatalogClientRepos {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let n = proto::NamespaceCreateRequest {
            name: name.to_string(),
            partition_template: partition_template.and_then(|t| t.as_proto().cloned()),
            retention_period_ns,
            service_protection_limits: service_protection_limits.map(|l| {
                proto::ServiceProtectionLimits {
                    max_tables: l.max_tables.map(|x| x.get_i32()),
                    max_columns_per_table: l.max_columns_per_table.map(|x| x.get_i32()),
                }
            }),
        };

        let resp = self
            .retry("namespace_create", n, async move |data, mut client| {
                client.namespace_create(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn update_retention_period(
        &mut self,
        id: NamespaceId,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        use generated_types::influxdata::iox::catalog::v2::namespace_update_retention_period_request::Target;
        let n = proto::NamespaceUpdateRetentionPeriodRequest {
            target: Some(Target::Id(id.get())),
            retention_period_ns,
        };

        let resp = self
            .retry(
                "namespace_update_retention_period",
                n,
                async move |data, mut client| client.namespace_update_retention_period(data).await,
            )
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let n = proto::NamespaceListRequest {
            deleted: serialize_soft_deleted_rows(deleted),
        };

        self.retry("namespace_list", n, async move |data, mut client| {
            buffer_stream_response(client.namespace_list(data).await).await
        })
        .await?
        .into_iter()
        .map(|res| {
            deserialize_namespace(res.namespace.required().ctx("namespace")?).map_err(Error::from)
        })
        .collect()
    }

    async fn list_storage(
        &mut self,
        _sorting: Option<NamespaceSorting>,
        _pagination: Option<PaginationOptions>,
    ) -> Result<Paginated<NamespaceWithStorage>> {
        // The storage API is intentionally not exposed to this client to avoid confusion.
        // The storage API is mainly for admin view via the Granite Admin UI or influxctl,
        // and not for any IOx internal communications.
        Err(Error::NotImplemented {
            descr: "list_storage".to_owned(),
        })
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let n = proto::NamespaceGetByIdRequest {
            id: id.get(),
            deleted: serialize_soft_deleted_rows(deleted),
        };

        let resp = self
            .retry("namespace_get_by_id", n, async move |data, mut client| {
                client.namespace_get_by_id(data).await
            })
            .await?;
        Ok(resp.namespace.map(deserialize_namespace).transpose()?)
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>> {
        #[expect(deprecated)]
        let n = proto::NamespaceGetByNameRequest {
            name: name.to_owned(),
            // This is a deprecated field that is ignored and should eventually be removed. For now
            // and to avoid compatibility problems with rollout of this change, we use
            // `SoftDeletedRows::ExcludeDeleted` so that old servers will accept the new client
            // requests.
            deleted: Some(proto::SoftDeletedRows::ExcludeDeleted.into()),
        };

        let resp = self
            .retry("namespace_get_by_name", n, async move |data, mut client| {
                client.namespace_get_by_name(data).await
            })
            .await?;
        Ok(resp.namespace.map(deserialize_namespace).transpose()?)
    }

    async fn soft_delete(&mut self, id: NamespaceId) -> Result<Namespace> {
        use generated_types::influxdata::iox::catalog::v2::{
            namespace_soft_delete_request::Target, namespace_soft_delete_response::Deleted,
        };

        let n = proto::NamespaceSoftDeleteRequest {
            target: Some(Target::Id(id.get())),
        };

        let resp = self
            .retry("namespace_soft_delete", n, async move |data, mut client| {
                client.namespace_soft_delete(data).await
            })
            .await?;

        match resp.deleted.required()? {
            Deleted::NamespaceId(id) => {
                NamespaceRepo::get_by_id(self, NamespaceId::new(id), SoftDeletedRows::OnlyDeleted)
                    .await
                    .map(Option::unwrap)
            }
            Deleted::Namespace(ns) => deserialize_namespace(ns).map_err(Into::into),
        }
    }

    async fn update_table_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxTables,
    ) -> Result<Namespace> {
        use proto::namespace_update_table_limit_request::Target;
        let n = proto::NamespaceUpdateTableLimitRequest {
            target: Some(Target::Id(id.get())),
            new_max: new_max.get_i32(),
        };

        let resp = self
            .retry(
                "namespace_update_table_limit",
                n,
                async move |data, mut client| client.namespace_update_table_limit(data).await,
            )
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn update_column_limit(
        &mut self,
        id: NamespaceId,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        use proto::namespace_update_column_limit_request::Target;
        let n = proto::NamespaceUpdateColumnLimitRequest {
            target: Some(Target::Id(id.get())),
            new_max: new_max.get_i32(),
        };

        let resp = self
            .retry(
                "namespace_update_column_limit",
                n,
                async move |data, mut client| client.namespace_update_column_limit(data).await,
            )
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot> {
        let req = proto::NamespaceSnapshotRequest {
            namespace_id: namespace_id.get(),
        };
        let resp = self
            .retry("namespace_snapshot", req, async move |req, mut client| {
                client.namespace_snapshot(req).await
            })
            .await?;

        let ns = resp.namespace.required().ctx("namespace")?;
        let snapshot = NamespaceSnapshot::decode(ns, resp.generation)?;
        Ok(snapshot)
    }

    async fn snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot> {
        let req = proto::NamespaceSnapshotByNameRequest {
            name: name.to_string(),
        };
        let r = self
            .retry("namespace_snapshot", req, async move |req, mut client| {
                client.namespace_snapshot_by_name(req).await
            })
            .await;

        if matches!(r, Err(Error::NotImplemented { .. })) {
            // Fallback logic for old servers
            warn!("NamespaceSnapshotByName not implemented by remote, falling back");
            return namespace_snapshot_by_name(self, name).await;
        }

        let resp = r?;
        let ns = resp.namespace.required().ctx("namespace")?;
        let snapshot = NamespaceSnapshot::decode(ns, resp.generation)?;
        Ok(snapshot)
    }

    async fn get_storage_by_id(
        &mut self,
        _id: NamespaceId,
    ) -> Result<Option<NamespaceWithStorage>> {
        // The storage API is intentionally not exposed to this client to avoid confusion.
        // The storage API is mainly for admin view via the Granite Admin UI or influxctl,
        // and not for any IOx internal communications.
        Err(Error::NotImplemented {
            descr: "namespace_get_storage_by_id".to_owned(),
        })
    }

    /// Rename the namespace corresponding to `id`.
    async fn rename(&mut self, id: NamespaceId, new_name: NamespaceName<'_>) -> Result<Namespace> {
        let req = proto::NamespaceRenameRequest {
            id: id.get(),
            new_name: new_name.into(),
        };

        let resp = self
            .retry("namespace_rename", req, async move |data, mut client| {
                client.namespace_rename(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    /// Undelete the soft deleted namespace corresponding to `id`.
    ///
    /// There must be no active, undeleted namespace using the target's name.
    async fn undelete(&mut self, id: NamespaceId) -> Result<Namespace> {
        let req = proto::NamespaceUndeleteRequest { id: id.get() };

        let resp = self
            .retry("namespace_undelete", req, async move |data, mut client| {
                client.namespace_undelete(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }
}

#[async_trait]
impl TableRepo for GrpcCatalogClientRepos {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let t = proto::TableCreateRequest {
            name: name.to_owned(),
            partition_template: partition_template.as_proto().cloned(),
            namespace_id: namespace_id.get(),
        };

        let resp = self
            .retry("table_create", t, async move |data, mut client| {
                client.table_create(data).await
            })
            .await?;
        Ok(deserialize_table(resp.table.required().ctx("table")?)?)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let t = proto::TableGetByIdRequest { id: table_id.get() };

        let resp = self
            .retry("table_get_by_id", t, async move |data, mut client| {
                client.table_get_by_id(data).await
            })
            .await?;
        Ok(resp.table.map(deserialize_table).transpose()?)
    }

    async fn get_storage_by_id(&mut self, _table_id: TableId) -> Result<Option<TableWithStorage>> {
        // The storage API is intentionally not exposed to this client to avoid confusion.
        // The storage API is mainly for admin view via the Granite Admin UI or influxctl,
        // and not for any IOx internal communications.
        Err(Error::NotImplemented {
            descr: "table_get_storage_by_id".to_owned(),
        })
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let t = proto::TableGetByNamespaceAndNameRequest {
            namespace_id: namespace_id.get(),
            name: name.to_owned(),
        };

        let resp = self
            .retry(
                "table_get_by_namespace_and_name",
                t,
                async move |data, mut client| client.table_get_by_namespace_and_name(data).await,
            )
            .await?;
        Ok(resp.table.map(deserialize_table).transpose()?)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let t = proto::TableListByNamespaceIdRequest {
            namespace_id: namespace_id.get(),
        };

        self.retry(
            "table_list_by_namespace_id",
            t,
            async move |data, mut client| {
                buffer_stream_response(client.table_list_by_namespace_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_table(res.table.required().ctx("table")?)?))
        .collect()
    }

    async fn list_storage_by_namespace_id(
        &mut self,
        _namespace_id: NamespaceId,
        _sorting: Option<TableSorting>,
        _pagination: Option<PaginationOptions>,
    ) -> Result<Paginated<TableWithStorage>> {
        // The storage API is intentionally not exposed to this client to avoid confusion.
        // The storage API is mainly for admin view via the Granite Admin UI or influxctl,
        // and not for any IOx internal communications.
        Err(Error::NotImplemented {
            descr: "list_storage_by_namespace_id".to_owned(),
        })
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let t = proto::TableListRequest {};

        self.retry("table_list", t, async move |data, mut client| {
            buffer_stream_response(client.table_list(data).await).await
        })
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_table(res.table.required().ctx("table")?)?))
        .collect()
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        let t = proto::TableSnapshotRequest {
            table_id: table_id.get(),
        };

        let resp = self
            .retry("table_snapshot", t, async move |data, mut client| {
                client.table_snapshot(data).await
            })
            .await?;

        let table = resp.table.required().ctx("table")?;
        Ok(TableSnapshot::decode(table, resp.generation))
    }

    async fn list_by_iceberg_enabled(&mut self, namespace_id: NamespaceId) -> Result<Vec<TableId>> {
        let t = proto::TableListByIcebergEnabledRequest {
            namespace_id: namespace_id.get(),
        };

        self.retry(
            "list_by_iceberg_enabled",
            t,
            |data, mut client| async move {
                buffer_stream_response(client.table_list_by_iceberg_enabled(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| Ok(TableId::new(res.table_id)))
        .collect()
    }

    async fn enable_iceberg(&mut self, table_id: TableId) -> Result<()> {
        let t = proto::TableEnableIcebergRequest {
            table_id: table_id.get(),
        };

        self.retry("enable_iceberg", t, |data, mut client| async move {
            client.table_enable_iceberg(data).await
        })
        .await?;

        Ok(())
    }

    async fn disable_iceberg(&mut self, table_id: TableId) -> Result<()> {
        let t = proto::TableDisableIcebergRequest {
            table_id: table_id.get(),
        };

        self.retry("disable_iceberg", t, |data, mut client| async move {
            client.table_disable_iceberg(data).await
        })
        .await?;

        Ok(())
    }

    async fn soft_delete(&mut self, id: TableId) -> Result<Table> {
        let req = proto::TableSoftDeleteRequest { table_id: id.get() };

        let resp = self
            .retry("soft_delete", req, async move |data, mut client| {
                client.table_soft_delete(data).await
            })
            .await?;

        Ok(deserialize_table(resp.table.required().ctx("table")?)?)
    }

    async fn rename(&mut self, id: TableId, new_name: &str) -> Result<Table> {
        let req = proto::TableRenameRequest {
            table_id: id.get(),
            new_name: new_name.to_owned(),
        };

        let resp = self
            .retry("rename", req, async move |data, mut client| {
                client.table_rename(data).await
            })
            .await?;

        Ok(deserialize_table(resp.table.required().ctx("table")?)?)
    }

    async fn undelete(&mut self, id: TableId) -> Result<Table> {
        let req = proto::TableUndeleteRequest { table_id: id.get() };

        let resp = self
            .retry("undelete", req, async move |data, mut client| {
                client.table_undelete(data).await
            })
            .await?;

        Ok(deserialize_table(resp.table.required().ctx("table")?)?)
    }
}

#[async_trait]
impl ColumnRepo for GrpcCatalogClientRepos {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let c = proto::ColumnCreateOrGetRequest {
            name: name.to_owned(),
            table_id: table_id.get(),
            column_type: serialize_column_type(column_type),
        };

        let resp = self
            .retry("column_create_or_get", c, async move |data, mut client| {
                client.column_create_or_get(data).await
            })
            .await?;
        Ok(deserialize_column(resp.column.required().ctx("column")?)?)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let c = proto::ColumnCreateOrGetManyUncheckedRequest {
            table_id: table_id.get(),
            columns: columns
                .into_iter()
                .map(|(name, t)| (name.to_owned(), serialize_column_type(t)))
                .collect(),
        };

        self.retry(
            "column_create_or_get_many_unchecked",
            c,
            async move |data, mut client| {
                buffer_stream_response(client.column_create_or_get_many_unchecked(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_column(res.column.required().ctx("column")?)?))
        .collect()
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let c = proto::ColumnListByNamespaceIdRequest {
            namespace_id: namespace_id.get(),
        };

        self.retry(
            "column_list_by_namespace_id",
            c,
            async move |data, mut client| {
                buffer_stream_response(client.column_list_by_namespace_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_column(res.column.required().ctx("column")?)?))
        .collect()
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let c = proto::ColumnListByTableIdRequest {
            table_id: table_id.get(),
        };

        self.retry(
            "column_list_by_table_id",
            c,
            async move |data, mut client| {
                buffer_stream_response(client.column_list_by_table_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_column(res.column.required().ctx("column")?)?))
        .collect()
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Column>> {
        let c = proto::ColumnListRequest {
            deleted: Some(serialize_soft_deleted_rows(deleted)),
        };

        self.retry("column_list", c, async move |data, mut client| {
            buffer_stream_response(client.column_list(data).await).await
        })
        .await?
        .into_iter()
        .map(|res| Ok(deserialize_column(res.column.required().ctx("column")?)?))
        .collect()
    }
}

#[async_trait]
impl PartitionRepo for GrpcCatalogClientRepos {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let p = proto::PartitionCreateOrGetRequest {
            key: key.inner().to_owned(),
            table_id: table_id.get(),
        };

        let resp = self
            .retry(
                "partition_create_or_get",
                p,
                async move |data, mut client| client.partition_create_or_get(data).await,
            )
            .await?;

        Ok(deserialize_partition(
            resp.partition.required().ctx("partition")?,
        )?)
    }

    async fn set_new_file_at(
        &mut self,
        _partition_id: PartitionId,
        _new_file_at: Timestamp,
    ) -> Result<()> {
        Err(Error::NotImplemented {
            descr: "set_new_file_at is for test use only, not implemented for grpc client"
                .to_string(),
        })
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        let p = proto::PartitionGetByIdBatchRequest {
            partition_ids: partition_ids.iter().map(|id| id.get()).collect(),
        };

        self.retry(
            "partition_get_by_id_batch",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_get_by_id_batch(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .collect()
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let p = proto::PartitionListByTableIdRequest {
            table_id: table_id.get(),
        };

        self.retry(
            "partition_list_by_table_id",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_list_by_table_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .collect()
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        let p = proto::PartitionListIdsRequest {};

        Ok(self
            .retry("partition_list_ids", p, async move |data, mut client| {
                buffer_stream_response(client.partition_list_ids(data).await).await
            })
            .await?
            .into_iter()
            .map(|res| PartitionId::new(res.partition_id))
            .collect())
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        // This method does not use request/request_streaming_response
        // because the error handling (converting to CasFailure) differs
        // from how all the other methods handle errors.

        let p = proto::PartitionCasSortKeyRequest {
            partition_id: partition_id.get(),
            old_sort_key_ids: old_sort_key_ids.map(serialize_sort_key_ids),
            new_sort_key_ids: Some(serialize_sort_key_ids(new_sort_key_ids)),
        };

        let res = self
            .retry(
                "partition_cas_sort_key",
                p,
                async move |data, mut client| client.partition_cas_sort_key(data).await,
            )
            .await
            .map_err(CasFailure::QueryError)?;

        let res = res
            .res
            .required()
            .ctx("res")
            .map_err(|e| CasFailure::QueryError(e.into()))?;

        match res {
            proto::partition_cas_sort_key_response::Res::Partition(p) => {
                let p = deserialize_partition(p).map_err(|e| CasFailure::QueryError(e.into()))?;
                Ok(p)
            }
            proto::partition_cas_sort_key_response::Res::CurrentSortKey(k) => {
                Err(CasFailure::ValueMismatch(deserialize_sort_key_ids(k)))
            }
        }
    }

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
        let p = proto::PartitionRecordSkippedCompactionRequest {
            partition_id: partition_id.get(),
            reason: reason.to_owned(),
            num_files: num_files as u64,
            limit_num_files: limit_num_files as u64,
            limit_num_files_first_in_partition: limit_num_files_first_in_partition as u64,
            estimated_bytes,
            limit_bytes,
        };

        self.retry(
            "partition_record_skipped_compaction",
            p,
            async move |data, mut client| client.partition_record_skipped_compaction(data).await,
        )
        .await?;
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let p = proto::PartitionGetInSkippedCompactionsRequest {
            partition_ids: partition_id.iter().map(|id| id.get()).collect(),
        };

        self.retry(
            "partition_get_in_skipped_compactions",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_get_in_skipped_compactions(data).await)
                    .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_skipped_compaction(
                res.skipped_compaction
                    .required()
                    .ctx("skipped_compaction")?,
            ))
        })
        .collect()
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        let p = proto::PartitionListSkippedCompactionsRequest {};

        self.retry(
            "partition_list_skipped_compactions",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_list_skipped_compactions(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_skipped_compaction(
                res.skipped_compaction
                    .required()
                    .ctx("skipped_compaction")?,
            ))
        })
        .collect()
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let p = proto::PartitionDeleteSkippedCompactionsRequest {
            partition_id: partition_id.get(),
        };

        let resp = self
            .retry(
                "partition_delete_skipped_compactions",
                p,
                async move |data, mut client| {
                    client.partition_delete_skipped_compactions(data).await
                },
            )
            .await?;

        Ok(resp.skipped_compaction.map(deserialize_skipped_compaction))
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        let p = proto::PartitionMostRecentNRequest { n: n as u64 };

        self.retry(
            "partition_most_recent_n",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_most_recent_n(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .collect()
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let p = proto::PartitionNewFileBetweenRequest {
            minimum_time: minimum_time.get(),
            maximum_time: maximum_time.map(|ts| ts.get()),
        };

        Ok(self
            .retry(
                "partition_new_file_between",
                p,
                async move |data, mut client| {
                    buffer_stream_response(client.partition_new_file_between(data).await).await
                },
            )
            .await?
            .into_iter()
            .map(|res| PartitionId::new(res.partition_id))
            .collect())
    }

    async fn partitions_needing_cold_compact(
        &mut self,
        maximum_time: Timestamp,
        n: usize,
    ) -> Result<Vec<PartitionId>> {
        let p = proto::PartitionNeedingColdCompactRequest {
            maximum_time: maximum_time.get(),
            n: n as u64,
        };

        Ok(self
            .retry(
                "partitions_needing_cold_compact",
                p,
                async move |data, mut client| {
                    buffer_stream_response(client.partition_needing_cold_compact(data).await).await
                },
            )
            .await?
            .into_iter()
            .map(|res| PartitionId::new(res.partition_id))
            .collect())
    }

    async fn update_cold_compact(
        &mut self,
        partition_id: PartitionId,
        cold_compact_at: Timestamp,
    ) -> Result<()> {
        let p = proto::PartitionUpdateColdCompactRequest {
            partition_id: partition_id.get(),
            cold_compact_at: cold_compact_at.get(),
        };

        self.retry("update_cold_compact", p, async move |data, mut client| {
            client.partition_update_cold_compact(data).await
        })
        .await?;
        Ok(())
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        let p = proto::PartitionListOldStyleRequest {};

        self.retry(
            "partition_list_old_style",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.partition_list_old_style(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .collect()
    }

    async fn delete_by_retention(
        &mut self,
        partition_cutoff: Duration,
    ) -> Result<Vec<(TableId, PartitionId)>> {
        let p = proto::PartitionDeleteByRetentionRequest {
            partition_cutoff_seconds: Some(partition_cutoff.as_secs() as i64),
        };

        let res = self
            .retry(
                "partition_delete_by_retention",
                p,
                async move |data, mut client| {
                    buffer_stream_response(client.partition_delete_by_retention(data).await).await
                },
            )
            .await?
            .into_iter()
            .map(|res| {
                (
                    TableId::new(res.table_id),
                    PartitionId::new(res.partition_id),
                )
            })
            .collect();
        Ok(res)
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        let p = proto::PartitionSnapshotRequest {
            partition_id: partition_id.get(),
        };

        let resp = self
            .retry("partition_snapshot", p, async move |data, mut client| {
                client.partition_snapshot(data).await
            })
            .await?;
        let partition = resp.partition.required().ctx("partition")?;
        Ok(PartitionSnapshot::decode(partition, resp.generation))
    }

    async fn snapshot_generation(&mut self, partition_id: PartitionId) -> Result<u64> {
        (self as &mut dyn PartitionRepo)
            .snapshot(partition_id)
            .await
            .map(|snapshot| snapshot.generation())
    }
}

#[async_trait]
impl ParquetFileRepo for GrpcCatalogClientRepos {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let p = proto::ParquetFileFlagForDeleteByRetentionRequest {};

        self.retry(
            "parquet_file_flag_for_delete_by_retention",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_flag_for_delete_by_retention(data).await)
                    .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok((
                PartitionId::new(res.partition_id),
                deserialize_object_store_id(res.object_store_id.required().ctx("object_store_id")?),
            ))
        })
        .collect()
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        let p = proto::ParquetFileDeleteOldIdsOnlyRequest {
            older_than: older_than.get(),
        };

        self.retry(
            "parquet_file_delete_old_ids_only",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_delete_old_ids_only(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_object_store_id(
                res.object_store_id.required().ctx("object_store_id")?,
            ))
        })
        .collect()
    }

    async fn delete_old_ids_count(&mut self, older_than: Timestamp, limit: u32) -> Result<u64> {
        let p = proto::ParquetFileDeleteOldIdsCountRequest {
            older_than: older_than.get(),
            limit: Some(limit),
        };

        let resp = self
            .retry(
                "parquet_file_delete_old_ids_count",
                p,
                async move |data, mut client| client.parquet_file_delete_old_ids_count(data).await,
            )
            .await;

        match resp {
            Ok(resp) => Ok(resp.num_deleted),
            // If we get an Unimplemented error, that means that the `delete_old_ids_count` method
            // hasn't been rolled out to the remote end yet, so we have to fallback to calling the
            // `delete_old_ids_only` method and counting its vec
            Err(Error::NotImplemented { descr: _ }) =>
            {
                #[expect(deprecated)]
                self.delete_old_ids_only(older_than).await.map(|v| {
                    v.len()
                        .try_into()
                        .expect("128-bit computers don't exist yet")
                })
            }
            // And we're assuming any other error is not recoverable by trying the other method -
            // we just need to return it.
            Err(e) => Err(e),
        }
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        let p = proto::ParquetFileListByPartitionNotToDeleteBatchRequest {
            partition_ids: partition_ids.into_iter().map(|p| p.get()).collect(),
        };

        self.retry(
            "parquet_file_list_by_partition_not_to_delete_batch",
            p,
            async move |data, mut client| {
                buffer_stream_response(
                    client
                        .parquet_file_list_by_partition_not_to_delete_batch(data)
                        .await,
                )
                .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_parquet_file(
                res.parquet_file.required().ctx("parquet_file")?,
            )?)
        })
        .collect()
    }

    async fn active_as_of(&mut self, as_of: Timestamp) -> Result<Vec<ParquetFile>> {
        let p = proto::ParquetFileActiveAsOfRequest { as_of: as_of.get() };

        self.retry(
            "parquet_file_active_as_of",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_active_as_of(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_parquet_file(
                res.parquet_file.required().ctx("parquet_file")?,
            )?)
        })
        .collect()
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        let p = proto::ParquetFileGetByObjectStoreIdRequest {
            object_store_id: Some(serialize_object_store_id(object_store_id)),
        };

        let maybe_file = self
            .retry(
                "parquet_file_get_by_object_store_id",
                p,
                async move |data, mut client| {
                    client.parquet_file_get_by_object_store_id(data).await
                },
            )
            .await?
            .parquet_file
            .map(deserialize_parquet_file)
            .transpose()?;
        Ok(maybe_file)
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        let p = futures::stream::iter(object_store_ids.into_iter().map(|id| {
            proto::ParquetFileExistsByObjectStoreIdBatchRequest {
                object_store_id: Some(serialize_object_store_id(id)),
            }
        }));

        self.retry(
            "parquet_file_exists_by_object_store_id_batch",
            p,
            async move |data, mut client: ServiceClient| {
                buffer_stream_response(
                    client
                        .parquet_file_exists_by_object_store_id_batch(data)
                        .await,
                )
                .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_object_store_id(
                res.object_store_id.required().ctx("object_store_id")?,
            ))
        })
        .collect()
    }

    async fn exists_by_partition_and_object_store_id_batch(
        &mut self,
        ids: Vec<(PartitionId, ObjectStoreId)>,
    ) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let p = futures::stream::iter(ids.into_iter().map(|(partition_id, object_store_id)| {
            proto::ParquetFileExistsByPartitionAndObjectStoreIdBatchRequest {
                object_store_id: Some(serialize_object_store_id(object_store_id)),
                partition_id: partition_id.get(),
            }
        }));

        self.retry(
            "parquet_file_exists_by_partition_and_object_store_id_batch",
            p,
            async move |data, mut client: ServiceClient| {
                buffer_stream_response(
                    client
                        .parquet_file_exists_by_partition_and_object_store_id_batch(data)
                        .await,
                )
                .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            let partition_id = PartitionId::new(res.partition_id);
            let object_store_id =
                deserialize_object_store_id(res.object_store_id.required().ctx("object_store_id")?);
            Ok((partition_id, object_store_id))
        })
        .collect()
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFile>> {
        // If this newer code is sending these requests to a service running older code, that older
        // code expects `proto::ParquetFileParams` to have `created_at` fields set. If these
        // requests are sent to services also running newer code, the `created_at` fields will be
        // ignored and the value will be set by the underlying catalog. When all environments have
        // been upgraded to ignore the `created_at` fields, this can be removed.
        let created_at = self.time_provider.now().into();

        let p = proto::ParquetFileCreateUpgradeDeleteFullRequest {
            partition_id: partition_id.get(),
            delete: delete
                .iter()
                .copied()
                .map(serialize_object_store_id)
                .collect(),
            upgrade: upgrade
                .iter()
                .copied()
                .map(serialize_object_store_id)
                .collect(),
            create: create
                .iter()
                .map(|c| serialize_parquet_file_params(c, created_at))
                .collect(),
            target_level: target_level as i32,
        };

        self.retry(
            "parquet_file_create_upgrade_delete_full",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_create_upgrade_delete_full(data).await)
                    .await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_parquet_file(
                res.parquet_file.required().ctx("parquet_file")?,
            )?)
        })
        .collect()
    }

    async fn list_by_table_id(
        &mut self,
        table_id: TableId,
        compaction_level: Option<CompactionLevel>,
    ) -> Result<Vec<ParquetFile>> {
        let p = proto::ParquetFileListByTableIdRequest {
            table_id: table_id.get(),
            compaction_level: compaction_level.map(|l| l as i32),
        };

        self.retry(
            "parquet_file_list_by_table_id",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_list_by_table_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_parquet_file(
                res.file.required().ctx("parquet_file")?,
            )?)
        })
        .collect()
    }

    async fn list_by_namespace_id(
        &mut self,
        namespace_id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Vec<ParquetFile>> {
        let p = proto::ParquetFileListByNamespaceIdRequest {
            namespace_id: namespace_id.get(),
            deleted: serialize_soft_deleted_rows(deleted),
        };

        self.retry(
            "parquet_file_list_by_namespace_id",
            p,
            async move |data, mut client| {
                buffer_stream_response(client.parquet_file_list_by_namespace_id(data).await).await
            },
        )
        .await?
        .into_iter()
        .map(|res| {
            Ok(deserialize_parquet_file(
                res.file.required().ctx("parquet_file")?,
            )?)
        })
        .collect()
    }
}

/// Buffer [stream respose](tonic::Streaming).
///
/// Catalog return values are NOT streaming, so our retries can cover streaming responses as well.
async fn buffer_stream_response<T>(
    resp: Result<tonic::Response<tonic::Streaming<T>>, tonic::Status>,
) -> Result<tonic::Response<Vec<T>>, tonic::Status>
where
    T: Send,
{
    Ok(tonic::Response::new(
        resp?.into_inner().try_collect::<Vec<T>>().await?,
    ))
}
