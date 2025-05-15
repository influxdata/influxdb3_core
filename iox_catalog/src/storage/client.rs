//! gRPC client implementation of `CatalogStorageService`
use std::{fmt::Debug, future::Future, ops::ControlFlow, sync::Arc, time::Duration};

use crate::{
    interface::{
        CatalogStorage, Error, NamespaceSorting, Paginated, PaginationOptions, Result,
        SoftDeletedRows, TableSorting,
    },
    util_serialization::{convert_status, is_upstream_error},
};
use async_trait::async_trait;
use backoff::{Backoff, BackoffError};
use client_util::tower::SetRequestHeadersService;
use data_types::{
    NamespaceId, NamespaceWithStorage, ProtoV1AnyWithStorageError, TableId, TableWithStorage,
};
use generated_types::{
    Response, Status,
    influxdata::iox::catalog_storage::v1 as proto,
    transport::{Channel, Endpoint},
};
use http::{HeaderName, Uri};
use observability_deps::tracing::{debug, info, warn};
use trace::ctx::SpanContext;
use trace_http::tower::ServiceProtocol;
use trace_http::{
    ctx::format_jaeger_trace_context,
    metrics::{MetricFamily, RequestMetrics},
    tower::TraceService,
};

type InstrumentedChannel = TraceService<Channel>;

type ServiceClient = proto::catalog_storage_service_client::CatalogStorageServiceClient<
    SetRequestHeadersService<InstrumentedChannel>,
>;

/// Builder for [`GrpcCatalogStorageClient`].
#[derive(Debug)]
pub struct GrpcCatalogStorageClientBuilder {
    endpoints: Vec<Uri>,
    metrics: Arc<metric::Registry>,
    timeout: Duration,
    connect_timeout: Duration,
    trace_header_name: HeaderName,
    max_decoded_message_size: usize,
}

impl GrpcCatalogStorageClientBuilder {
    /// Build a new client.
    pub fn build(self) -> GrpcCatalogStorageClient {
        let Self {
            endpoints: uri,
            metrics,
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
        let channel = TraceService::new_client(
            channel,
            req_metrics,
            None,
            "catalog_storage",
            ServiceProtocol::Grpc,
        );

        GrpcCatalogStorageClient {
            channel,
            trace_header_name,
            max_decoded_message_size,
            span_ctx: None,
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

/// Catalog storage through a gRPC interface.
#[derive(Debug)]
pub struct GrpcCatalogStorageClient {
    channel: InstrumentedChannel,
    span_ctx: Option<SpanContext>,
    trace_header_name: HeaderName,
    max_decoded_message_size: usize,
}

impl GrpcCatalogStorageClient {
    /// Create builder for new client.
    pub fn builder(
        endpoints: Vec<Uri>,
        metrics: Arc<metric::Registry>,
    ) -> GrpcCatalogStorageClientBuilder {
        GrpcCatalogStorageClientBuilder {
            endpoints,
            metrics,
            timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(2),
            trace_header_name: HeaderName::from_static("influx-trace-id"),
            // default from tonic: 4 MiB
            max_decoded_message_size: 4 * 1024 * 1024,
        }
    }

    // TODO: Figure out whether this should be here at all, or figure out where we it should be
    // used. It's currently unused.
    #[expect(dead_code)]
    fn set_span_context(&mut self, span_ctx: Option<SpanContext>) {
        self.span_ctx = span_ctx;
    }

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

        proto::catalog_storage_service_client::CatalogStorageServiceClient::new(
            SetRequestHeadersService::new(self.channel.clone(), headers),
        )
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
        Fut: Future<Output = Result<Response<D>, Status>> + Send,
        D: std::fmt::Debug,
    {
        Backoff::new(&Default::default())
            .retry_with_backoff(operation, async || {
                let res = fun_io(upload.clone(), self.client()).await;
                match res {
                    Ok(r) => {
                        let ret = r.into_inner();
                        debug!(operation, ?ret, "successfully received");
                        ControlFlow::Break(Ok(ret))
                    }
                    Err(e) if is_upstream_error(&e) => {
                        info!(operation, %e, "retriable error encountered");
                        ControlFlow::Continue(e)
                    }
                    Err(e) => {
                        warn!(operation, %e, ?upload, "NON-retriable error encountered");
                        ControlFlow::Break(Err(convert_status(e)))
                    }
                }
            })
            .await
            .map_err(|e| {
                let status = match e {
                    BackoffError::DeadlineExceeded { source, .. } => source,
                    BackoffError::RetryDisallowed { source, .. } => source,
                };
                convert_status(status)
            })?
    }
}

#[async_trait]
impl CatalogStorage for GrpcCatalogStorageClient {
    async fn get_namespace_with_storage(
        &self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<NamespaceWithStorage>> {
        let req = proto::GetNamespaceWithStorageRequest {
            id: id.get(),
            deleted: Some(deleted.into()),
        };

        let resp = self
            .retry(
                "get_namespace_with_storage",
                req,
                async move |data, mut client| client.get_namespace_with_storage(data).await,
            )
            .await?;

        let namespace_with_storage = resp
            .namespace_with_storage
            .map(TryInto::try_into)
            .transpose()?;

        Ok(namespace_with_storage)
    }

    async fn get_namespaces_with_storage(
        &self,
        sorting: Option<NamespaceSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<NamespaceWithStorage>> {
        let req = proto::GetNamespacesWithStorageRequest {
            page_number: pagination.map(|v| v.page_number.get() as i32),
            page_size: pagination.map(|v| v.page_size.get() as i32),
            sort_field: sorting.map(|s| s.field.into()),
            sort_direction: sorting.map(|s| s.direction.into()),
            deleted: Some(deleted.into()),
        };

        let resp = self
            .retry(
                "get_namespaces_with_storage",
                req,
                async move |data, mut client| client.get_namespaces_with_storage(data).await,
            )
            .await?;

        let namespaces_with_storage = resp
            .namespaces_with_storage
            .into_iter()
            .map(data_types::NamespaceWithStorage::try_from)
            .collect::<Result<Vec<_>, ProtoV1AnyWithStorageError>>()?;

        Ok(Paginated::new_from_options(
            namespaces_with_storage,
            resp.total,
            pagination,
        ))
    }

    async fn get_table_with_storage(
        &self,
        id: TableId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<TableWithStorage>> {
        let req = proto::GetTableWithStorageRequest {
            table_id: id.get(),
            deleted: Some(deleted.into()),
        };

        let resp = self
            .retry(
                "get_table_with_storage",
                req,
                async move |data, mut client| client.get_table_with_storage(data).await,
            )
            .await?;

        let table_with_storage = resp.table_with_storage.map(TryInto::try_into).transpose()?;

        Ok(table_with_storage)
    }

    async fn get_tables_with_storage(
        &self,
        namespace_id: NamespaceId,
        sorting: Option<TableSorting>,
        pagination: Option<PaginationOptions>,
        deleted: SoftDeletedRows,
    ) -> Result<Paginated<TableWithStorage>> {
        let req = proto::GetTablesWithStorageRequest {
            namespace_id: namespace_id.get(),
            page_number: pagination.map(|v| v.page_number.get() as i32),
            page_size: pagination.map(|v| v.page_size.get() as i32),
            sort_field: sorting.map(|s| s.field.into()),
            sort_direction: sorting.map(|s| s.direction.into()),
            deleted: Some(deleted.into()),
        };

        let resp = self
            .retry(
                "get_tables_with_storage",
                req,
                async move |data, mut client| client.get_tables_with_storage(data).await,
            )
            .await?;

        let tables_with_storage = resp
            .tables_with_storage
            .into_iter()
            .map(data_types::TableWithStorage::try_from)
            .collect::<Result<Vec<_>, ProtoV1AnyWithStorageError>>()?;

        Ok(Paginated::new_from_options(
            tables_with_storage,
            resp.total,
            pagination,
        ))
    }
}
