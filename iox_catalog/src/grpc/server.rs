//! gRPC server implementation.

use std::{pin::Pin, sync::Arc, time::Duration};

use super::serialization::serialize_timestamp;
use crate::{
    constants::PARTITION_DELETION_CUTOFF,
    grpc::serialization::{
        deserialize_column_type, deserialize_object_store_id, deserialize_parquet_file_params,
        deserialize_sort_key_ids, serialize_column, serialize_namespace, serialize_object_store_id,
        serialize_parquet_file, serialize_partition, serialize_skipped_compaction,
        serialize_sort_key_ids, serialize_table,
    },
    interface::{CasFailure, Catalog, RepoCollection},
    util_serialization::{
        ContextExt, ConvertExt, ConvertOptExt, RequiredExt, catalog_error_to_status,
        deserialize_soft_deleted_rows,
    },
};
use async_trait::async_trait;
use data_types::{
    NamespaceId, NamespaceName, NamespaceServiceProtectionLimitsOverride, PartitionId,
    PartitionKey, TableId, Timestamp,
};
use futures::{Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::catalog::v2::{
    self as proto, ParquetFileDeleteOldIdsCountRequest, ParquetFileDeleteOldIdsCountResponse,
    PartitionDeleteByRetentionRequest,
};
use tonic::{Request, Response, Status};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

/// gRPC server.
#[derive(Debug)]
pub struct GrpcCatalogServer {
    catalog: Arc<dyn Catalog>,
}

impl GrpcCatalogServer {
    /// Create a new [`GrpcCatalogServer`].
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Get service for integration w/ tonic.
    pub fn service(&self) -> proto::catalog_service_server::CatalogServiceServer<Self> {
        let this = Self {
            catalog: Arc::clone(&self.catalog),
        };
        proto::catalog_service_server::CatalogServiceServer::new(this)
    }

    /// Pre-process request by extracting tracing information and setting up the [`RepoCollection`].
    fn preprocess_request<T>(&self, req: Request<T>) -> (Box<dyn RepoCollection>, T) {
        let mut repos = self.catalog.repositories();

        let (_, mut extensions, inner) = req.into_parts();
        repos.set_span_context(extensions.remove());

        (repos, inner)
    }
}

#[async_trait]
impl proto::catalog_service_server::CatalogService for GrpcCatalogServer {
    type NamespaceListStream = TonicStream<proto::NamespaceListResponse>;

    type TableListByNamespaceIdStream = TonicStream<proto::TableListByNamespaceIdResponse>;
    type TableListStream = TonicStream<proto::TableListResponse>;
    type TableListByIcebergEnabledStream = TonicStream<proto::TableListByIcebergEnabledResponse>;

    type ColumnCreateOrGetManyUncheckedStream =
        TonicStream<proto::ColumnCreateOrGetManyUncheckedResponse>;
    type ColumnListByNamespaceIdStream = TonicStream<proto::ColumnListByNamespaceIdResponse>;
    type ColumnListByTableIdStream = TonicStream<proto::ColumnListByTableIdResponse>;
    type ColumnListStream = TonicStream<proto::ColumnListResponse>;

    type PartitionGetByIdBatchStream = TonicStream<proto::PartitionGetByIdBatchResponse>;
    type PartitionListByTableIdStream = TonicStream<proto::PartitionListByTableIdResponse>;
    type PartitionListIdsStream = TonicStream<proto::PartitionListIdsResponse>;
    type PartitionGetInSkippedCompactionsStream =
        TonicStream<proto::PartitionGetInSkippedCompactionsResponse>;
    type PartitionListSkippedCompactionsStream =
        TonicStream<proto::PartitionListSkippedCompactionsResponse>;
    type PartitionMostRecentNStream = TonicStream<proto::PartitionMostRecentNResponse>;
    type PartitionNewFileBetweenStream = TonicStream<proto::PartitionNewFileBetweenResponse>;
    type PartitionNeedingColdCompactStream =
        TonicStream<proto::PartitionNeedingColdCompactResponse>;
    type PartitionListOldStyleStream = TonicStream<proto::PartitionListOldStyleResponse>;

    type PartitionDeleteByRetentionStream = TonicStream<proto::PartitionDeleteByRetentionResponse>;
    type ParquetFileDeleteOldIdsOnlyStream =
        TonicStream<proto::ParquetFileDeleteOldIdsOnlyResponse>;
    type ParquetFileFlagForDeleteByRetentionStream =
        TonicStream<proto::ParquetFileFlagForDeleteByRetentionResponse>;
    type ParquetFileListByPartitionNotToDeleteBatchStream =
        TonicStream<proto::ParquetFileListByPartitionNotToDeleteBatchResponse>;
    type ParquetFileActiveAsOfStream = TonicStream<proto::ParquetFileActiveAsOfResponse>;
    type ParquetFileExistsByObjectStoreIdBatchStream =
        TonicStream<proto::ParquetFileExistsByObjectStoreIdBatchResponse>;
    type ParquetFileExistsByPartitionAndObjectStoreIdBatchStream =
        TonicStream<proto::ParquetFileExistsByPartitionAndObjectStoreIdBatchResponse>;
    type ParquetFileCreateUpgradeDeleteFullStream =
        TonicStream<proto::ParquetFileCreateUpgradeDeleteFullResponse>;
    type ParquetFileListByTableIdStream = TonicStream<proto::ParquetFileListByTableIdResponse>;
    type ParquetFileListByNamespaceIdStream =
        TonicStream<proto::ParquetFileListByNamespaceIdResponse>;

    async fn root_snapshot(
        &self,
        request: Request<proto::RootSnapshotRequest>,
    ) -> Result<Response<proto::RootSnapshotResponse>, Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let snapshot = repos
            .root()
            .snapshot()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::RootSnapshotResponse {
            generation: snapshot.generation(),
            root: Some(snapshot.into()),
        }))
    }

    async fn namespace_create(
        &self,
        request: Request<proto::NamespaceCreateRequest>,
    ) -> Result<Response<proto::NamespaceCreateResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let ns = repos
            .namespaces()
            .create(
                &req.name.convert().ctx("name")?,
                req.partition_template
                    .convert_opt()
                    .ctx("partition_template")?,
                req.retention_period_ns,
                req.service_protection_limits
                    .map(|l| {
                        let l = NamespaceServiceProtectionLimitsOverride {
                            max_tables: l.max_tables.convert_opt().ctx("max_tables")?,
                            max_columns_per_table: l
                                .max_columns_per_table
                                .convert_opt()
                                .ctx("max_columns_per_table")?,
                        };
                        Ok(l) as Result<_, tonic::Status>
                    })
                    .transpose()?,
            )
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(proto::NamespaceCreateResponse {
            namespace: Some(ns),
        }))
    }

    async fn namespace_update_retention_period(
        &self,
        request: Request<proto::NamespaceUpdateRetentionPeriodRequest>,
    ) -> Result<Response<proto::NamespaceUpdateRetentionPeriodResponse>, tonic::Status> {
        use proto::namespace_update_retention_period_request::Target;
        let (mut repos, req) = self.preprocess_request(request);

        let id = match req.target.required()? {
            Target::Name(name) => {
                repos
                    .namespaces()
                    .get_by_name(&name)
                    .await
                    .map_err(catalog_error_to_status)?
                    .ok_or(tonic::Status::not_found(name.to_string()))?
                    .id
            }
            Target::Id(id) => NamespaceId::new(id),
        };

        let ns = repos
            .namespaces()
            .update_retention_period(id, req.retention_period_ns)
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(
            proto::NamespaceUpdateRetentionPeriodResponse {
                namespace: Some(ns),
            },
        ))
    }

    async fn namespace_list(
        &self,
        request: Request<proto::NamespaceListRequest>,
    ) -> Result<Response<Self::NamespaceListStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let deleted = deserialize_soft_deleted_rows(req.deleted)?;

        let ns_list = repos
            .namespaces()
            .list(deleted)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(ns_list.into_iter().map(|ns| {
                let ns = serialize_namespace(ns);

                Ok(proto::NamespaceListResponse {
                    namespace: Some(ns),
                })
            }))
            .boxed(),
        ))
    }

    async fn namespace_get_by_id(
        &self,
        request: Request<proto::NamespaceGetByIdRequest>,
    ) -> Result<Response<proto::NamespaceGetByIdResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let deleted = deserialize_soft_deleted_rows(req.deleted)?;

        let maybe_ns = repos
            .namespaces()
            .get_by_id(NamespaceId::new(req.id), deleted)
            .await
            .map_err(catalog_error_to_status)?;

        let maybe_ns = maybe_ns.map(serialize_namespace);

        Ok(Response::new(proto::NamespaceGetByIdResponse {
            namespace: maybe_ns,
        }))
    }

    async fn namespace_get_by_name(
        &self,
        request: Request<proto::NamespaceGetByNameRequest>,
    ) -> Result<Response<proto::NamespaceGetByNameResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_ns = repos
            .namespaces()
            .get_by_name(&req.name)
            .await
            .map_err(catalog_error_to_status)?;

        let maybe_ns = maybe_ns.map(serialize_namespace);

        Ok(Response::new(proto::NamespaceGetByNameResponse {
            namespace: maybe_ns,
        }))
    }

    async fn namespace_soft_delete(
        &self,
        request: Request<proto::NamespaceSoftDeleteRequest>,
    ) -> Result<Response<proto::NamespaceSoftDeleteResponse>, tonic::Status> {
        use proto::{
            namespace_soft_delete_request::Target, namespace_soft_delete_response::Deleted,
        };
        let (mut repos, req) = self.preprocess_request(request);

        let id = match req.target.required()? {
            Target::Name(name) => {
                repos
                    .namespaces()
                    .get_by_name(&name)
                    .await
                    .map_err(catalog_error_to_status)?
                    .ok_or(tonic::Status::not_found(name.to_string()))?
                    .id
            }
            Target::Id(v) => NamespaceId::new(v),
        };

        let ns = repos
            .namespaces()
            .soft_delete(id)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::NamespaceSoftDeleteResponse {
            deleted: Some(Deleted::Namespace(serialize_namespace(ns))),
        }))
    }

    async fn namespace_update_table_limit(
        &self,
        request: Request<proto::NamespaceUpdateTableLimitRequest>,
    ) -> Result<Response<proto::NamespaceUpdateTableLimitResponse>, tonic::Status> {
        use proto::namespace_update_table_limit_request::Target;
        let (mut repos, req) = self.preprocess_request(request);

        let id = match req.target.required()? {
            Target::Name(name) => {
                repos
                    .namespaces()
                    .get_by_name(&name)
                    .await
                    .map_err(catalog_error_to_status)?
                    .ok_or(tonic::Status::not_found(name.to_string()))?
                    .id
            }
            Target::Id(id) => NamespaceId::new(id),
        };

        let ns = repos
            .namespaces()
            .update_table_limit(id, req.new_max.convert().ctx("new_max")?)
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(proto::NamespaceUpdateTableLimitResponse {
            namespace: Some(ns),
        }))
    }

    async fn namespace_update_column_limit(
        &self,
        request: Request<proto::NamespaceUpdateColumnLimitRequest>,
    ) -> Result<Response<proto::NamespaceUpdateColumnLimitResponse>, tonic::Status> {
        use proto::namespace_update_column_limit_request::Target;
        let (mut repos, req) = self.preprocess_request(request);

        let id = match req.target.required()? {
            Target::Name(name) => {
                repos
                    .namespaces()
                    .get_by_name(&name)
                    .await
                    .map_err(catalog_error_to_status)?
                    .ok_or(tonic::Status::not_found(name.to_string()))?
                    .id
            }
            Target::Id(id) => NamespaceId::new(id),
        };

        let ns = repos
            .namespaces()
            .update_column_limit(id, req.new_max.convert().ctx("new_max")?)
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(proto::NamespaceUpdateColumnLimitResponse {
            namespace: Some(ns),
        }))
    }

    async fn namespace_snapshot(
        &self,
        request: Request<proto::NamespaceSnapshotRequest>,
    ) -> Result<Response<proto::NamespaceSnapshotResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let snapshot = repos
            .namespaces()
            .snapshot(NamespaceId::new(req.namespace_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::NamespaceSnapshotResponse {
            generation: snapshot.generation(),
            namespace: Some(snapshot.into()),
        }))
    }

    async fn namespace_snapshot_by_name(
        &self,
        request: Request<proto::NamespaceSnapshotByNameRequest>,
    ) -> Result<Response<proto::NamespaceSnapshotByNameResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);
        let snapshot = repos
            .namespaces()
            .snapshot_by_name(&req.name)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::NamespaceSnapshotByNameResponse {
            generation: snapshot.generation(),
            namespace: Some(snapshot.into()),
        }))
    }

    async fn namespace_rename(
        &self,
        request: Request<proto::NamespaceRenameRequest>,
    ) -> Result<Response<proto::NamespaceRenameResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);
        let new_name = NamespaceName::new(req.new_name)
            .map_err(|v| Status::invalid_argument(v.to_string()))?;
        let ns = repos
            .namespaces()
            .rename(NamespaceId::new(req.id), new_name)
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(proto::NamespaceRenameResponse {
            namespace: Some(ns),
        }))
    }

    async fn namespace_undelete(
        &self,
        request: Request<proto::NamespaceUndeleteRequest>,
    ) -> Result<Response<proto::NamespaceUndeleteResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);
        let ns = repos
            .namespaces()
            .undelete(NamespaceId::new(req.id))
            .await
            .map_err(catalog_error_to_status)?;

        let ns = serialize_namespace(ns);

        Ok(Response::new(proto::NamespaceUndeleteResponse {
            namespace: Some(ns),
        }))
    }

    async fn table_create(
        &self,
        request: Request<proto::TableCreateRequest>,
    ) -> Result<Response<proto::TableCreateResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let table = repos
            .tables()
            .create(
                &req.name,
                req.partition_template.convert().ctx("partition_template")?,
                NamespaceId::new(req.namespace_id),
            )
            .await
            .map_err(catalog_error_to_status)?;

        let table = serialize_table(table);

        Ok(Response::new(proto::TableCreateResponse {
            table: Some(table),
        }))
    }

    async fn table_get_by_id(
        &self,
        request: Request<proto::TableGetByIdRequest>,
    ) -> Result<Response<proto::TableGetByIdResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_table = repos
            .tables()
            .get_by_id(TableId::new(req.id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::TableGetByIdResponse {
            table: maybe_table.map(serialize_table),
        }))
    }

    async fn table_get_by_namespace_and_name(
        &self,
        request: Request<proto::TableGetByNamespaceAndNameRequest>,
    ) -> Result<Response<proto::TableGetByNamespaceAndNameResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_table = repos
            .tables()
            .get_by_namespace_and_name(NamespaceId::new(req.namespace_id), &req.name)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::TableGetByNamespaceAndNameResponse {
            table: maybe_table.map(serialize_table),
        }))
    }

    async fn table_list_by_namespace_id(
        &self,
        request: Request<proto::TableListByNamespaceIdRequest>,
    ) -> Result<Response<Self::TableListByNamespaceIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let table_list = repos
            .tables()
            .list_by_namespace_id(NamespaceId::new(req.namespace_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(table_list.into_iter().map(|table| {
                let table = serialize_table(table);
                Ok(proto::TableListByNamespaceIdResponse { table: Some(table) })
            }))
            .boxed(),
        ))
    }

    async fn table_list(
        &self,
        request: Request<proto::TableListRequest>,
    ) -> Result<Response<Self::TableListStream>, tonic::Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let table_list = repos
            .tables()
            .list()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(table_list.into_iter().map(|table| {
                let table = serialize_table(table);
                Ok(proto::TableListResponse { table: Some(table) })
            }))
            .boxed(),
        ))
    }

    async fn table_snapshot(
        &self,
        request: Request<proto::TableSnapshotRequest>,
    ) -> Result<Response<proto::TableSnapshotResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let snapshot = repos
            .tables()
            .snapshot(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::TableSnapshotResponse {
            generation: snapshot.generation(),
            table: Some(snapshot.into()),
        }))
    }

    async fn table_list_by_iceberg_enabled(
        &self,
        request: Request<proto::TableListByIcebergEnabledRequest>,
    ) -> Result<Response<Self::TableListByIcebergEnabledStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let tables_with_iceberg = repos
            .tables()
            .list_by_iceberg_enabled(NamespaceId::new(req.namespace_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(tables_with_iceberg.into_iter().map(|table_id| {
                Ok(proto::TableListByIcebergEnabledResponse {
                    table_id: table_id.get(),
                })
            }))
            .boxed(),
        ))
    }

    async fn table_enable_iceberg(
        &self,
        request: Request<proto::TableEnableIcebergRequest>,
    ) -> Result<Response<proto::TableEnableIcebergResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        repos
            .tables()
            .enable_iceberg(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::TableEnableIcebergResponse {}))
    }

    async fn table_disable_iceberg(
        &self,
        request: Request<proto::TableDisableIcebergRequest>,
    ) -> Result<Response<proto::TableDisableIcebergResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        repos
            .tables()
            .disable_iceberg(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::TableDisableIcebergResponse {}))
    }

    async fn table_rename(
        &self,
        req: Request<proto::TableRenameRequest>,
    ) -> Result<Response<proto::TableRenameResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(req);

        repos
            .tables()
            .rename(TableId::new(req.table_id), &req.new_name)
            .await
            .map_err(catalog_error_to_status)
            .map(|table| {
                Response::new(proto::TableRenameResponse {
                    table: Some(serialize_table(table)),
                })
            })
    }

    async fn table_soft_delete(
        &self,
        req: Request<proto::TableSoftDeleteRequest>,
    ) -> Result<Response<proto::TableSoftDeleteResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(req);

        repos
            .tables()
            .soft_delete(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)
            .map(|table| {
                Response::new(proto::TableSoftDeleteResponse {
                    table: Some(serialize_table(table)),
                })
            })
    }

    async fn table_undelete(
        &self,
        req: Request<proto::TableUndeleteRequest>,
    ) -> Result<Response<proto::TableUndeleteResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(req);

        repos
            .tables()
            .undelete(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)
            .map(|table| {
                Response::new(proto::TableUndeleteResponse {
                    table: Some(serialize_table(table)),
                })
            })
    }

    async fn column_create_or_get(
        &self,
        request: Request<proto::ColumnCreateOrGetRequest>,
    ) -> Result<Response<proto::ColumnCreateOrGetResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let column_type = deserialize_column_type(req.column_type)?;

        let column = repos
            .columns()
            .create_or_get(&req.name, TableId::new(req.table_id), column_type)
            .await
            .map_err(catalog_error_to_status)?;

        let column = serialize_column(column);

        Ok(Response::new(proto::ColumnCreateOrGetResponse {
            column: Some(column),
        }))
    }

    async fn column_create_or_get_many_unchecked(
        &self,
        request: Request<proto::ColumnCreateOrGetManyUncheckedRequest>,
    ) -> Result<Response<Self::ColumnCreateOrGetManyUncheckedStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let columns = req
            .columns
            .iter()
            .map(|(name, t)| {
                let t = deserialize_column_type(*t)?;
                Ok((name.as_str(), t))
            })
            .collect::<Result<_, tonic::Status>>()?;

        let column_list = repos
            .columns()
            .create_or_get_many_unchecked(TableId::new(req.table_id), columns)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(column_list.into_iter().map(|column| {
                let column = serialize_column(column);
                Ok(proto::ColumnCreateOrGetManyUncheckedResponse {
                    column: Some(column),
                })
            }))
            .boxed(),
        ))
    }

    async fn column_list_by_namespace_id(
        &self,
        request: Request<proto::ColumnListByNamespaceIdRequest>,
    ) -> Result<Response<Self::ColumnListByNamespaceIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let column_list = repos
            .columns()
            .list_by_namespace_id(NamespaceId::new(req.namespace_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(column_list.into_iter().map(|column| {
                let column = serialize_column(column);
                Ok(proto::ColumnListByNamespaceIdResponse {
                    column: Some(column),
                })
            }))
            .boxed(),
        ))
    }

    async fn column_list_by_table_id(
        &self,
        request: Request<proto::ColumnListByTableIdRequest>,
    ) -> Result<Response<Self::ColumnListByTableIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let column_list = repos
            .columns()
            .list_by_table_id(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(column_list.into_iter().map(|column| {
                let column = serialize_column(column);
                Ok(proto::ColumnListByTableIdResponse {
                    column: Some(column),
                })
            }))
            .boxed(),
        ))
    }

    async fn column_list(
        &self,
        request: Request<proto::ColumnListRequest>,
    ) -> Result<Response<Self::ColumnListStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        // Default an empty request to AllRows for backwards compatibility
        let deleted = deserialize_soft_deleted_rows(
            req.deleted
                .unwrap_or(proto::SoftDeletedRows::AllRows as i32),
        )?;

        let column_list = repos
            .columns()
            .list(deleted)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(column_list.into_iter().map(|column| {
                let column = serialize_column(column);
                Ok(proto::ColumnListResponse {
                    column: Some(column),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_create_or_get(
        &self,
        request: Request<proto::PartitionCreateOrGetRequest>,
    ) -> Result<Response<proto::PartitionCreateOrGetResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let partition = repos
            .partitions()
            .create_or_get(PartitionKey::from(req.key), TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        let partition = serialize_partition(partition);

        Ok(Response::new(proto::PartitionCreateOrGetResponse {
            partition: Some(partition),
        }))
    }

    async fn partition_get_by_id_batch(
        &self,
        request: Request<proto::PartitionGetByIdBatchRequest>,
    ) -> Result<Response<Self::PartitionGetByIdBatchStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let partition_ids = req
            .partition_ids
            .into_iter()
            .map(PartitionId::new)
            .collect::<Vec<_>>();

        let partition_list = repos
            .partitions()
            .get_by_id_batch(&partition_ids)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(partition_list.into_iter().map(|partition| {
                let partition = serialize_partition(partition);
                Ok(proto::PartitionGetByIdBatchResponse {
                    partition: Some(partition),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_list_by_table_id(
        &self,
        request: Request<proto::PartitionListByTableIdRequest>,
    ) -> Result<Response<Self::PartitionListByTableIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let partition_list = repos
            .partitions()
            .list_by_table_id(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(partition_list.into_iter().map(|partition| {
                let partition = serialize_partition(partition);
                Ok(proto::PartitionListByTableIdResponse {
                    partition: Some(partition),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_list_ids(
        &self,
        request: Request<proto::PartitionListIdsRequest>,
    ) -> Result<Response<Self::PartitionListIdsStream>, tonic::Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let id_list = repos
            .partitions()
            .list_ids()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|id| {
                Ok(proto::PartitionListIdsResponse {
                    partition_id: id.get(),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_cas_sort_key(
        &self,
        request: Request<proto::PartitionCasSortKeyRequest>,
    ) -> Result<Response<proto::PartitionCasSortKeyResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let res = repos
            .partitions()
            .cas_sort_key(
                PartitionId::new(req.partition_id),
                req.old_sort_key_ids.map(deserialize_sort_key_ids).as_ref(),
                &deserialize_sort_key_ids(req.new_sort_key_ids.required().ctx("new_sort_key_ids")?),
            )
            .await;

        match res {
            Ok(partition) => Ok(Response::new(proto::PartitionCasSortKeyResponse {
                res: Some(proto::partition_cas_sort_key_response::Res::Partition(
                    serialize_partition(partition),
                )),
            })),
            Err(CasFailure::ValueMismatch(sort_key_ids)) => {
                Ok(Response::new(proto::PartitionCasSortKeyResponse {
                    res: Some(proto::partition_cas_sort_key_response::Res::CurrentSortKey(
                        serialize_sort_key_ids(&sort_key_ids),
                    )),
                }))
            }
            Err(CasFailure::QueryError(e)) => Err(catalog_error_to_status(e)),
        }
    }

    async fn partition_record_skipped_compaction(
        &self,
        request: Request<proto::PartitionRecordSkippedCompactionRequest>,
    ) -> Result<Response<proto::PartitionRecordSkippedCompactionResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        repos
            .partitions()
            .record_skipped_compaction(
                PartitionId::new(req.partition_id),
                &req.reason,
                req.num_files as usize,
                req.limit_num_files as usize,
                req.limit_num_files_first_in_partition as usize,
                req.estimated_bytes,
                req.limit_bytes,
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            proto::PartitionRecordSkippedCompactionResponse {},
        ))
    }

    async fn partition_get_in_skipped_compactions(
        &self,
        request: Request<proto::PartitionGetInSkippedCompactionsRequest>,
    ) -> Result<Response<Self::PartitionGetInSkippedCompactionsStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let partition_ids = req
            .partition_ids
            .into_iter()
            .map(PartitionId::new)
            .collect::<Vec<_>>();

        let skipped_compaction_list = repos
            .partitions()
            .get_in_skipped_compactions(&partition_ids)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(skipped_compaction_list.into_iter().map(|sc| {
                let sc = serialize_skipped_compaction(sc);
                Ok(proto::PartitionGetInSkippedCompactionsResponse {
                    skipped_compaction: Some(sc),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_list_skipped_compactions(
        &self,
        request: Request<proto::PartitionListSkippedCompactionsRequest>,
    ) -> Result<Response<Self::PartitionListSkippedCompactionsStream>, tonic::Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let skipped_compaction_list = repos
            .partitions()
            .list_skipped_compactions()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(skipped_compaction_list.into_iter().map(|sc| {
                let sc = serialize_skipped_compaction(sc);
                Ok(proto::PartitionListSkippedCompactionsResponse {
                    skipped_compaction: Some(sc),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_delete_skipped_compactions(
        &self,
        request: Request<proto::PartitionDeleteSkippedCompactionsRequest>,
    ) -> Result<Response<proto::PartitionDeleteSkippedCompactionsResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(PartitionId::new(req.partition_id))
            .await
            .map_err(catalog_error_to_status)?;

        let maybe_skipped_compaction = maybe_skipped_compaction.map(serialize_skipped_compaction);

        Ok(Response::new(
            proto::PartitionDeleteSkippedCompactionsResponse {
                skipped_compaction: maybe_skipped_compaction,
            },
        ))
    }

    async fn partition_most_recent_n(
        &self,
        request: Request<proto::PartitionMostRecentNRequest>,
    ) -> Result<Response<Self::PartitionMostRecentNStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let partition_list = repos
            .partitions()
            .most_recent_n(req.n as usize)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(partition_list.into_iter().map(|partition| {
                let partition = serialize_partition(partition);
                Ok(proto::PartitionMostRecentNResponse {
                    partition: Some(partition),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_new_file_between(
        &self,
        request: Request<proto::PartitionNewFileBetweenRequest>,
    ) -> Result<Response<Self::PartitionNewFileBetweenStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let id_list = repos
            .partitions()
            .partitions_new_file_between(
                Timestamp::new(req.minimum_time),
                req.maximum_time.map(Timestamp::new),
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|id| {
                Ok(proto::PartitionNewFileBetweenResponse {
                    partition_id: id.get(),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_update_cold_compact(
        &self,
        request: Request<proto::PartitionUpdateColdCompactRequest>,
    ) -> Result<Response<proto::PartitionUpdateColdCompactResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        repos
            .partitions()
            .update_cold_compact(
                PartitionId::new(req.partition_id),
                Timestamp::new(req.cold_compact_at),
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::PartitionUpdateColdCompactResponse {}))
    }

    async fn partition_needing_cold_compact(
        &self,
        request: Request<proto::PartitionNeedingColdCompactRequest>,
    ) -> Result<Response<Self::PartitionNeedingColdCompactStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let id_list = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(req.maximum_time), req.n as usize)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|id| {
                Ok(proto::PartitionNeedingColdCompactResponse {
                    partition_id: id.get(),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_list_old_style(
        &self,
        request: Request<proto::PartitionListOldStyleRequest>,
    ) -> Result<Response<Self::PartitionListOldStyleStream>, tonic::Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let partition_list = repos
            .partitions()
            .list_old_style()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(partition_list.into_iter().map(|partition| {
                let partition = serialize_partition(partition);
                Ok(proto::PartitionListOldStyleResponse {
                    partition: Some(partition),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_delete_by_retention(
        &self,
        request: Request<PartitionDeleteByRetentionRequest>,
    ) -> Result<Response<Self::PartitionDeleteByRetentionStream>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        // Having `partition_cutoff_seconds` be an optional value is to facilitate deployment of
        // the addition of the `partition_cutoff_seconds` field. After deployment to all clients,
        // the default value set by `iox_catalog::constants::PARTITION_DELETION_CUTOFF` should
        // always be sent.
        let partition_cutoff = req
            .partition_cutoff_seconds
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(PARTITION_DELETION_CUTOFF);

        let res = repos
            .partitions()
            .delete_by_retention(partition_cutoff)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(res.into_iter().map(|(t_id, p_id)| {
                Ok(proto::PartitionDeleteByRetentionResponse {
                    table_id: t_id.get(),
                    partition_id: p_id.get(),
                })
            }))
            .boxed(),
        ))
    }

    async fn partition_snapshot(
        &self,
        request: Request<proto::PartitionSnapshotRequest>,
    ) -> Result<Response<proto::PartitionSnapshotResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let snapshot = repos
            .partitions()
            .snapshot(PartitionId::new(req.partition_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::PartitionSnapshotResponse {
            generation: snapshot.generation(),
            partition: Some(snapshot.into()),
        }))
    }

    async fn parquet_file_flag_for_delete_by_retention(
        &self,
        request: Request<proto::ParquetFileFlagForDeleteByRetentionRequest>,
    ) -> Result<Response<Self::ParquetFileFlagForDeleteByRetentionStream>, tonic::Status> {
        let (mut repos, _req) = self.preprocess_request(request);

        let id_list = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|(p_id, os_id)| {
                let object_store_id = serialize_object_store_id(os_id);
                Ok(proto::ParquetFileFlagForDeleteByRetentionResponse {
                    partition_id: p_id.get(),
                    object_store_id: Some(object_store_id),
                })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_delete_old_ids_only(
        &self,
        request: Request<proto::ParquetFileDeleteOldIdsOnlyRequest>,
    ) -> Result<Response<Self::ParquetFileDeleteOldIdsOnlyStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        #[expect(deprecated)]
        let id_list = repos
            .parquet_files()
            .delete_old_ids_only(Timestamp::new(req.older_than))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|id| {
                let object_store_id = serialize_object_store_id(id);
                Ok(proto::ParquetFileDeleteOldIdsOnlyResponse {
                    object_store_id: Some(object_store_id),
                })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_delete_old_ids_count(
        &self,
        request: Request<ParquetFileDeleteOldIdsCountRequest>,
    ) -> Result<Response<ParquetFileDeleteOldIdsCountResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);
        let ParquetFileDeleteOldIdsCountRequest { older_than, limit } = req;

        let num_deleted = repos
            .parquet_files()
            .delete_old_ids_count(
                Timestamp::new(older_than),
                // At time of writing, this limit should always be `Some(_)`, since that is what we
                // have written the code in the garbage collector to do. However, we can't make
                // this a non-optional field because we need to keep the grpc interface
                // backwards-compatible. So here, if the limit is not specified, we just unwrap to
                // the limit that we previously were using in this code.
                limit.unwrap_or(10_000),
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::ParquetFileDeleteOldIdsCountResponse {
            num_deleted,
        }))
    }

    async fn parquet_file_list_by_partition_not_to_delete_batch(
        &self,
        request: Request<proto::ParquetFileListByPartitionNotToDeleteBatchRequest>,
    ) -> Result<Response<Self::ParquetFileListByPartitionNotToDeleteBatchStream>, tonic::Status>
    {
        let (mut repos, req) = self.preprocess_request(request);

        let partition_ids = req
            .partition_ids
            .into_iter()
            .map(PartitionId::new)
            .collect::<Vec<_>>();

        let file_list = repos
            .parquet_files()
            .list_by_partition_not_to_delete_batch(partition_ids)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(file_list.into_iter().map(|file| {
                let file = serialize_parquet_file(file);
                Ok(proto::ParquetFileListByPartitionNotToDeleteBatchResponse {
                    parquet_file: Some(file),
                })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_active_as_of(
        &self,
        _request: Request<proto::ParquetFileActiveAsOfRequest>,
    ) -> Result<Response<Self::ParquetFileActiveAsOfStream>, tonic::Status> {
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    async fn parquet_file_get_by_object_store_id(
        &self,
        request: Request<proto::ParquetFileGetByObjectStoreIdRequest>,
    ) -> Result<Response<proto::ParquetFileGetByObjectStoreIdResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_file = repos
            .parquet_files()
            .get_by_object_store_id(deserialize_object_store_id(
                req.object_store_id.required().ctx("object_store_id")?,
            ))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            proto::ParquetFileGetByObjectStoreIdResponse {
                parquet_file: maybe_file.map(serialize_parquet_file),
            },
        ))
    }

    async fn parquet_file_exists_by_object_store_id_batch(
        &self,
        request: Request<tonic::Streaming<proto::ParquetFileExistsByObjectStoreIdBatchRequest>>,
    ) -> Result<Response<Self::ParquetFileExistsByObjectStoreIdBatchStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let object_store_ids = req
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))
            .and_then(async move |req| {
                Ok(deserialize_object_store_id(
                    req.object_store_id.required().ctx("object_store_id")?,
                ))
            })
            .try_collect::<Vec<_>>()
            .await?;

        let id_list = repos
            .parquet_files()
            .exists_by_object_store_id_batch(object_store_ids)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|id| {
                let object_store_id = serialize_object_store_id(id);
                Ok(proto::ParquetFileExistsByObjectStoreIdBatchResponse {
                    object_store_id: Some(object_store_id),
                })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_exists_by_partition_and_object_store_id_batch(
        &self,
        request: Request<
            tonic::Streaming<proto::ParquetFileExistsByPartitionAndObjectStoreIdBatchRequest>,
        >,
    ) -> Result<
        Response<Self::ParquetFileExistsByPartitionAndObjectStoreIdBatchStream>,
        tonic::Status,
    > {
        let (mut repos, req) = self.preprocess_request(request);

        let ids = req
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))
            .and_then(async move |req| {
                let partiton_id = PartitionId::new(req.partition_id);
                let object_store_id = deserialize_object_store_id(
                    req.object_store_id.required().ctx("object_store_id")?,
                );
                Ok((partiton_id, object_store_id))
            })
            .try_collect::<Vec<_>>()
            .await?;

        let id_list = repos
            .parquet_files()
            .exists_by_partition_and_object_store_id_batch(ids)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(id_list.into_iter().map(|(partition_id, object_store_id)| {
                let object_store_id = serialize_object_store_id(object_store_id);
                Ok(
                    proto::ParquetFileExistsByPartitionAndObjectStoreIdBatchResponse {
                        object_store_id: Some(object_store_id),
                        partition_id: partition_id.get(),
                    },
                )
            }))
            .boxed(),
        ))
    }

    /// Prost doesn't currently generate deprecated annotations for functions that correspond to
    /// deprecated rpc functions, but this is essentially deprecated. Please use
    /// `parquet_file_create_upgrade_delete_full` instead.
    async fn parquet_file_create_upgrade_delete(
        &self,
        request: Request<proto::ParquetFileCreateUpgradeDeleteRequest>,
    ) -> Result<Response<proto::ParquetFileCreateUpgradeDeleteResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let delete = req
            .delete
            .into_iter()
            .map(deserialize_object_store_id)
            .collect::<Vec<_>>();
        let upgrade = req
            .upgrade
            .into_iter()
            .map(deserialize_object_store_id)
            .collect::<Vec<_>>();
        let create = req
            .create
            .into_iter()
            .map(deserialize_parquet_file_params)
            .collect::<Result<Vec<_>, _>>()?;

        let created = repos
            .parquet_files()
            .create_upgrade_delete(
                PartitionId::new(req.partition_id),
                &delete,
                &upgrade,
                &create,
                req.target_level.convert().ctx("target_level")?,
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            proto::ParquetFileCreateUpgradeDeleteResponse {
                created_parquet_file_ids: created.into_iter().map(|file| file.id.get()).collect(),
            },
        ))
    }

    async fn parquet_file_create_upgrade_delete_full(
        &self,
        request: Request<proto::ParquetFileCreateUpgradeDeleteFullRequest>,
    ) -> Result<Response<Self::ParquetFileCreateUpgradeDeleteFullStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let delete = req
            .delete
            .into_iter()
            .map(deserialize_object_store_id)
            .collect::<Vec<_>>();
        let upgrade = req
            .upgrade
            .into_iter()
            .map(deserialize_object_store_id)
            .collect::<Vec<_>>();
        let create = req
            .create
            .into_iter()
            .map(deserialize_parquet_file_params)
            .collect::<Result<Vec<_>, _>>()?;

        let created = repos
            .parquet_files()
            .create_upgrade_delete(
                PartitionId::new(req.partition_id),
                &delete,
                &upgrade,
                &create,
                req.target_level.convert().ctx("target_level")?,
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(created.into_iter().map(|file| {
                let file = serialize_parquet_file(file);
                Ok(proto::ParquetFileCreateUpgradeDeleteFullResponse {
                    parquet_file: Some(file),
                })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_list_by_table_id(
        &self,
        request: Request<proto::ParquetFileListByTableIdRequest>,
    ) -> Result<Response<Self::ParquetFileListByTableIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let compaction_level = if let Some(l) = req.compaction_level {
            Some(l.try_into().map_err(|e| {
                catalog_error_to_status(
                    crate::interface::UnhandledError::GrpcSerialization {
                        source: Arc::new(e),
                    }
                    .into(),
                )
            })?)
        } else {
            None
        };

        let files = repos
            .parquet_files()
            .list_by_table_id(TableId::new(req.table_id), compaction_level)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(files.into_iter().map(|f| {
                let file = serialize_parquet_file(f);
                Ok(proto::ParquetFileListByTableIdResponse { file: Some(file) })
            }))
            .boxed(),
        ))
    }

    async fn parquet_file_list_by_namespace_id(
        &self,
        request: Request<proto::ParquetFileListByNamespaceIdRequest>,
    ) -> Result<Response<Self::ParquetFileListByNamespaceIdStream>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let deleted = deserialize_soft_deleted_rows(req.deleted)?;

        let files = repos
            .parquet_files()
            .list_by_namespace_id(NamespaceId::new(req.namespace_id), deleted)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(
            futures::stream::iter(files.into_iter().map(|f| {
                let file = serialize_parquet_file(f);
                Ok(proto::ParquetFileListByNamespaceIdResponse { file: Some(file) })
            }))
            .boxed(),
        ))
    }

    async fn get_time(
        &self,
        _request: Request<proto::GetTimeRequest>,
    ) -> Result<Response<proto::GetTimeResponse>, tonic::Status> {
        match self.catalog.get_time().await {
            Ok(t) => Ok(Response::new(proto::GetTimeResponse {
                time: Some(serialize_timestamp(t)),
            })),
            Err(e) => {
                return Err(catalog_error_to_status(e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::grpc::server::GrpcCatalogServer;
    use crate::mem::MemCatalog;
    use generated_types::influxdata::iox::catalog::v2::ColumnListRequest;
    use generated_types::influxdata::iox::catalog::v2::catalog_service_server::CatalogService;
    use iox_time::Time;
    use std::sync::Arc;
    use tonic::Request;

    #[tokio::test]
    async fn test_rollout() {
        // This test demonstrates a scenario that caused us to error when testing the rollout.
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(iox_time::MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(MemCatalog::new(metrics, time_provider));
        let server = GrpcCatalogServer::new(catalog);
        let req = Request::new(ColumnListRequest { deleted: None });
        let res = server.column_list(req).await;
        assert!(res.is_ok());
    }
}
