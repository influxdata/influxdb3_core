//! gRPC server implementation.

use std::{pin::Pin, sync::Arc};

use crate::{
    grpc::serialization::{
        catalog_error_to_status, deserialize_column_type, deserialize_object_store_id,
        deserialize_parquet_file_params, deserialize_soft_deleted_rows, deserialize_sort_key_ids,
        serialize_column, serialize_namespace, serialize_object_store_id, serialize_parquet_file,
        serialize_partition, serialize_skipped_compaction, serialize_sort_key_ids, serialize_table,
        ContextExt, ConvertExt, ConvertOptExt, RequiredExt,
    },
    interface::{CasFailure, Catalog, RepoCollection},
};
use async_trait::async_trait;
use data_types::{
    NamespaceId, NamespaceServiceProtectionLimitsOverride, PartitionId, PartitionKey, TableId,
    Timestamp,
};
use futures::{Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::catalog::v2 as proto;
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

        repos.set_span_context(req.extensions().get().cloned());

        (repos, req.into_inner())
    }
}

#[async_trait]
impl proto::catalog_service_server::CatalogService for GrpcCatalogServer {
    type NamespaceListStream = TonicStream<proto::NamespaceListResponse>;

    type TableListByNamespaceIdStream = TonicStream<proto::TableListByNamespaceIdResponse>;
    type TableListStream = TonicStream<proto::TableListResponse>;

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

    type ParquetFileFlagForDeleteByRetentionStream =
        TonicStream<proto::ParquetFileFlagForDeleteByRetentionResponse>;
    type ParquetFileDeleteOldIdsOnlyStream =
        TonicStream<proto::ParquetFileDeleteOldIdsOnlyResponse>;
    type ParquetFileListByPartitionNotToDeleteBatchStream =
        TonicStream<proto::ParquetFileListByPartitionNotToDeleteBatchResponse>;
    type ParquetFileExistsByObjectStoreIdBatchStream =
        TonicStream<proto::ParquetFileExistsByObjectStoreIdBatchResponse>;

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
        let (mut repos, req) = self.preprocess_request(request);

        let ns = repos
            .namespaces()
            .update_retention_period(&req.name, req.retention_period_ns)
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

        let deleted = deserialize_soft_deleted_rows(req.deleted)?;

        let maybe_ns = repos
            .namespaces()
            .get_by_name(&req.name, deleted)
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
        let (mut repos, req) = self.preprocess_request(request);

        let id = repos
            .namespaces()
            .soft_delete(&req.name)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::NamespaceSoftDeleteResponse {
            namespace_id: id.get(),
        }))
    }

    async fn namespace_update_table_limit(
        &self,
        request: Request<proto::NamespaceUpdateTableLimitRequest>,
    ) -> Result<Response<proto::NamespaceUpdateTableLimitResponse>, tonic::Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let ns = repos
            .namespaces()
            .update_table_limit(&req.name, req.new_max.convert().ctx("new_max")?)
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
        let (mut repos, req) = self.preprocess_request(request);

        let ns = repos
            .namespaces()
            .update_column_limit(&req.name, req.new_max.convert().ctx("new_max")?)
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
        let (mut repos, _req) = self.preprocess_request(request);

        let column_list = repos
            .columns()
            .list()
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
            .and_then(|req| async move {
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

        let id_list = repos
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
                created_parquet_file_ids: id_list.into_iter().map(|id| id.get()).collect(),
            },
        ))
    }
}
