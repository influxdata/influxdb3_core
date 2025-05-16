//! gRPC server implementation of `CatalogStorageService`

use std::sync::Arc;

use crate::interface::{
    Catalog, NamespaceSorting, PaginationOptions, RepoCollection, SoftDeletedRows, TableSorting,
};
use crate::util_serialization::catalog_error_to_status;

use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use generated_types::{Request, Response, Status, influxdata::iox::catalog_storage::v1 as proto};

/// gRPC server that provides:
/// - namespace with storage, such as size and table count
/// - table with storage, such as size
#[derive(Debug)]
pub struct CatalogStorageServer {
    catalog: Arc<dyn Catalog>,
}

impl CatalogStorageServer {
    /// Create a new [`CatalogStorageServer`].
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Get service for integration with tonic.
    pub fn service(
        &self,
    ) -> proto::catalog_storage_service_server::CatalogStorageServiceServer<Self> {
        let this = Self {
            catalog: Arc::clone(&self.catalog),
        };
        proto::catalog_storage_service_server::CatalogStorageServiceServer::new(this)
    }

    /// Pre-process request by extracting tracing information and setting up the [`RepoCollection`].
    fn preprocess_request<T>(&self, req: Request<T>) -> (Box<dyn RepoCollection>, T) {
        let mut repos = self.catalog.repositories();

        repos.set_span_context(req.extensions().get().cloned());

        (repos, req.into_inner())
    }
}

#[async_trait]
impl proto::catalog_storage_service_server::CatalogStorageService for CatalogStorageServer {
    async fn get_namespaces_with_storage(
        &self,
        request: Request<proto::GetNamespacesWithStorageRequest>,
    ) -> Result<Response<proto::GetNamespacesWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let sorting = NamespaceSorting::from((req.sort_field, req.sort_direction));
        let pagination = PaginationOptions::from((req.page_number, req.page_size));
        let deleted = SoftDeletedRows::from(req.deleted);

        let paginated_namespaces = repos
            .namespaces()
            .list_storage(Some(sorting), Some(pagination), deleted)
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetNamespacesWithStorageResponse {
            namespaces_with_storage: paginated_namespaces
                .items
                .into_iter()
                .map(From::from)
                .collect(),
            total: paginated_namespaces.total,
            pages: paginated_namespaces.pages,
        }))
    }

    async fn get_namespace_with_storage(
        &self,
        request: Request<proto::GetNamespaceWithStorageRequest>,
    ) -> Result<Response<proto::GetNamespaceWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_namespace_with_storage = repos
            .namespaces()
            .get_storage_by_id(NamespaceId::new(req.id), SoftDeletedRows::from(req.deleted))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetNamespaceWithStorageResponse {
            namespace_with_storage: maybe_namespace_with_storage.map(From::from),
        }))
    }

    async fn get_tables_with_storage(
        &self,
        request: Request<proto::GetTablesWithStorageRequest>,
    ) -> Result<Response<proto::GetTablesWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let sorting = TableSorting::from((req.sort_field, req.sort_direction));
        let pagination = PaginationOptions::from((req.page_number, req.page_size));
        let deleted = SoftDeletedRows::from(req.deleted);

        let paginated_tables = repos
            .tables()
            .list_storage_by_namespace_id(
                NamespaceId::new(req.namespace_id),
                Some(sorting),
                Some(pagination),
                deleted,
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetTablesWithStorageResponse {
            tables_with_storage: paginated_tables.items.into_iter().map(From::from).collect(),
            total: paginated_tables.total,
            pages: paginated_tables.pages,
        }))
    }

    async fn get_table_with_storage(
        &self,
        request: Request<proto::GetTableWithStorageRequest>,
    ) -> Result<Response<proto::GetTableWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_table_with_storage = repos
            .tables()
            .get_storage_by_id(
                TableId::new(req.table_id),
                SoftDeletedRows::from(req.deleted),
            )
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetTableWithStorageResponse {
            table_with_storage: maybe_table_with_storage.map(From::from),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride,
    };
    use data_types::{
        MaxColumnsPerTable, MaxTables, NamespaceWithStorage, TableWithStorage, Timestamp,
    };
    use generated_types::influxdata::iox::common::v1::SoftDeleted;
    use generated_types::{
        Request,
        influxdata::iox::catalog_storage::v1::{
            self as proto, catalog_storage_service_server::CatalogStorageService,
        },
    };
    use iox_time::SystemProvider;
    use test_helpers::maybe_start_logging;

    use crate::interface::{NamespaceSortField, SortDirection, TableSortField};
    use crate::storage::server::CatalogStorageServer;
    use crate::test_helpers::{
        arbitrary_namespace, create_and_get_file, delete_file, setup_table_and_partition,
    };
    use crate::{interface::Catalog, mem::MemCatalog};

    #[tokio::test]
    async fn test_catalog_storage_get_namespaces_with_storage() {
        maybe_start_logging();
        let namespace_name_1 = "namespace_name_1";
        let namespace_name_2 = "namespace_name_2";
        let internal_name_1 = "_influx_test";

        // Set up catalog, server, and create a namespace
        let catalog = catalog();
        let server = CatalogStorageServer::new(Arc::clone(&catalog));
        let mut repos = catalog.repositories();
        let namespace_1 = arbitrary_namespace(&mut *repos, namespace_name_1).await;

        // Create two tables, their partitions, and files in namespace_1
        let (ns_1_table_1, ns_1_partition_1) =
            setup_table_and_partition(&mut *repos, "ns_1_table_1", &namespace_1).await;
        let (ns_1_table_2, ns_1_partition_2) =
            setup_table_and_partition(&mut *repos, "ns_1_table_2", &namespace_1).await;
        let ns_1_file_1 =
            create_and_get_file(&mut *repos, &namespace_1, &ns_1_table_1, &ns_1_partition_1).await;
        let ns_1_file_2 =
            create_and_get_file(&mut *repos, &namespace_1, &ns_1_table_2, &ns_1_partition_2).await;

        // Verify the two files are in the same namespace, but different tables
        assert_eq!(ns_1_file_1.namespace_id, ns_1_file_2.namespace_id);
        assert_ne!(ns_1_file_1.table_id, ns_1_file_2.table_id);

        // Create internal table to show they aren't returned
        arbitrary_namespace(&mut *repos, internal_name_1).await;

        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: Some(NamespaceSortField::Id.into()),
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected one namespace
        assert_eq!(response.namespaces_with_storage.len(), 1);

        // Expected file size and table count is the sum of all tables
        let expected_namespace_1 = create_expected_namespace_with_storage(
            namespace_name_1,
            &ns_1_file_1,
            ns_1_file_1.file_size_bytes + ns_1_file_2.file_size_bytes,
            2, // expect two tables
            None,
        );
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![expected_namespace_1],
                total: 1,
                pages: 1,
            }
        );

        // Delete a file
        delete_file(&mut *repos, &ns_1_file_1).await;

        // Expected namespace size is the size of the file_2
        let expected_namespace_1 = create_expected_namespace_with_storage(
            namespace_name_1,
            &ns_1_file_2,
            ns_1_file_2.file_size_bytes,
            2, // still expect two tables, even though one of the table has 0 size
            None,
        );
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: Some(NamespaceSortField::Id.into()),
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![expected_namespace_1.clone()],
                total: 1,
                pages: 1,
            }
        );

        // Create another namespace, and its table, partition, and file
        let namespace_2 = arbitrary_namespace(&mut *repos, namespace_name_2).await;
        let (ns_2_table_1, ns_2_partition) =
            setup_table_and_partition(&mut *repos, "ns_2_table_1", &namespace_2).await;
        let ns_2_file =
            create_and_get_file(&mut *repos, &namespace_2, &ns_2_table_1, &ns_2_partition).await;

        // Expected two namespaces
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: Some(NamespaceSortField::Id.into()),
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.namespaces_with_storage.len(), 2);

        // Delete a file in namespace_2
        delete_file(&mut *repos, &ns_2_file).await;

        // Expected two namespaces and namespace_2 has size 0
        let expected_namespace_2 = create_expected_namespace_with_storage(
            namespace_name_2,
            &ns_2_file,
            0, // expect no size
            1, // expect one table even though no files are in this tables
            None,
        );

        // Sort by name desc
        // Expected namespace_1 to be last because it has the "greater" name alphabetically
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: Some(NamespaceSortField::Name.into()),
                sort_direction: Some(SortDirection::Descending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![
                    expected_namespace_2.clone(),
                    expected_namespace_1.clone()
                ],
                total: 2,
                pages: 1,
            }
        );

        // Sort by name asc
        // Expected namespace_1 to be first
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: Some(NamespaceSortField::Name.into()),
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![
                    expected_namespace_1.clone(),
                    expected_namespace_2.clone()
                ],
                total: 2,
                pages: 1,
            }
        );

        // Provide no sort field
        // Expected namespace_1 to be first
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![
                    expected_namespace_1.clone(),
                    expected_namespace_2.clone()
                ],
                total: 2,
                pages: 1,
            }
        );

        // Get page 1
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: Some(1),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![expected_namespace_1.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Get page 2
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: Some(2),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![expected_namespace_2.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Get non-existent page returns an empty list
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: Some(3),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![],
                total: 2,
                pages: 2,
            }
        );

        // Negative page number reverts to default
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: Some(-1),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![expected_namespace_1.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Negative page size reverts to default
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                page_number: Some(1),
                page_size: Some(-1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespaces_with_storage: vec![
                    expected_namespace_1.clone(),
                    expected_namespace_2.clone()
                ],
                total: 2,
                pages: 1,
            }
        );

        // Delete namespace_2
        let deleted_namespace_2 = repos
            .namespaces()
            .soft_delete(data_types::NamespaceId::new(expected_namespace_2.id))
            .await
            .unwrap();
        let expected_namespace_2_deleted = create_expected_namespace_with_storage(
            namespace_name_2,
            &ns_2_file,
            0, // expect no size
            1, // expect one table even though no files are in this tables
            deleted_namespace_2.deleted_at,
        );

        // Helper function to test deletion status
        async fn assert_deletion_status(
            server: &CatalogStorageServer,
            expected_namespaces: Vec<proto::NamespaceWithStorage>,
            expected_total: i64,
            expected_pages: i64,
            deleted: Option<SoftDeleted>,
        ) {
            let response = server
                .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                    page_number: None,
                    page_size: None,
                    sort_field: None,
                    sort_direction: None,
                    deleted: deleted.map(Into::into),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response,
                proto::GetNamespacesWithStorageResponse {
                    namespaces_with_storage: expected_namespaces,
                    total: expected_total,
                    pages: expected_pages,
                }
            );
        }

        // When deletion status is not set, expect only namespace_1 in the list
        assert_deletion_status(&server, vec![expected_namespace_1.clone()], 1, 1, None).await;

        // When deletion status is set to show all, expect both namespace_1 and namespace_2 in the list
        assert_deletion_status(
            &server,
            vec![
                expected_namespace_1.clone(),
                expected_namespace_2_deleted.clone(),
            ],
            2,
            1,
            Some(SoftDeleted::ListAll),
        )
        .await;

        // When deletion status is set to show only active, expect only namespace_1 in the list
        assert_deletion_status(
            &server,
            vec![expected_namespace_1.clone()],
            1,
            1,
            Some(SoftDeleted::OnlyActive),
        )
        .await;

        // When deletion status is set to show only deleted, expect only namespace_2 in the list
        assert_deletion_status(
            &server,
            vec![expected_namespace_2_deleted.clone()],
            1,
            1,
            Some(SoftDeleted::OnlyDeleted),
        )
        .await;
    }

    #[tokio::test]
    async fn test_catalog_storage_get_namespace_with_storage() {
        maybe_start_logging();
        let namespace_name_1 = "namespace_name_1";

        // Set up catalog, server, and create a namespace
        let catalog = catalog();
        let server = CatalogStorageServer::new(Arc::clone(&catalog));
        let mut repos = catalog.repositories();
        let namespace_1 = arbitrary_namespace(&mut *repos, namespace_name_1).await;

        // Create two tables, their partitions, and files in namespace_1
        let (ns_1_table_1, ns_1_partition_1) =
            setup_table_and_partition(&mut *repos, "ns_1_table_1", &namespace_1).await;
        let (ns_1_table_2, ns_1_partition_2) =
            setup_table_and_partition(&mut *repos, "ns_1_table_2", &namespace_1).await;
        let ns_1_file_1 =
            create_and_get_file(&mut *repos, &namespace_1, &ns_1_table_1, &ns_1_partition_1).await;
        let ns_1_file_2 =
            create_and_get_file(&mut *repos, &namespace_1, &ns_1_table_2, &ns_1_partition_2).await;

        // Verify the two files are in the same namespace, but different tables
        assert_eq!(ns_1_file_1.namespace_id, ns_1_file_2.namespace_id);
        assert_ne!(ns_1_file_1.table_id, ns_1_file_2.table_id);

        let response = server
            .get_namespace_with_storage(Request::new(proto::GetNamespaceWithStorageRequest {
                id: ns_1_file_1.namespace_id.get(),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected file size and table count is the sum of all tables
        let expected_namespace_1 = create_expected_namespace_with_storage(
            namespace_name_1,
            &ns_1_file_1,
            ns_1_file_1.file_size_bytes + ns_1_file_2.file_size_bytes,
            2, // expect two tables
            None,
        );
        assert_eq!(
            response,
            proto::GetNamespaceWithStorageResponse {
                namespace_with_storage: Some(expected_namespace_1),
            }
        );

        // Get a non-existent namespace
        let non_existent_namespace = server
            .get_namespace_with_storage(Request::new(proto::GetNamespaceWithStorageRequest {
                id: 100, // non-existent namespace ID
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            non_existent_namespace,
            proto::GetNamespaceWithStorageResponse {
                namespace_with_storage: None,
            }
        );
    }

    #[tokio::test]
    async fn test_catalog_storage_get_tables_with_storage() {
        maybe_start_logging();
        let table_name_1 = "test_table_1";
        let table_name_2 = "test_table_2";

        // Set up catalog, server, and create namespace
        let catalog = catalog();
        let server = CatalogStorageServer::new(Arc::clone(&catalog));
        let mut repos = catalog.repositories();
        let test_namespace = arbitrary_namespace(&mut *repos, "test_namespace").await;

        // Set up table1 and its partition
        let (test_table_1, test_partition_1) =
            setup_table_and_partition(&mut *repos, table_name_1, &test_namespace).await;

        // Create two files
        let table_1_file_1 = create_and_get_file(
            &mut *repos,
            &test_namespace,
            &test_table_1,
            &test_partition_1,
        )
        .await;
        let table_1_file_2 = create_and_get_file(
            &mut *repos,
            &test_namespace,
            &test_table_1,
            &test_partition_1,
        )
        .await;

        // Verify all the files are in the same namespace and table
        assert_eq!(table_1_file_1.namespace_id, table_1_file_2.namespace_id);
        assert_eq!(table_1_file_1.table_id, table_1_file_2.table_id);

        // Get tables with storage
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_1_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected one table
        assert_eq!(response.tables_with_storage.len(), 1);

        // Expected file size is the sum of all files
        let expected_table_1 = create_expected_table_with_storage(
            table_name_1,
            &table_1_file_1,
            table_1_file_1.file_size_bytes + table_1_file_2.file_size_bytes,
            None,
        );
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1],
                total: 1,
                pages: 1,
            }
        );

        // Delete a file
        delete_file(&mut *repos, &table_1_file_1).await;

        // Expected file size is the size of the remaining file
        let expected_table_1 = create_expected_table_with_storage(
            table_name_1,
            &table_1_file_1,
            table_1_file_2.file_size_bytes,
            None,
        );
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_1_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone()],
                total: 1,
                pages: 1,
            }
        );

        // Set up another table and partition
        let (test_table_2, test_partition_2) =
            setup_table_and_partition(&mut *repos, table_name_2, &test_namespace).await;

        // Create one file in table2
        let table_2_file_1 = create_and_get_file(
            &mut *repos,
            &test_namespace,
            &test_table_2,
            &test_partition_2,
        )
        .await;

        // Get tables with storage
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected two tables
        assert_eq!(response.tables_with_storage.len(), 2);

        // Delete a file in table2
        delete_file(&mut *repos, &table_2_file_1).await;

        // Expected two tables and table2 has size 0
        let expected_table_2 =
            create_expected_table_with_storage(table_name_2, &table_2_file_1, 0, None);
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone(), expected_table_2.clone()],
                total: 2,
                pages: 1,
            }
        );

        // Sort by name desc
        // Expected table_2 to be first
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: Some(TableSortField::Name.into()),
                sort_direction: Some(SortDirection::Descending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_2.clone(), expected_table_1.clone()],
                total: 2,
                pages: 1,
            }
        );

        // Sort by name asc
        // Expected table_1 to be first
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: Some(TableSortField::Name.into()),
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone(), expected_table_2.clone()],
                total: 2,
                pages: 1,
            }
        );

        // Provide a sort direction but no sort field
        // Expect no sorting to have been applied
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: None,
                page_size: None,
                sort_field: None,
                sort_direction: Some(SortDirection::Ascending.into()),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone(), expected_table_2.clone()],
                total: 2,
                pages: 1,
            }
        );

        // Get page 1
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: Some(1),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Get page 2
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: Some(2),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_2.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Get non-existent page returns an empty list
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: Some(3),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![],
                total: 2,
                pages: 2,
            }
        );

        // Negative page number reverts to default
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: Some(-1),
                page_size: Some(1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone()],
                total: 2,
                pages: 2,
            }
        );

        // Negative page size reverts to default
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
                page_number: Some(1),
                page_size: Some(-1),
                sort_field: None,
                sort_direction: None,
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                tables_with_storage: vec![expected_table_1.clone(), expected_table_2.clone()],
                total: 2,
                pages: 1,
            }
        );

        // Delete table_2
        let deleted_table_2 = repos.tables().soft_delete(test_table_2.id).await.unwrap();
        let expected_table_2_deleted = create_expected_table_with_storage(
            table_name_2,
            &table_2_file_1,
            0, // expect no size
            deleted_table_2.deleted_at,
        );

        // Helper function to test deletion status
        async fn assert_deletion_status(
            server: &CatalogStorageServer,
            namespace_id: i64,
            expected_tables: Vec<proto::TableWithStorage>,
            expected_total: i64,
            expected_pages: i64,
            deleted: Option<SoftDeleted>,
        ) {
            let response = server
                .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                    namespace_id,
                    page_number: None,
                    page_size: None,
                    sort_field: None,
                    sort_direction: None,
                    deleted: deleted.map(Into::into),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response,
                proto::GetTablesWithStorageResponse {
                    tables_with_storage: expected_tables,
                    total: expected_total,
                    pages: expected_pages,
                }
            );
        }

        // When deletion status is not set, expect only table_1 in the list
        assert_deletion_status(
            &server,
            table_1_file_1.namespace_id.get(),
            vec![expected_table_1.clone()],
            1,
            1,
            None,
        )
        .await;

        // When deletion status is set to show all, expect both table_1 and table_2 in the list
        assert_deletion_status(
            &server,
            table_1_file_1.namespace_id.get(),
            vec![expected_table_1.clone(), expected_table_2_deleted.clone()],
            2,
            1,
            Some(SoftDeleted::ListAll),
        )
        .await;

        // When deletion status is set to show only active, expect only table_1 in the list
        assert_deletion_status(
            &server,
            table_1_file_1.namespace_id.get(),
            vec![expected_table_1.clone()],
            1,
            1,
            Some(SoftDeleted::OnlyActive),
        )
        .await;

        // When deletion status is set to show only deleted, expect only table_2 in the list
        assert_deletion_status(
            &server,
            table_1_file_1.namespace_id.get(),
            vec![expected_table_2_deleted.clone()],
            1,
            1,
            Some(SoftDeleted::OnlyDeleted),
        )
        .await;
    }

    #[tokio::test]
    async fn test_catalog_storage_get_table_with_storage() {
        maybe_start_logging();
        let table_name = "test_table";

        // Set up catalog and create namespace, table, and partition
        let catalog = catalog();
        let mut repos = catalog.repositories();
        let test_namespace = arbitrary_namespace(&mut *repos, "test_namespace").await;
        let (test_table, test_partition) =
            setup_table_and_partition(&mut *repos, table_name, &test_namespace).await;

        // Create two files
        let file_1 =
            create_and_get_file(&mut *repos, &test_namespace, &test_table, &test_partition).await;
        let file_2 =
            create_and_get_file(&mut *repos, &test_namespace, &test_table, &test_partition).await;

        // Verify both files are in the same namespace and table
        assert_eq!(file_1.namespace_id, file_2.namespace_id);
        assert_eq!(file_1.table_id, file_2.table_id);

        // Get table with storage
        let server = CatalogStorageServer::new(Arc::clone(&catalog));
        let table_with_storage = server
            .get_table_with_storage(Request::new(proto::GetTableWithStorageRequest {
                table_id: file_1.table_id.get(),
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected file size is the sum of both files
        let expected = create_expected_table_with_storage(
            table_name,
            &file_1,
            file_1.file_size_bytes + file_2.file_size_bytes,
            None,
        );
        assert_eq!(
            table_with_storage,
            proto::GetTableWithStorageResponse {
                table_with_storage: Some(expected),
            }
        );

        // Get a non-existent table
        let non_existent_table = server
            .get_table_with_storage(Request::new(proto::GetTableWithStorageRequest {
                table_id: 100, // non-existent table ID
                deleted: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            non_existent_table,
            proto::GetTableWithStorageResponse {
                table_with_storage: None,
            }
        );
    }

    fn catalog() -> Arc<dyn Catalog> {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::new());
        Arc::new(MemCatalog::new(metrics, time_provider))
    }

    // Helper function to create an expected namespace
    fn create_expected_namespace_with_storage(
        namespace_name: &str,
        file: &data_types::ParquetFile,
        expected_size: i64,
        expected_table_count: i64,
        deleted_at: Option<Timestamp>,
    ) -> proto::NamespaceWithStorage {
        NamespaceWithStorage {
            id: file.namespace_id,
            name: namespace_name.to_string(),
            retention_period_ns: None,
            max_tables: MaxTables::const_default(),
            max_columns_per_table: MaxColumnsPerTable::const_default(),
            partition_template: NamespacePartitionTemplateOverride::default(),
            size_bytes: expected_size,
            table_count: expected_table_count,
            deleted_at,
        }
        .into()
    }

    // Helper function to create an expected table
    fn create_expected_table_with_storage(
        table_name: &str,
        file: &data_types::ParquetFile,
        expected_size: i64,
        deleted_at: Option<Timestamp>,
    ) -> proto::TableWithStorage {
        TableWithStorage {
            id: file.table_id,
            namespace_id: file.namespace_id,
            name: table_name.to_string(),
            partition_template: TablePartitionTemplateOverride::default(),
            size_bytes: expected_size,
            deleted_at,
            column_count: 0,
        }
        .into()
    }
}
