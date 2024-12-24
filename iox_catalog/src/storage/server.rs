//! gRPC server implementation of `CatalogStorageService`

use std::sync::Arc;

use crate::interface::{Catalog, RepoCollection};
use crate::storage::serialization::{
    serialize_namespace_with_storage, serialize_table_with_storage,
};
use crate::util_serialization::catalog_error_to_status;

use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use generated_types::influxdata::iox::catalog_storage::v1 as proto;
use tonic::{Request, Response, Status};

use super::serialization::{deserialize_namespace_sort_field, deserialize_sort_direction};
use super::sorting::sort_namespaces;

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

        let mut namespace_list = repos
            .namespaces()
            .list_storage()
            .await
            .map_err(catalog_error_to_status)?;

        let sort_field = deserialize_namespace_sort_field(req.sort_field)?;
        let sort_direction = deserialize_sort_direction(req.sort_direction)?;
        sort_namespaces(&mut namespace_list, sort_field, sort_direction);

        Ok(Response::new(proto::GetNamespacesWithStorageResponse {
            namespace_with_storage: namespace_list
                .into_iter()
                .map(serialize_namespace_with_storage)
                .collect(),
        }))
    }

    async fn get_namespace_with_storage(
        &self,
        request: Request<proto::GetNamespaceWithStorageRequest>,
    ) -> Result<Response<proto::GetNamespaceWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_namespace_with_storage = repos
            .namespaces()
            .get_storage_by_id(NamespaceId::new(req.id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetNamespaceWithStorageResponse {
            namespace_with_storage: maybe_namespace_with_storage
                .map(serialize_namespace_with_storage),
        }))
    }

    async fn get_tables_with_storage(
        &self,
        request: Request<proto::GetTablesWithStorageRequest>,
    ) -> Result<Response<proto::GetTablesWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let table_list = repos
            .tables()
            .list_storage_by_namespace_id(NamespaceId::new(req.namespace_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetTablesWithStorageResponse {
            table_with_storage: table_list
                .into_iter()
                .map(serialize_table_with_storage)
                .collect(),
        }))
    }

    async fn get_table_with_storage(
        &self,
        request: Request<proto::GetTableWithStorageRequest>,
    ) -> Result<Response<proto::GetTableWithStorageResponse>, Status> {
        let (mut repos, req) = self.preprocess_request(request);

        let maybe_table_with_storage = repos
            .tables()
            .get_storage_by_id(TableId::new(req.table_id))
            .await
            .map_err(catalog_error_to_status)?;

        Ok(Response::new(proto::GetTableWithStorageResponse {
            table_with_storage: maybe_table_with_storage.map(serialize_table_with_storage),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride,
    };
    use data_types::{MaxColumnsPerTable, MaxTables, NamespaceWithStorage, TableWithStorage};
    use generated_types::influxdata::iox::catalog_storage::v1::{
        self as proto, catalog_storage_service_server::CatalogStorageService,
    };
    use iox_time::SystemProvider;
    use test_helpers::maybe_start_logging;
    use tonic::Request;

    use crate::storage::{
        serialization::{
            serialize_namespace_sort_field, serialize_namespace_with_storage,
            serialize_sort_direction, serialize_table_with_storage,
        },
        server::CatalogStorageServer,
        sorting::{NamespaceSortField, SortDirection},
    };
    use crate::test_helpers::{
        arbitrary_namespace, create_and_get_file, delete_file, setup_table_and_partition,
    };
    use crate::{interface::Catalog, mem::MemCatalog};

    #[tokio::test]
    async fn test_catalog_storage_get_namespaces_with_storage() {
        maybe_start_logging();
        let namespace_name_1 = "namespace_name_1";
        let namespace_name_2 = "namespace_name_2";

        // Set up catalog, server, and create a namespace
        let catalog = catalog();
        let server = CatalogStorageServer::new(Arc::clone(&catalog));
        let mut repos = catalog.repositories();
        let namespace_1 = arbitrary_namespace(&mut *repos, namespace_name_1).await;

        // Create two tables, thier partitions, and files in namespace_1
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
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Id),
                sort_direction: serialize_sort_direction(SortDirection::Asc),
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected one namespace
        assert_eq!(response.namespace_with_storage.len(), 1);

        // Expected file size and table count is the sum of all tables
        let expected_namespace_1 = create_expected_namespace_with_storage(
            namespace_name_1,
            &ns_1_file_1,
            ns_1_file_1.file_size_bytes + ns_1_file_2.file_size_bytes,
            2, // expect two tables
        );
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespace_with_storage: vec![expected_namespace_1]
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
        );
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Id),
                sort_direction: serialize_sort_direction(SortDirection::Asc),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespace_with_storage: vec![expected_namespace_1.clone()]
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
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Id),
                sort_direction: serialize_sort_direction(SortDirection::Asc),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.namespace_with_storage.len(), 2);

        // Delete a file in namespace_2
        delete_file(&mut *repos, &ns_2_file).await;

        // Expected two namespaces and namespace_2 has size 0
        let expected_namespace_2 = create_expected_namespace_with_storage(
            namespace_name_2,
            &ns_2_file,
            0, // expect no size
            1, // expect one table even though no files are in this tables
        );
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Storage),
                sort_direction: serialize_sort_direction(SortDirection::Asc),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                // namespace_2 should be first because it has size 0
                namespace_with_storage: vec![
                    expected_namespace_2.clone(),
                    expected_namespace_1.clone()
                ]
            }
        );

        // Sort by name desc
        // Expected namespace_1 to be last because it has the "greater" name alphabetically
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Name),
                sort_direction: serialize_sort_direction(SortDirection::Desc),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespace_with_storage: vec![
                    expected_namespace_2.clone(),
                    expected_namespace_1.clone()
                ]
            }
        );

        // Sort by name asc
        // Expected namespace_1 to be first
        let response = server
            .get_namespaces_with_storage(Request::new(proto::GetNamespacesWithStorageRequest {
                sort_field: serialize_namespace_sort_field(NamespaceSortField::Name),
                sort_direction: serialize_sort_direction(SortDirection::Asc),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetNamespacesWithStorageResponse {
                namespace_with_storage: vec![
                    expected_namespace_1.clone(),
                    expected_namespace_2.clone()
                ]
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
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected one table
        assert_eq!(response.table_with_storage.len(), 1);

        // Expected file size is the sum of all files
        let expected_table_1 = create_expected_table_with_storage(
            table_name_1,
            &table_1_file_1,
            table_1_file_1.file_size_bytes + table_1_file_2.file_size_bytes,
        );
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                table_with_storage: vec![expected_table_1]
            }
        );

        // Delete a file
        delete_file(&mut *repos, &table_1_file_1).await;

        // Expected file size is the size of the remaining file
        let expected_table_1 = create_expected_table_with_storage(
            table_name_1,
            &table_1_file_1,
            table_1_file_2.file_size_bytes,
        );
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_1_file_1.namespace_id.get(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                table_with_storage: vec![expected_table_1.clone()]
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
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected two tables
        assert_eq!(response.table_with_storage.len(), 2);

        // Delete a file in table2
        delete_file(&mut *repos, &table_2_file_1).await;

        // Expected two tables and table2 has size 0
        let expected_table_2 = create_expected_table_with_storage(table_name_2, &table_2_file_1, 0);
        let response = server
            .get_tables_with_storage(Request::new(proto::GetTablesWithStorageRequest {
                namespace_id: table_2_file_1.namespace_id.get(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response,
            proto::GetTablesWithStorageResponse {
                table_with_storage: vec![expected_table_1, expected_table_2]
            }
        );
    }

    #[tokio::test]
    async fn test_catalog_storage_get_table_storage_by_id() {
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
                namespace_id: file_1.namespace_id.get(),
                table_id: file_1.table_id.get(),
            }))
            .await
            .unwrap()
            .into_inner();

        // Expected file size is the sum of both files
        let expected = create_expected_table_with_storage(
            table_name,
            &file_1,
            file_1.file_size_bytes + file_2.file_size_bytes,
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
                namespace_id: file_1.namespace_id.get(),
                table_id: 100, // non-existent table ID
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
    ) -> proto::NamespaceWithStorage {
        serialize_namespace_with_storage(NamespaceWithStorage {
            id: file.namespace_id,
            name: namespace_name.to_string(),
            retention_period_ns: None,
            max_tables: MaxTables::const_default(),
            max_columns_per_table: MaxColumnsPerTable::const_default(),
            partition_template: NamespacePartitionTemplateOverride::default(),
            size_bytes: expected_size,
            table_count: expected_table_count,
        })
    }

    // Helper function to create an expected table
    fn create_expected_table_with_storage(
        table_name: &str,
        file: &data_types::ParquetFile,
        expected_size: i64,
    ) -> proto::TableWithStorage {
        serialize_table_with_storage(TableWithStorage {
            id: file.table_id,
            namespace_id: file.namespace_id,
            name: table_name.to_string(),
            partition_template: TablePartitionTemplateOverride::default(),
            size_bytes: expected_size,
        })
    }
}
