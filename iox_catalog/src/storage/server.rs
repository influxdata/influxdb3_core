//! gRPC server implementation of `CatalogStorageService`

use std::sync::Arc;

use crate::interface::{Catalog, Error};
use crate::util::catalog_error_to_status;

use async_trait::async_trait;
use generated_types::influxdata::iox::catalog_storage::v1 as proto;
use tonic::{Request, Response, Status};

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
}

#[async_trait]
impl proto::catalog_storage_service_server::CatalogStorageService for CatalogStorageServer {
    async fn get_namespaces_with_storage(
        &self,
        _request: Request<proto::GetNamespacesWithStorageRequest>,
    ) -> Result<Response<proto::GetNamespacesWithStorageResponse>, Status> {
        Err(catalog_error_to_status(Error::NotImplemented {
            descr: "get_namespaces_with_storage".to_owned(),
        }))
    }

    async fn get_namespace_with_storage(
        &self,
        _request: Request<proto::GetNamespaceWithStorageRequest>,
    ) -> Result<Response<proto::GetNamespaceWithStorageResponse>, Status> {
        Err(catalog_error_to_status(Error::NotImplemented {
            descr: "get_namespace_with_storage".to_owned(),
        }))
    }

    async fn get_tables_with_storage(
        &self,
        _request: Request<proto::GetTablesWithStorageRequest>,
    ) -> Result<Response<proto::GetTablesWithStorageResponse>, Status> {
        Err(catalog_error_to_status(Error::NotImplemented {
            descr: "get_tables_with_storage".to_owned(),
        }))
    }

    async fn get_table_with_storage(
        &self,
        _request: Request<proto::GetTableWithStorageRequest>,
    ) -> Result<Response<proto::GetTableWithStorageResponse>, Status> {
        Err(catalog_error_to_status(Error::NotImplemented {
            descr: "get_table_with_storage".to_owned(),
        }))
    }
}
