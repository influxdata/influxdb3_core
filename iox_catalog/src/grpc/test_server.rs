//! Test gRPC server for the catalog.

use generated_types::transport::{Server, Uri, server::TcpIncoming};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpListener, task::JoinSet};

use crate::interface::Catalog;

use super::server::GrpcCatalogServer;

/// A test gRPC server that serves the catalog.
/// This is useful for testing the gRPC client.
#[derive(Debug)]
pub struct TestGrpcServer {
    addr: SocketAddr,
    #[expect(dead_code)]
    task: JoinSet<()>,
}

impl TestGrpcServer {
    /// Create a new test gRPC server backed by the provided catalog.
    pub async fn new(catalog: Arc<dyn Catalog>) -> Self {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        let mut task = tokio::task::JoinSet::new();
        task.spawn(async move {
            Server::builder()
                .add_service(GrpcCatalogServer::new(catalog).service())
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        Self { addr, task }
    }

    /// Get the URI of the server.
    pub fn uri(&self) -> Uri {
        format!("http://{}:{}", self.addr.ip(), self.addr.port())
            .parse()
            .unwrap()
    }
}
