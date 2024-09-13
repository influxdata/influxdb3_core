use std::sync::Arc;

use clap_blocks::run_config::RunConfig;
use clap_blocks::socket_addr::SocketAddrOrUDS;

use crate::server_type::ServerType;

/// A service that will start on the specified addresses
#[derive(Debug)]
pub struct Service {
    pub http_bind_address: Option<SocketAddrOrUDS>,
    pub grpc_bind_address: Option<SocketAddrOrUDS>,
    pub server_type: Arc<dyn ServerType>,
}

impl Service {
    pub fn create(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: Some(run_config.http_bind_address.clone()),
            grpc_bind_address: Some(run_config.grpc_bind_address.clone()),
            server_type,
        }
    }

    pub fn create_grpc_only(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: None,
            grpc_bind_address: Some(run_config.grpc_bind_address.clone()),
            server_type,
        }
    }

    pub fn create_http_only(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: Some(run_config.http_bind_address.clone()),
            grpc_bind_address: None,
            server_type,
        }
    }
}
