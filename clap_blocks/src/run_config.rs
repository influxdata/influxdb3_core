//! Common config for all `run` commands.
use std::path::PathBuf;

use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

use crate::socket_addr::SocketAddr;

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// Common config for all `run` commands.
#[derive(Debug, Clone, clap::Parser)]
pub struct RunConfig {
    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// The address on which IOx will serve HTTP API requests.
    #[clap(
        long = "api-bind",
        env = "INFLUXDB_IOX_BIND_ADDR",
        default_value = DEFAULT_API_BIND_ADDR,
        action,
    )]
    pub http_bind_address: SocketAddr,

    /// Maximum number of pending RST_STREAM before a GOAWAY will be sent
    /// by the service bound to `http_bind_address`
    ///
    /// This is important for services that produce a large number of stream
    /// resets as part of normal operation, e.g. quorum catalog reads
    #[clap(long, env = "INFLUXDB_IOX_MAX_PENDING_RESET_STREAMS")]
    pub http_max_pending_reset_streams: Option<usize>,

    /// The address on which IOx will serve Storage gRPC API requests.
    #[clap(
        long = "grpc-bind",
        env = "INFLUXDB_IOX_GRPC_BIND_ADDR",
        default_value = DEFAULT_GRPC_BIND_ADDR,
        action,
    )]
    pub grpc_bind_address: SocketAddr,

    /// Kernel-side TCP send buffer size for the gRPC server, in bytes.
    ///
    /// Will cause the server to fail to start if the value is out of range or if that setting is not supported on this
    /// operating system.
    #[clap(
        long = "grpc-server-tcp-kernel-send-buffer-size",
        env = "INFLUXDB_IOX_GRPC_SERVER_TCP_KERNEL_SEND_BUFFER_SIZE"
    )]
    pub grpc_server_tcp_kernel_send_buffer_size: Option<usize>,

    /// Kernel-side TCP receive buffer size for the gRPC server, in bytes.
    ///
    /// Will cause the server to fail to start if the value is out of range or if that setting is not supported on this
    /// operating system.
    #[clap(
        long = "grpc-server-tcp-kernel-receive-buffer-size",
        env = "INFLUXDB_IOX_GRPC_SERVER_TCP_KERNEL_RECEIVE_BUFFER_SIZE"
    )]
    pub grpc_server_tcp_kernel_receive_buffer_size: Option<usize>,

    /// Kernel-side TCP send buffer size for the HTTP server, in bytes.
    ///
    /// Will cause the server to fail to start if the value is out of range or if that setting is not supported on this
    /// operating system.
    #[clap(
        long = "http-server-tcp-kernel-send-buffer-size",
        env = "INFLUXDB_IOX_HTTP_SERVER_TCP_KERNEL_SEND_BUFFER_SIZE"
    )]
    pub http_server_tcp_kernel_send_buffer_size: Option<usize>,

    /// Kernel-side TCP receive buffer size for the HTTP server, in bytes.
    ///
    /// Will cause the server to fail to start if the value is out of range or if that setting is not supported on this
    /// operating system.
    #[clap(
        long = "http-server-tcp-kernel-receive-buffer-size",
        env = "INFLUXDB_IOX_HTTP_SERVER_TCP_KERNEL_RECEIVE_BUFFER_SIZE"
    )]
    pub http_server_tcp_kernel_receive_buffer_size: Option<usize>,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760", // 10 MiB
        action,
    )]
    pub max_http_request_size: usize,

    /// Path to file containing license JWT.
    #[clap(
        long = "license-path",
        env = "INFLUXDB_IOX_LICENSE_PATH",
        default_value = "/etc/influxdb3/license.jwt",
        action
    )]
    pub license_path: PathBuf,
}

impl RunConfig {
    /// Get a reference to the run config's tracing config.
    pub fn tracing_config(&self) -> &TracingConfig {
        &self.tracing_config
    }

    /// Get a mutable reference to the run config's tracing config.
    pub fn tracing_config_mut(&mut self) -> &mut TracingConfig {
        &mut self.tracing_config
    }

    /// Get a reference to the run config's logging config.
    pub fn logging_config(&self) -> &LoggingConfig {
        &self.logging_config
    }

    /// set the http bind address
    pub fn with_http_bind_address(mut self, http_bind_address: SocketAddr) -> Self {
        self.http_bind_address = http_bind_address;
        self
    }

    /// set the grpc bind address
    pub fn with_grpc_bind_address(mut self, grpc_bind_address: SocketAddr) -> Self {
        self.grpc_bind_address = grpc_bind_address;
        self
    }

    /// Create a new instance for all-in-one mode, only allowing some arguments.
    pub fn new(
        logging_config: LoggingConfig,
        tracing_config: TracingConfig,
        http_bind_address: SocketAddr,
        grpc_bind_address: SocketAddr,
        max_http_request_size: usize,
        license_path: PathBuf,
    ) -> Self {
        Self {
            logging_config,
            tracing_config,
            http_bind_address,
            grpc_bind_address,
            max_http_request_size,
            http_max_pending_reset_streams: None,
            grpc_server_tcp_kernel_send_buffer_size: None,
            grpc_server_tcp_kernel_receive_buffer_size: None,
            http_server_tcp_kernel_send_buffer_size: None,
            http_server_tcp_kernel_receive_buffer_size: None,
            license_path,
        }
    }
}
