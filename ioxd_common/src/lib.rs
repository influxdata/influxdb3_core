// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod http;
pub mod rpc;
pub mod server_type;
mod service;

// These crates are used by the macros we export; provide a stable
// path to use them from in downstream crates.
pub mod reexport {
    pub use generated_types;
    pub use service_grpc_testing;
    pub use clap_blocks;
    pub use tokio;
    pub use tokio_stream;
    pub use tonic;
    pub use tonic_health;
    pub use tonic_reflection;
    pub use tower_http;
    pub use tower_trailer;
    pub use trace_http;
}

pub use service::Service;

use crate::server_type::{CommonServerState, ServerType};
use futures::{future::FusedFuture, pin_mut, FutureExt};
use hyper::server::conn::AddrIncoming;
use observability_deps::tracing::{error, info};
use snafu::{ResultExt, Snafu};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use clap_blocks::socket_addr::SocketAddrOrUDS;
use trace_http::ctx::TraceHeaderParser;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Neither grpc nor http listeners are available"))]
    MissingListener,

    #[snafu(display("Unable to bind to listen for HTTP requests on {}: {}", addr, source))]
    StartListeningHttp {
        addr: SocketAddr,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to bind to listen for gRPC requests on {}: {}", addr, source))]
    StartListeningGrpc {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Error serving HTTP: {}", source))]
    ServingHttp { source: hyper::Error },

    #[snafu(display("Error serving RPC: {}", source))]
    ServingRpc { source: server_type::RpcError },

    #[snafu(display("Early Http shutdown"))]
    LostHttp,

    #[snafu(display("Early RPC shutdown"))]
    LostRpc,

    #[snafu(display("Early server shutdown"))]
    LostServer,

    #[snafu(display("Cannot set set gRPC kernel-side TCP send buffer size: {source}"))]
    GrpcTcpKernelSendBufferSize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot set set gRPC kernel-side TCP receive buffer size: {source}"))]
    GrpcTcpKernelReceiveBufferSize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot set set HTTP kernel-side TCP send buffer size: {source}"))]
    HttpTcpKernelSendBufferSize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot set set HTTP kernel-side TCP receive buffer size: {source}"))]
    HttpTcpKernelReceiveBufferSize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

#[cfg(windows)]
/// ctrl_c is the cross-platform way to intercept the equivalent of SIGINT
/// This method returns if this occurs
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// grpc_tcp_listener takes a socket address and returns a tcp listener on that
/// socket with the provided configuration.
pub async fn grpc_tcp_listener(
    addr: SocketAddr,
    tcp_kernel_send_buffer_size: Option<usize>,
    tcp_kernel_receive_buffer_size: Option<usize>,
) -> Result<tokio::net::TcpListener> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(StartListeningGrpcSnafu { addr })?;

    match listener.local_addr() {
        Ok(local_addr) => info!(%local_addr, "bound gRPC listener"),
        Err(_) => info!(%addr, "bound gRPC listener"),
    }

    if let Some(send_buffer_size) = tcp_kernel_send_buffer_size {
        set_tcp_kernel_send_buffer_size(&listener, send_buffer_size)
            .context(GrpcTcpKernelSendBufferSizeSnafu)?;
    }

    if let Some(receive_buffer_size) = tcp_kernel_receive_buffer_size {
        set_tcp_kernel_receive_buffer_size(&listener, receive_buffer_size)
            .context(GrpcTcpKernelReceiveBufferSizeSnafu)?;
    }

    Ok(listener)
}

pub async fn http_listener(
    addr: SocketAddr,
    tcp_kernel_send_buffer_size: Option<usize>,
    tcp_kernel_receive_buffer_size: Option<usize>,
) -> Result<AddrIncoming> {
    let listener =
        tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::StartListeningHttp {
                addr,
                source: Box::new(e),
            })?;

    if let Some(send_buffer_size) = tcp_kernel_send_buffer_size {
        set_tcp_kernel_send_buffer_size(&listener, send_buffer_size)
            .context(HttpTcpKernelSendBufferSizeSnafu)?;
    }

    if let Some(receive_buffer_size) = tcp_kernel_receive_buffer_size {
        set_tcp_kernel_receive_buffer_size(&listener, receive_buffer_size)
            .context(HttpTcpKernelReceiveBufferSizeSnafu)?;
    }

    let listener =
        AddrIncoming::from_listener(listener).map_err(|e| Error::StartListeningHttp {
            addr,
            source: Box::new(e),
        })?;
    info!(bind_addr=%listener.local_addr(), "bound HTTP listener");

    Ok(listener)
}

/// Set and check given socket option.
///
/// Calls [`setsockopt`] on the given listener and checks the result using [`getsocktopt`]. This check is required
/// because some options silently accept out-of-range values.
///
/// Also some options are converted by the kernel when set (e.g. they are multiplied). The `return_conversion` should
/// perform the same conversion so that the check can work properly.
///
///
/// [`getsocktopt`]: https://linux.die.net/man/3/getsockopt
/// [`setsockopt`]: https://linux.die.net/man/3/setsockopt
#[cfg(unix)]
fn set_and_check_sock_option<O, V, C>(
    listener: &tokio::net::TcpListener,
    opt: O,
    val: &V,
    return_conversion: C,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    O: nix::sys::socket::GetSockOpt<Val = V>
        + nix::sys::socket::SetSockOpt<Val = V>
        + std::fmt::Debug,
    V: std::cmp::PartialEq + std::fmt::Debug,
    C: for<'a> FnOnce(&'a V) -> V,
{
    nix::sys::socket::setsockopt(listener, opt, val)
        .map_err(|e| format!("Cannot set {opt:?} to {val:?}: {e}"))?;

    // setting an out-of-range value silently works
    let current = nix::sys::socket::getsockopt(listener, opt)
        .map_err(|e| format!("Cannot get {opt:?}: {e}"))?;

    // Some socket options are silently converted/multiplied by the OS during SET and the changed value is returned
    // during GET. So we need to make sure to check the converted value, not the original one.
    let expected = return_conversion(val);

    if current != expected {
        return Err(format!(
            "When setting {opt:?} to {val:?} the OS should have returned {expected:?} afterwards but we got {current:?}. Value is likely out of range"
        ).into());
    }

    Ok(())
}

#[cfg(unix)]
fn set_tcp_kernel_send_buffer_size(
    listener: &tokio::net::TcpListener,
    size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    set_and_check_sock_option(listener, nix::sys::socket::sockopt::SndBuf, &size, |n| {
        n * 2
    })
}

#[cfg(not(unix))]
fn set_tcp_kernel_send_buffer_size(
    _listener: &tokio::net::TcpListener,
    _size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Err(
        "Setting TCP kernel-side send buffer size is NOT supported on this operating system."
            .to_string()
            .into(),
    )
}

#[cfg(unix)]
fn set_tcp_kernel_receive_buffer_size(
    listener: &tokio::net::TcpListener,
    size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    set_and_check_sock_option(listener, nix::sys::socket::sockopt::RcvBuf, &size, |n| {
        n * 2
    })
}

#[cfg(not(unix))]
fn set_tcp_kernel_receive_buffer_size(
    _listener: &tokio::net::TcpListener,
    _size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Err(
        "Setting TCP kernel-side receive buffer size is NOT supported on this operating system."
            .to_string()
            .into(),
    )
}

/// Instantiates gRPC and HTTP listeners and returns a `Future` that completes when
/// the listeners have all exited or the `frontend_shutdown` token is called.
/// Instantiates gRPC or HTTP or both for the provided server type.
pub async fn serve(
    common_state: CommonServerState,
    frontend_shutdown: CancellationToken,
    grpc_address: Option<SocketAddrOrUDS>,
    http_listener: Option<AddrIncoming>,
    server_type: Arc<dyn ServerType>,
) -> Result<()> {
    if grpc_address.is_none() && http_listener.is_none() {
        return Err(Error::MissingListener);
    }

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            &common_state
                .run_config()
                .tracing_config()
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(
            &common_state
                .run_config()
                .tracing_config()
                .traces_jaeger_debug_name,
        );

    // Construct and start up gRPC server
    let captured_server_type = Arc::clone(&server_type);
    let captured_shutdown = frontend_shutdown.clone();
    let captured_trace_header_parser = trace_header_parser.clone();
    let grpc_server = async move {
        if let Some(grpc_listener) = grpc_address {
            info!(?captured_server_type, "gRPC server listening");
            rpc::serve(
                grpc_listener,
                captured_server_type,
                captured_trace_header_parser,
                captured_shutdown,
            )
            .await?
        } else {
            // don't resolve otherwise will cause server to shutdown
            captured_shutdown.cancelled().await
        }
        Ok(())
    }
    .fuse();

    let captured_server_type = Arc::clone(&server_type);
    let captured_shutdown = frontend_shutdown.clone();
    let http_server = async move {
        if let Some(http_listener) = http_listener {
            info!(server_type=?captured_server_type, "HTTP server listening");
            http::serve(
                http_listener,
                captured_server_type,
                captured_shutdown,
                trace_header_parser,
                common_state.run_config().http_max_pending_reset_streams,
            )
            .await?
        } else {
            // don't resolve otherwise will cause server to shutdown
            captured_shutdown.cancelled().await
        }
        Ok(())
    }
    .fuse();

    // Purposefully use log not tokio-tracing to ensure correctly hooked up
    log::info!("InfluxDB IOx {:?} server ready", server_type);

    // Get IOx background worker join handle
    let server_handle = Arc::clone(&server_type).join().fuse();

    // Shutdown signal
    let signal = wait_for_signal().fuse();

    // There are two different select macros - tokio::select and futures::select
    //
    // tokio::select takes ownership of the passed future "moving" it into the
    // select block. This works well when not running select inside a loop, or
    // when using a future that can be dropped and recreated, often the case
    // with tokio's futures e.g. `channel.recv()`
    //
    // futures::select is more flexible as it doesn't take ownership of the provided
    // future. However, to safely provide this it imposes some additional
    // requirements
    //
    // All passed futures must implement FusedFuture - it is IB to poll a future
    // that has returned Poll::Ready(_). A FusedFuture has an is_terminated()
    // method that indicates if it is safe to poll - e.g. false if it has
    // returned Poll::Ready(_). futures::select uses this to implement its
    // functionality. futures::FutureExt adds a fuse() method that
    // wraps an arbitrary future and makes it a FusedFuture
    //
    // The additional requirement of futures::select is that if the future passed
    // outlives the select block, it must be Unpin or already Pinned

    // pin_mut constructs a Pin<&mut T> from a T by preventing moving the T
    // from the current stack frame and constructing a Pin<&mut T> to it
    pin_mut!(signal);
    pin_mut!(server_handle);
    pin_mut!(grpc_server);
    pin_mut!(http_server);

    // Return the first error encountered
    let mut res = Ok(());

    // Graceful shutdown can be triggered by sending SIGINT or SIGTERM to the
    // process, or by a background task exiting - most likely with an error
    //
    // Graceful shutdown should then proceed in the following order
    // 1. Stop accepting new HTTP and gRPC requests and drain existing connections
    // 2. Trigger shutdown of internal background workers loops
    //
    // This is important to ensure background tasks, such as polling the tracker
    // registry, don't exit before HTTP and gRPC requests dependent on them
    while !grpc_server.is_terminated() || !http_server.is_terminated() {
        futures::select! {
            _ = signal => info!(?server_type, "shutdown requested"),
            _ = server_handle => {
                // If the frontend & backend stop together, the select! may
                // choose to follow the "background has shutdown" signal instead
                // of one of the frontend paths.
                //
                // This should not be a problem so long as the frontend has
                // stopped.
                if frontend_shutdown.is_cancelled() {
                    break;
                }
                error!(?server_type, "server worker shutdown before frontend");
                res = res.and(Err(Error::LostServer));
            },
            result = grpc_server => match result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!(?server_type, "gRPC server shutdown"),
                Ok(_) => {
                    error!(?server_type, "early gRPC server exit");
                    res = res.and(Err(Error::LostRpc));
                }
                Err(error) => {
                    error!(%error, ?server_type, "gRPC server error");
                    res = res.and(Err(Error::ServingRpc{source: error}));
                }
            },
            result = http_server => match result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!(?server_type, "HTTP server shutdown"),
                Ok(_) => {
                    error!(?server_type, "early HTTP server exit");
                    res = res.and(Err(Error::LostHttp));
                }
                Err(error) => {
                    error!(%error, ?server_type, "HTTP server error");
                    res = res.and(Err(Error::ServingHttp{source: error}));
                }
            },
        }

        // Delegate shutting down the frontend to the background shutdown
        // handler, allowing it to sequence the stopping of the RPC/HTTP
        // servers as needed.
        server_type.shutdown(frontend_shutdown.clone())
    }
    info!(?server_type, "frontend shutdown completed");

    if !server_handle.is_terminated() {
        server_handle.await;
    }
    info!(?server_type, "backend shutdown completed");

    res
}

#[cfg(test)]
mod tests {
    // The only test in this module currently only applies to linux; silence warnings on other
    // platforms but leave this import here in case more tests are added in the future.
    #[allow(unused_imports)]
    use super::*;

    /// Operatoring system have an allowed config range, but for Linux we at least know the lower bound of this range
    /// and can use that for testing.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_set_tcp_kernel_buffer_size() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let err = set_tcp_kernel_send_buffer_size(&listener, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "When setting SndBuf to 1 the OS should have returned 2 afterwards but we got 4608. Value is likely out of range"
        );

        set_tcp_kernel_send_buffer_size(&listener, 2304).unwrap();

        let err = set_tcp_kernel_receive_buffer_size(&listener, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "When setting RcvBuf to 1 the OS should have returned 2 afterwards but we got 2304. Value is likely out of range"
        );

        set_tcp_kernel_receive_buffer_size(&listener, 1152).unwrap();
    }
}
