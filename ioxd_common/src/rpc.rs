use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

pub use generated_types::FileDescriptorSet;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::{body::BoxBody, server::NamedService, Code};
use tonic_health::server::HealthReporter;
use trace_http::ctx::TraceHeaderParser;

use crate::server_type::{RpcError, ServerType};

/// Returns the name of the gRPC service S.
pub fn service_name<S: NamedService>(_: &S) -> &'static str {
    S::NAME
}

#[derive(Debug)]
pub struct RpcBuilderInput {
    pub socket: TcpListener,
    pub trace_header_parser: TraceHeaderParser,
    pub shutdown: CancellationToken,
}

#[derive(Debug)]
pub struct RpcBuilder<T> {
    pub inner: T,
    pub reflection_fd_set: FileDescriptorSet,
    pub expected_missing_fds: HashSet<&'static str>,
    pub health_reporter: HealthReporter,
    pub shutdown: CancellationToken,
    pub socket: TcpListener,
}

/// Adds a gRPC service to the builder, and registers it with the
/// health reporter
#[macro_export]
macro_rules! add_service {
    ($builder:ident, $svc:expr) => {
        $crate::add_service!($builder, $svc, Serving)
    };
    ($builder:ident, $svc:expr, $status:ident) => {
        let $builder = {
            // `inner` might be required to be `mut` or not depending if we're acting on:
            // - a `Server`, no service added yet, no `mut` required
            // - a `Router`, some service was added already, `mut` required
            #[allow(unused_mut)]
            {
                use $crate::rpc::{service_name, RpcBuilder};

                let RpcBuilder {
                    mut inner,
                    mut health_reporter,
                    mut reflection_fd_set,
                    expected_missing_fds,
                    shutdown,
                    socket,
                } = $builder;
                let service = $svc;

                let status = $crate::reexport::tonic_health::ServingStatus::$status;
                let service_name = service_name(&service);
                health_reporter
                    .set_service_status(service_name, status)
                    .await;

                let inner = inner.add_service(service);

                if let Some(proto) =
                    $crate::reexport::generated_types::FILE_DESCRIPTOR_MAP.get(service_name)
                {
                    reflection_fd_set.file.push(proto.clone());
                } else if !expected_missing_fds.contains(&service_name) {
                    // other than a few known FileDescriptorProtos that we don't expect to be
                    // available, we panic here to ensure that in the future when protobufs are
                    // changed or new services are added we make sure they are made properly
                    // available for reflection
                    panic!("missing from FileDescriptorProto map: {service_name}");
                }

                RpcBuilder {
                    inner,
                    reflection_fd_set,
                    expected_missing_fds,
                    health_reporter,
                    shutdown,
                    socket,
                }
            }
        };
    };
}

/// Creates a [`RpcBuilder`] from [`RpcBuilderInput`].
///
/// The resulting builder can be used w/ [`add_service`]. After adding all services it should
/// be used w/ [`serve_builder!`](crate::serve_builder).
#[macro_export]
macro_rules! setup_builder {
    ($input:ident, $server_type:ident) => {
        $crate::setup_builder_impl!($input, $server_type, |b| b)
    };
    ($input:ident, $server_type:ident, $server_opts:expr) => {
        $crate::setup_builder_impl!($input, $server_type, $server_opts)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! setup_builder_impl {
    ($input:ident, $server_type:ident, $server_opts:expr) => {{
        #[allow(unused_imports)]
        use std::collections::HashSet;
        #[allow(unused_imports)]
        use $crate::rpc::FileDescriptorSet;
        #[allow(unused_imports)]
        use $crate::{add_service, rpc::RpcBuilder, server_type::ServerType};

        let RpcBuilderInput {
            socket,
            trace_header_parser,
            shutdown,
        } = $input;

        let (health_reporter, health_service) =
            $crate::reexport::tonic_health::server::health_reporter();

        let builder = $crate::reexport::tonic::transport::Server::builder();

        // to force type inference on the macro expr
        fn handle_opts(
            b: $crate::reexport::tonic::transport::Server,
            f: impl FnOnce($crate::reexport::tonic::transport::Server) -> $crate::reexport::tonic::transport::Server,
        ) -> $crate::reexport::tonic::transport::Server {
            f(b)
        }

        let builder = handle_opts(builder, $server_opts);

        let builder = builder
            .layer($crate::reexport::trace_http::tower::TraceLayer::new(
                trace_header_parser,
                Arc::new($crate::reexport::trace_http::metrics::RequestMetrics::new(
                    $server_type.metric_registry(),
                    $crate::reexport::trace_http::metrics::MetricFamily::GrpcServer,
                )),
                $server_type.trace_collector(),
                $server_type.name(),
            ))
            .layer(
                $crate::reexport::tower_http::catch_panic::CatchPanicLayer::custom(
                    $crate::rpc::handle_panic,
                ),
            )
            .layer($crate::reexport::tower_trailer::TrailerLayer::default());

        // all services being registered should have a FileDescriptorProto available execept for
        // these
        let expected_missing_fds = HashSet::from([
            // the reflection service lists itself without the need to be explicitly registered and
            // it's not defined in this git repo anyway so we shouldn't expect it to be in
            // FILE_DESCRIPTOR_MAP
            "grpc.reflection.v1alpha.ServerReflection",
            // the FileDescriptorProto of this service is known to be unavailable since it lives in
            // a different git repo: https://github.com/influxdata/influxdb_iox/issues/4543
            // we can remove it from this set once that issue is addressed
            "arrow.flight.protocol.FlightService",
        ]);

        let builder = RpcBuilder {
            inner: builder,
            reflection_fd_set: FileDescriptorSet { file: Vec::new() },
            expected_missing_fds,
            health_reporter,
            shutdown,
            socket,
        };

        add_service!(builder, health_service);
        add_service!(
            builder,
            $crate::reexport::service_grpc_testing::make_server()
        );

        builder
    }};
}

/// Serve a server constructed using [`RpcBuilder`].
#[macro_export]
macro_rules! serve_builder {
    ($builder:ident) => {{
        use $crate::rpc::RpcBuilder;

        let reflection_service = $crate::reexport::tonic_reflection::server::Builder::configure()
            .register_file_descriptor_set($builder.reflection_fd_set.clone())
            .build()
            .expect("gRPC reflection data broken");
        $crate::add_service!($builder, reflection_service);

        let RpcBuilder {
            inner,
            shutdown,
            socket,
            ..
        } = $builder;

        let stream = $crate::reexport::tonic::transport::server::TcpIncoming::from_listener(
            socket, true, None,
        )
        .expect("failed to initialise tcp socket");
        inner
            .serve_with_incoming_shutdown(stream, shutdown.cancelled())
            .await?;
    }};
}

pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> http::Response<BoxBody> {
    let message = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "unknown internal error".to_string()
    };

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-status", Code::Internal as u32)
        .header("grpc-message", message) // we don't want to leak the panic message
        .body(tonic::body::empty_body())
        .unwrap()
}

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve(
    socket: TcpListener,
    server_type: Arc<dyn ServerType>,
    trace_header_parser: TraceHeaderParser,
    shutdown: CancellationToken,
) -> Result<(), RpcError> {
    let builder_input = RpcBuilderInput {
        socket,
        trace_header_parser,
        shutdown,
    };

    server_type.server_grpc(builder_input).await
}
