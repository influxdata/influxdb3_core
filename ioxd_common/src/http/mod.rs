use http::StatusCode;
use std::{convert::Infallible, sync::Arc};

use authz::http::AuthorizationHeaderExtension;
use hyper::{
    server::conn::{AddrIncoming, AddrStream},
    Body, Method, Request, Response,
};
use observability_deps::tracing::{debug, error};
use snafu::Snafu;
use tokio_util::sync::CancellationToken;
use tower::Layer;
use trace_http::{ctx::TraceHeaderParser, tower::TraceLayer};

use crate::{
    http::error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
    server_type::ServerType,
};

pub mod error;
pub mod metrics;
pub mod utils;

pub mod test_utils;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ApplicationError {
    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("PProf error: {}", source))]
    PProf {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Protobuf error: {}", source))]
    Prost {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Protobuf error: {}", source))]
    ProstIO { source: std::io::Error },

    #[snafu(display("Route error from run mode: {}", e))]
    RunModeRouteError { e: Box<dyn HttpApiErrorSource> },
}

impl HttpApiErrorSource for ApplicationError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::InvalidQueryString { .. } => e.invalid(),
            e @ Self::PProf { .. } => e.internal_error(),
            e @ Self::Prost { .. } => e.internal_error(),
            e @ Self::ProstIO { .. } => e.internal_error(),
            Self::RunModeRouteError { e } => e.to_http_api_error(),
        }
    }
}

pub async fn serve(
    addr: AddrIncoming,
    server_type: Arc<dyn ServerType>,
    shutdown: CancellationToken,
    trace_header_parser: TraceHeaderParser,
    max_pending_reset_streams: Option<usize>,
) -> Result<(), hyper::Error> {
    let trace_collector = server_type.trace_collector();
    let trace_layer = TraceLayer::new(
        trace_header_parser,
        Arc::new(server_type.http_request_metrics()),
        trace_collector,
        server_type.name(),
    );

    let mut builder = hyper::Server::builder(addr)
        .tcp_nodelay(true)
        .http2_adaptive_window(true);
    if let Some(x) = max_pending_reset_streams {
        builder = builder.http2_max_pending_accept_reset_streams(Some(x));
    }

    builder
        .serve(hyper::service::make_service_fn(|_conn: &AddrStream| {
            let server_type = Arc::clone(&server_type);
            let service = hyper::service::service_fn(move |request: Request<_>| {
                route_request(Arc::clone(&server_type), request)
            });

            let service = trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        }))
        .with_graceful_shutdown(shutdown.cancelled())
        .await
}

async fn route_request(
    server_type: Arc<dyn ServerType>,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let auth = { req.headers().get(hyper::header::AUTHORIZATION).cloned() };
    req.extensions_mut()
        .insert(AuthorizationHeaderExtension::new(auth));

    // we don't need the authorization header anymore and we don't want to accidentally log it.
    req.headers_mut().remove(hyper::header::AUTHORIZATION);
    debug!(request = ?req,"Processing request");

    let method = req.method().clone();
    let uri = req.uri().clone();
    let content_length = req.headers().get("content-length").cloned();

    let response = match (method.clone(), uri.path()) {
        (Method::GET, "/health") => Ok(check_response(server_type.is_healthy(), "OK")),
        (Method::GET, "/ready") => Ok(check_response(server_type.is_ready(), "READY")),
        (Method::GET, "/metrics") => handle_metrics(server_type.as_ref()),
        _ => server_type
            .route_http_request(req)
            .await
            .map_err(|e| ApplicationError::RunModeRouteError { e }),
    };

    // TODO: Move logging to TraceLayer
    match response {
        Ok(response) => {
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            let error: HttpApiError = error.to_http_api_error();
            if error.is_internal() {
                error!(%error, %method, %uri, ?content_length, "Error while handling request");
            } else {
                debug!(%error, %method, %uri, ?content_length, "Error while handling request");
            }
            Ok(error.response())
        }
    }
}

/// Returns HTTP 200 with a body of `response_body` when `is_ok` is true, and a
/// 5xx with no body otherwise.
fn check_response(is_ok: bool, response_body: &'static str) -> Response<Body> {
    match is_ok {
        true => Response::new(Body::from(response_body)),
        false => {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            resp
        }
    }
}

fn handle_metrics(server_type: &dyn ServerType) -> Result<Response<Body>, ApplicationError> {
    let mut body: Vec<u8> = Default::default();
    let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
    server_type.metric_registry().report(&mut reporter);

    Ok(Response::new(Body::from(body)))
}
