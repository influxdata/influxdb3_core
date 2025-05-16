//! Defines type aliases and helper functions involving `hyper`, `http`, `http_body`, and
//! `http_body_utils` for use in any other crate. Goals:
//!
//! - Reduce duplication
//! - Make upgrades of these http-related crates easier by having one place where definitions may
//!   need to be updated
//! - Make it easier to pass http-related types between crates
//!
//! This crate is lower-level than `iox_http`; this crate is meant to be more general-purpose and
//! `iox_http` is meant for services providing HTTP APIs.

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use futures::{Stream, StreamExt, TryStreamExt};

/// The type of all request bodies.
pub type RequestBody = hyper::Body;

/// The type of all requests.
pub type Request = hyper::Request<RequestBody>;

/// Builder for requests. Mostly useful in tests.
pub type RequestBuilder = http::request::Builder;

/// Empty request body, properly typed.
///
/// Mostly useful when constructing test requests.
pub fn empty_request_body() -> RequestBody {
    RequestBody::empty()
}

/// Convert something that can be converted into a [`hyper::body::Bytes`] into a [`RequestBody`]
/// that sends one chunk of bytes in full.
///
/// Mostly useful when constructing test requests.
pub fn bytes_to_request_body(bytes: impl Into<hyper::body::Bytes>) -> RequestBody {
    RequestBody::from(bytes.into())
}

/// The type of all response bodies.
pub type ResponseBody = hyper::Body;

/// The type of all responses.
pub type Response = hyper::Response<ResponseBody>;

/// Builder for responses.
pub type ResponseBuilder = http::response::Builder;

/// Empty response body when there's no content to return.
pub fn empty_response_body() -> ResponseBody {
    ResponseBody::empty()
}

/// Responding with one chunk of bytes. For streaming, see [`stream_bytes_to_response_body`].
pub fn bytes_to_response_body(bytes: impl Into<hyper::body::Bytes>) -> ResponseBody {
    ResponseBody::from(bytes.into())
}

/// Responding with a stream of bytes, wrapping each frame in `Ok`.
///
/// If you have a stream of `Result`s of `Bytes`, see [`stream_results_to_response_body`].
///
/// If you don't want to stream, see [`bytes_to_response_body`].
pub fn stream_bytes_to_response_body<S, B>(stream: S) -> ResponseBody
where
    S: Stream<Item = B> + Send + 'static,
    B: Into<hyper::body::Bytes> + 'static,
{
    let stream = stream.map(Ok::<B, std::convert::Infallible>);
    stream_results_to_response_body(stream)
}

/// Responding with a stream of bytes, wrapping each frame in `Ok`.
///
/// If you have a stream of `Result`s of `Bytes`, see [`stream_results_to_response_body`].
///
/// If you don't want to stream, see [`bytes_to_response_body`].
pub fn stream_results_to_response_body<S, B, E>(stream: S) -> ResponseBody
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: Into<hyper::body::Bytes> + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let stream = stream.map_ok(Into::into);
    hyper::Body::wrap_stream(stream)
}

/// FOR TESTS ONLY: Read the full response as bytes.
///
/// # Panics
///
/// Panics if reading any frame fails! Non-test code should be processing the stream and
/// propagating errors correctly!
pub async fn read_body_bytes_for_tests(mut body: ResponseBody) -> hyper::body::Bytes {
    use hyper::body::{Buf, HttpBody};
    let mut bufs = vec![];
    while let Some(buf) = body.data().await {
        let buf = buf.expect("failed to read response body");
        if buf.has_remaining() {
            bufs.extend(buf.to_vec());
        }
    }

    bufs.into()
}
