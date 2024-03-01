use bytes::{Buf, BufMut, BytesMut};
use http::{uri::Authority, HeaderMap, Method, Uri};
use hyper::Body;

use crate::ServerError;

use super::WriteHintRequestBody;

type Location = String;

#[derive(Debug)]
pub enum Request {
    /// Internal-only request used during pre-warming, for `PATCH /warmed`
    Warmed,
    /// For `GET /state`
    GetState,
    /// For `GET /keyspace`
    GetKeyspace,
    /// For `GET /metadata?location=<location>`
    GetMetadata(Location, HeaderMap),
    /// For `GET /object?location=<location>`
    GetObject(Location, HeaderMap),
    /// For `POST /write-hint`
    WriteHint(WriteHintRequestBody),
}

impl Request {
    /// Parse a [`Request`] from a [`http::Request`].
    pub async fn parse(req: http::Request<Body>) -> Result<Self, ServerError> {
        match (req.method(), req.uri().path()) {
            (&Method::PATCH, "/warmed") => Err(ServerError::BadRequest(
                "`PATCH /warmed` is an internal-only server request".into(),
            )),
            (&Method::GET, "/state") => Ok(Self::GetState),
            (&Method::GET, "/keyspace") => Ok(Self::GetKeyspace),
            (&Method::GET, "/metadata") => Ok(Self::GetMetadata(
                parse_object_location(req.uri())?,
                req.headers().clone(),
            )),
            (&Method::GET, "/object") => Ok(Self::GetObject(
                parse_object_location(req.uri())?,
                req.headers().clone(),
            )),
            (&Method::POST, "/write-hint") => Ok(Self::WriteHint(
                parse_write_hint_body(req.into_body()).await?,
            )),
            (any_method, any_path) => Err(ServerError::BadRequest(format!(
                "invalid path: {} {}",
                any_method, any_path
            ))),
        }
    }

    /// Convert into a [`http::Request`] with the provided authority.
    pub fn into_request_with_authority(
        self,
        authority: Option<Authority>,
    ) -> Result<http::Request<Body>, http::Error> {
        // reduce unnecessary (within cluster) overhead from https
        let authority = authority
            .map(|a| format!("http://{}", a.as_str()))
            .unwrap_or_default();

        match self {
            Self::Warmed => http::Request::builder()
                .method(Method::PATCH)
                .uri(format!("{}/warmed", authority))
                .body(Body::empty()),

            Self::GetState => http::Request::builder()
                .method(Method::GET)
                .uri(format!("{}/state", authority))
                .uri("/state")
                .body(Body::empty()),

            Self::GetKeyspace => http::Request::builder()
                .method(Method::GET)
                .uri(format!("{}/keyspace", authority))
                .body(Body::empty()),

            Self::GetMetadata(location, headers) => {
                let mut req: http::request::Builder = http::Request::builder()
                    .method(Method::GET)
                    .uri(format!("{}/metadata?location={}", authority, location));

                for (k, v) in &headers {
                    req = req.header(k, v);
                }

                req.body(Body::empty())
            }
            Self::GetObject(location, headers) => {
                let mut req: http::request::Builder = http::Request::builder()
                    .method(Method::GET)
                    .uri(format!("{}/object?location={}", authority, location));

                for (k, v) in &headers {
                    req = req.header(k, v);
                }

                req.body(Body::empty())
            }
            Self::WriteHint(write_hint) => {
                let mut buf = BytesMut::new().writer();
                serde_json::to_writer(&mut buf, &write_hint).expect("should write request body");

                http::Request::builder()
                    .method(Method::POST)
                    .uri(format!("{}/write-hint", authority))
                    .body(hyper::Body::from(buf.into_inner().freeze()))
            }
        }
    }
}

fn parse_object_location(uri: &Uri) -> Result<Location, ServerError> {
    if let Some((_, location)) = uri
        .query()
        .and_then(|v| url::form_urlencoded::parse(v.as_bytes()).find(|(k, _v)| k.eq("location")))
    {
        return Ok(location.to_string());
    }

    Err(ServerError::BadRequest(
        "missing required query parameter: location".into(),
    ))
}

async fn parse_write_hint_body(body: Body) -> Result<WriteHintRequestBody, ServerError> {
    let reader = hyper::body::aggregate(body)
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?
        .reader();
    let write_hint =
        serde_json::from_reader(reader).map_err(|e| ServerError::BadRequest(e.to_string()))?;

    Ok(write_hint)
}

impl From<Request> for http::Request<Body> {
    fn from(req: Request) -> Self {
        req.into_request_with_authority(None)
            .expect("should always be able to create an http::Request from a Request")
    }
}
