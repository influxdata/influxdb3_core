use std::pin::Pin;

use futures::Future;
use http::uri::Authority;
use hyper::Body;

use crate::data_types::Request;

pub type PinnedFuture<R, E> = Pin<Box<dyn Future<Output = Result<R, E>> + Send>>;

#[derive(Debug)]
pub struct RawRequest {
    pub request: Request,
    pub authority: Option<Authority>,
}

impl TryFrom<RawRequest> for http::Request<Body> {
    type Error = http::Error;

    fn try_from(value: RawRequest) -> Result<Self, Self::Error> {
        let RawRequest { request, authority } = value;

        Request::into_request_with_authority(request, authority)
    }
}
