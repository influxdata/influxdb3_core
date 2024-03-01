use std::{cmp::Ordering, task::Poll};

use chrono::{DateTime, FixedOffset, Utc};
use http::{HeaderMap, HeaderName, HeaderValue};
use tower::{Layer, Service};

use crate::{
    data_types::{GetObjectMetaResponse, Request, X_HEAD_HEADER, X_VERSION_HEADER},
    server::response::Response,
};

use super::{error::Error, response::PinnedFuture};

#[allow(clippy::declare_interior_mutable_const)]
const X_VERSION_HEADERNAME: HeaderName = HeaderName::from_static(X_VERSION_HEADER);
#[allow(clippy::declare_interior_mutable_const)]
const X_HEAD_HEADERNAME: HeaderName = HeaderName::from_static(X_HEAD_HEADER);

/// PreconditionError is a local error type.
/// Tower service layers are expected to convert to [`Error`].
#[derive(Debug, thiserror::Error)]
pub enum PreconditionError {
    #[error("invalid precondition value: {0}")]
    InvalidPreconditionValue(String),
    #[error("If-Match failed: no match found for `{0}`")]
    NoMatchFound(String),
    #[error("If-None-Match failed: match found for `{0}`")]
    MatchFound(String),
    #[error("etag match was requested, but no etag exists for object")]
    NoEtagExists,
    #[error("If-Modified-Since failed: not modified since `{0}`")]
    NotModified(String),
    #[error("If-Unmodified-Since failed: modified since `{0}`")]
    Modified(String),
    #[error("does not match requested version")]
    Version,
    #[error("version match was requested, but no version exists for object")]
    NoVersionExists,
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Bad Request: object location does not exist in catalog or object store")]
    DoesNotExist,
}

/// Service that applies the preconditions per request.
///
/// Refer to GetOptions:
/// <https://github.com/apache/arrow-rs/blob/481652a4f8d972b633063158903dbdb0adcf094d/object_store/src/lib.rs#L871>
#[derive(Debug, Clone)]
pub struct PreconditionService<S: Clone + Send + Sync + 'static> {
    inner: S,
}

impl<S: Clone + Send + Sync + 'static> PreconditionService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Request> for PreconditionService<S>
where
    S: Service<Request, Future = PinnedFuture, Error = Error> + Clone + Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;
    type Future = PinnedFuture;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // preconditions only apply to /metadata & /object requests
        match req {
            Request::GetMetadata(ref location, ref headers)
            | Request::GetObject(ref location, ref headers) => {
                let clone = self.inner.clone();
                let mut this = std::mem::replace(&mut self.inner, clone);
                let location = location.clone();
                let headers = headers.clone();
                Box::pin(async move {
                    // (1) issue `/head` req to inner
                    match this
                        .call(Request::GetMetadata(location, headers.clone()))
                        .await
                    {
                        Ok(Response::Head(meta)) => {
                            // (2) check metadata vs preconditions
                            let return_only_head = passes_preconditions_check(headers, &meta)?;
                            // (3) return metadata request or pass original request to inner
                            if return_only_head {
                                Ok(Response::Head(meta))
                            } else {
                                this.call(req).await
                            }
                        }
                        Err(e) => Err(e),
                        _ => unreachable!("code error, did not issue proper head_req to inner"),
                    }
                })
            }
            _ => self.inner.call(req),
        }
    }
}

fn passes_preconditions_check(
    preconditions: HeaderMap,
    metadata: &GetObjectMetaResponse,
) -> Result<bool /* HEAD-only */, PreconditionError> {
    let mut head_req = false;

    let GetObjectMetaResponse {
        location: _,
        last_modified,
        size: _,
        e_tag,
        version,
    } = metadata;

    if let Some(value) = preconditions.get(&http::header::IF_MATCH) {
        let value_string = value
            .to_str()
            .map_err(|e| PreconditionError::InvalidPreconditionValue(e.to_string()))?
            .to_string();
        let patterns = try_extract_header_multiple_values(&value_string)?;

        if let Some(e_tag) = &e_tag {
            match patterns {
                PermittedValue::Any => {}
                PermittedValue::OneOf(one_of) => {
                    // e_tags are always strong matches
                    if !one_of.contains(&Match::Strong(e_tag.as_str())) {
                        return Err(PreconditionError::NoMatchFound(value_string));
                    }
                }
            }
        } else {
            return Err(PreconditionError::NoEtagExists);
        }
    };
    if let Some(value) = preconditions.get(&http::header::IF_NONE_MATCH) {
        let value_string = value
            .to_str()
            .map_err(|e| PreconditionError::InvalidPreconditionValue(e.to_string()))?
            .to_string();
        let patterns = try_extract_header_multiple_values(&value_string)?;

        if let Some(e_tag) = &e_tag {
            // If-None-Match header **does** permit weak matches.
            // However, we treat all etag matches with strong validation.
            match patterns {
                PermittedValue::Any => {
                    // For If-None-Match, wildcard * is conditional on
                    // a recipient cache not having any current representation of the target resource.
                    //
                    // See <https://datatracker.ietf.org/doc/html/rfc9110#field.if-none-match>
                    //
                    // Preconditions are only used by [`GetOptions`](object_store::GetOptions),
                    // which should return a 404 NotFound when missing. It does not make sense to
                    // over-ride this 404 based on a `If-None-Match: *` header. Ignore the wildcard.
                }
                PermittedValue::OneOf(one_of) => {
                    // e_tags are always strong matches
                    if one_of.contains(&Match::Strong(e_tag.as_str())) {
                        return Err(PreconditionError::MatchFound(value_string));
                    }
                }
            }
        } else {
            return Err(PreconditionError::NoEtagExists);
        }
    };
    if let Some(value) = preconditions.get(&http::header::IF_MODIFIED_SINCE) {
        // See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
        // This header **should** be ignored when If-None-Match exists.
        if !preconditions.contains_key(&http::header::IF_NONE_MATCH) {
            let value = try_extract_rfc2822_timestamp(value)?.with_timezone(&Utc);
            // rfc2822 timestamps do not have sub-second precision
            if last_modified.timestamp().cmp(&value.timestamp()) != Ordering::Greater {
                return Err(PreconditionError::NotModified(value.to_rfc2822()));
            }
        }
    };
    if let Some(value) = preconditions.get(&http::header::IF_UNMODIFIED_SINCE) {
        // See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
        // This header **should** be ignored when If-Match exists.
        if !preconditions.contains_key(&http::header::IF_MATCH) {
            let value = try_extract_rfc2822_timestamp(value)?;
            // rfc2822 timestamps do not have sub-second precision
            if last_modified.timestamp().cmp(&value.timestamp()) == Ordering::Greater {
                return Err(PreconditionError::Modified(value.to_rfc2822()));
            }
        }
    };
    #[allow(clippy::borrow_interior_mutable_const)]
    if let Some(value) = preconditions.get(&X_VERSION_HEADERNAME) {
        match &version {
            Some(v) if !v.eq(value) => return Err(PreconditionError::Version),
            None => return Err(PreconditionError::NoVersionExists),
            _ => {}
        }
    };
    #[allow(clippy::borrow_interior_mutable_const)]
    if preconditions.contains_key(&X_HEAD_HEADERNAME) {
        head_req = true; // client-side only adds this header if it wants a HEAD response
    };

    Ok(head_req)
}

/// Header validation level required.
/// See <https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.3.3>
enum Match<'a> {
    Weak(&'a str),
    Strong(&'a str),
}

impl PartialEq for Match<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Match::Strong(s), Match::Strong(o)) => s.eq(o),
            _ => unimplemented!("weak matches not implemented"),
        }
    }
}

impl<'a> TryFrom<&'a str> for Match<'a> {
    type Error = PreconditionError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        if s.starts_with("/W") {
            Err(PreconditionError::InvalidPreconditionValue(format!(
                "weak matches are not permitted: {}",
                s
            )))
        } else {
            Ok(Match::Strong(s))
        }
    }
}

enum PermittedValue<'a> {
    Any,
    OneOf(Vec<Match<'a>>),
}

/// Extracts header value to match against.
fn try_extract_header_multiple_values(
    value: &str,
) -> Result<PermittedValue<'_>, PreconditionError> {
    let list = value.split(' ').collect::<Vec<_>>();

    let mut matches = Vec::with_capacity(list.len());
    for to_match in list.into_iter() {
        if to_match == "*" {
            return Ok(PermittedValue::Any);
        }
        matches.push(Match::try_from(to_match)?);
    }
    Ok(PermittedValue::OneOf(matches))
}

fn try_extract_rfc2822_timestamp(
    value: &HeaderValue,
) -> Result<DateTime<FixedOffset>, PreconditionError> {
    value
        .to_str()
        .map(DateTime::parse_from_rfc2822)
        .map_err(|_| {
            PreconditionError::InvalidPreconditionValue(
                "missing or non-utf8 value for If-Modified-Since".into(),
            )
        })?
        .map_err(|e| PreconditionError::InvalidPreconditionValue(e.to_string()))
}

pub struct BuildPreconditionService;

impl<S: Clone + Send + Sync + 'static> Layer<S> for BuildPreconditionService {
    type Service = PreconditionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PreconditionService::new(service)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert_matches::assert_matches;
    use chrono::{Days, Utc};
    use futures::future;

    use super::*;

    lazy_static::lazy_static! {
        static ref LAST_MODIFIED: DateTime<Utc> = Utc::now();
    }

    const E_TAG: &str = "foo_42";

    #[derive(Clone)]
    struct MockInnermostService;

    impl Service<Request> for MockInnermostService {
        type Response = Response;
        type Error = Error;
        type Future = PinnedFuture;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request) -> Self::Future {
            match req {
                Request::GetMetadata(location, _) if location == "foo" => {
                    Box::pin(future::ok(Response::Head(GetObjectMetaResponse {
                        location: "foo".to_string(),
                        last_modified: *LAST_MODIFIED,
                        size: 0,
                        e_tag: Some(E_TAG.to_string()),
                        version: Some("v1".to_string()),
                    })))
                }
                Request::GetObject(location, _) if location == "foo" => {
                    Box::pin(future::ok(Response::Ready)) // don't bother with dummy data. Return ready.
                }
                _ => unreachable!("code error, did not issue proper head_req to inner"),
            }
        }
    }

    #[tokio::test]
    async fn test_no_preconditions() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject("foo".into(), Default::default());
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected no precondition to work, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_if_match() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MATCH, HeaderValue::from_static(E_TAG))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_match to return ok with exact match, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MATCH, HeaderValue::from_static("42"))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NoMatchFound(_))),
            "expected if_match to return error as no weak matches allowed, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MATCH, HeaderValue::from_static("*"))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_match to return ok with wildcard, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MATCH, HeaderValue::from_static(""))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NoMatchFound(_))),
            "expected if_match to return error with empty string, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MATCH, format!("bar baz {}", E_TAG))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_match to return ok with multiple match patterns, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_MATCH,
                HeaderValue::from_static("bar baz nope"),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NoMatchFound(_))),
            "expected if_match to return error if nothing matches, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_if_none_match() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_NONE_MATCH, HeaderValue::from_static(E_TAG))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::MatchFound(_))),
            "expected if_none_match to return error with exact match, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_NONE_MATCH, HeaderValue::from_static("42"))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_none_match to pass (not error) on partial match, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_none_match to pass (not error) with wildcard, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_NONE_MATCH, HeaderValue::from_static(""))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_none_match to return ok for empty string, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_NONE_MATCH, format!("bar baz {}", E_TAG))]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::MatchFound(_))),
            "expected if_none_match to return error with multiple match patterns, instead found {:?}", res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_NONE_MATCH,
                HeaderValue::from_static("bar baz nope"),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_none_match to return ok if nothing matches, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_if_modified_since() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_MODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_sub_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_modified_since to return ok modified after date, instead found {:?}",
            res
        );

        // If the selected representation's last modification date is earlier or equal to the date provided in the field value, the condition is false.
        // See <https://datatracker.ietf.org/doc/html/rfc7232#section-3.3>
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(http::header::IF_MODIFIED_SINCE, LAST_MODIFIED.to_rfc2822())]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NotModified(_))),
            "expected if_modified_since to return error if modified on exact date, instead found {:?}", res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_MODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NotModified(_))),
            "expected if_modified_since to return error modified before date, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_if_unmodified_since() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_UNMODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_sub_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::Modified(_))),
            "expected if_unmodified_since to return error if modified after date, instead found {:?}", res
        );

        // If the selected representation's last modification date is earlier or equal to the date provided in the field value, the condition is true.
        // See <https://datatracker.ietf.org/doc/html/rfc7232#section-3.4>
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_UNMODIFIED_SINCE,
                LAST_MODIFIED.to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_unmodified_since to return ok if modified on exact date, instead found {:?}", res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_UNMODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_unmodified_since to return ok if modified before date, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_ignore_headers() {
        let mut service = PreconditionService::new(MockInnermostService);

        // Failure case for If-Modified-Since
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_MODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::NotModified(_))),
            "expected if_modified_since to return error modified before date, instead found {:?}",
            res
        );

        // add If-None-Match header, which should override If-Modified-Since.
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([
                (
                    http::header::IF_MODIFIED_SINCE,
                    LAST_MODIFIED
                        .checked_add_days(Days::new(1))
                        .unwrap()
                        .to_rfc2822(),
                ),
                (http::header::IF_NONE_MATCH, "*".to_string()),
            ]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_modified_since to to be ignored, instead found {:?}",
            res
        );

        // Failure case for If-Unmodified-Since
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                http::header::IF_UNMODIFIED_SINCE,
                LAST_MODIFIED
                    .checked_sub_days(Days::new(1))
                    .unwrap()
                    .to_rfc2822(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::Modified(_))),
            "expected if_unmodified_since to return error if modified after date, instead found {:?}", res
        );

        // add If-Match header, which should override If-Unmodified-Since.
        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([
                (
                    http::header::IF_UNMODIFIED_SINCE,
                    LAST_MODIFIED
                        .checked_sub_days(Days::new(1))
                        .unwrap()
                        .to_rfc2822(),
                ),
                (http::header::IF_MATCH, "*".to_string()),
            ]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected if_unmodified_since to to be ignored, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_version() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                crate::data_types::X_VERSION_HEADER.to_string(),
                "v0".to_string(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Err(Error::Precondition(PreconditionError::Version)),
            "expected version to return error if not matching, instead found {:?}",
            res
        );

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                crate::data_types::X_VERSION_HEADER.to_string(),
                "v1".to_string(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Ready),
            "expected version to return ok if matching, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_head() {
        let mut service = PreconditionService::new(MockInnermostService);

        let req = Request::GetObject(
            "foo".into(),
            (&HashMap::from([(
                crate::data_types::X_HEAD_HEADER.to_string(),
                "true".to_string(),
            )]))
                .try_into()
                .unwrap(),
        );
        let res = service.call(req).await;
        assert_matches!(
            res,
            Ok(Response::Head(GetObjectMetaResponse { .. })),
            "expected head=true in options to override any `GET /object` req, instead found {:?}",
            res
        );
    }
}
