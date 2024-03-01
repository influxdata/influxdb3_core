//! The remote API for the catalog cache

use crate::CacheKey;
use hyper::http::HeaderName;

pub mod client;

pub mod quorum;

pub mod server;

pub mod list;

/// The header used to encode the generation in a get response
static GENERATION: HeaderName = HeaderName::from_static("x-influx-generation");

/// The header used to make a conditional get request
static GENERATION_NOT_MATCH: HeaderName =
    HeaderName::from_static("x-influx-if-generation-not-match");

/// Defines the mapping to HTTP paths for given request types
#[derive(Debug, Eq, PartialEq)]
enum RequestPath {
    /// A request addressing a resource identified by [`CacheKey`]
    Resource(CacheKey),
    /// A list request
    List,
}

impl RequestPath {
    fn parse(s: &str) -> Option<Self> {
        let s = s.strip_prefix('/').unwrap_or(s);
        let mut parts = s.split('/');

        let version = parts.next()?;
        if version != "v1" {
            return None;
        }

        let variant = parts.next()?;

        let ensure_end = |mut parts: std::str::Split<'_, char>| {
            if parts.next().is_some() {
                // trailing information => unknown
                None
            } else {
                Some(())
            }
        };
        let parse_value = |mut parts: std::str::Split<'_, char>| {
            let s = parts.next()?;

            ensure_end(parts)?;

            u64::from_str_radix(s, 16).map(|v| v as i64).ok()
        };

        match variant {
            "" => {
                ensure_end(parts)?;
                Some(Self::List)
            }
            "r" => {
                ensure_end(parts)?;
                Some(Self::Resource(CacheKey::Root))
            }
            "n" => Some(Self::Resource(CacheKey::Namespace(parse_value(parts)?))),
            "t" => Some(Self::Resource(CacheKey::Table(parse_value(parts)?))),
            "p" => Some(Self::Resource(CacheKey::Partition(parse_value(parts)?))),
            _ => None,
        }
    }
}

impl std::fmt::Display for RequestPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List => write!(f, "v1/"),
            Self::Resource(CacheKey::Root) => {
                write!(f, "v1/r")
            }
            Self::Resource(CacheKey::Namespace(v)) => write!(f, "v1/n/{v:016x}"),
            Self::Resource(CacheKey::Table(v)) => write!(f, "v1/t/{v:016x}"),
            Self::Resource(CacheKey::Partition(v)) => write!(f, "v1/p/{v:016x}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::Error;
    use crate::api::list::ListEntry;
    use crate::api::server::test_util::TestCacheServer;
    use crate::api::RequestPath;
    use crate::{CacheKey, CacheValue};
    use futures::TryStreamExt;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn test_request_path() {
        let paths = [
            RequestPath::List,
            RequestPath::Resource(CacheKey::Root),
            RequestPath::Resource(CacheKey::Partition(12)),
            RequestPath::Resource(CacheKey::Partition(i64::MAX)),
            RequestPath::Resource(CacheKey::Partition(i64::MIN)),
            RequestPath::Resource(CacheKey::Namespace(12)),
            RequestPath::Resource(CacheKey::Namespace(i64::MAX)),
            RequestPath::Resource(CacheKey::Namespace(i64::MIN)),
            RequestPath::Resource(CacheKey::Table(12)),
            RequestPath::Resource(CacheKey::Table(i64::MAX)),
            RequestPath::Resource(CacheKey::Table(i64::MIN)),
        ];

        let mut set = HashSet::with_capacity(paths.len());
        for path in paths {
            println!("test: {path:?}");
            let s = path.to_string();
            let back = RequestPath::parse(&s).unwrap();
            assert_eq!(back, path);
            assert!(set.insert(s), "should be unique");
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let metric_registry = Arc::new(metric::Registry::new());
        let serve = TestCacheServer::bind_ephemeral(&metric_registry);
        let client = serve.client();

        let key = CacheKey::Partition(1);

        let v1 = CacheValue::new("1".into(), 2);
        assert!(client.put(key, &v1).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v1, returned);

        // Duplicate upsert ignored
        assert!(!client.put(key, &v1).await.unwrap());

        // Stale upsert ignored
        let v2 = CacheValue::new("2".into(), 1);
        assert!(!client.put(key, &v2).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v1, returned);

        let err = client.get_if_modified(key, Some(2)).await.unwrap_err();
        assert!(matches!(err, Error::NotModified), "{err}");

        let returned = client.get_if_modified(key, Some(1)).await.unwrap().unwrap();
        assert_eq!(v1, returned);

        let v3 = CacheValue::new("3".into(), 3);
        assert!(client.put(key, &v3).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v3, returned);

        let key2 = CacheKey::Partition(5);
        assert!(client.put(key2, &v1).await.unwrap());

        let mut result = client.list(None).try_collect::<Vec<_>>().await.unwrap();
        result.sort_unstable_by_key(|entry| entry.key());

        let expected = vec![ListEntry::new(key, v3), ListEntry::new(key2, v1)];
        assert_eq!(result, expected);

        serve.shutdown().await;
    }

    #[tokio::test]
    async fn test_list_size() {
        let metric_registry = Arc::new(metric::Registry::new());
        let serve = TestCacheServer::bind_ephemeral(&metric_registry);
        let client = serve.client();

        let v1 = CacheValue::new("123".into(), 2);
        client.put(CacheKey::Table(1), &v1).await.unwrap();

        let v2 = CacheValue::new("13".into(), 2);
        client.put(CacheKey::Table(2), &v2).await.unwrap();

        let v3 = CacheValue::new("1".into(), 2);
        client.put(CacheKey::Table(3), &v3).await.unwrap();

        let mut res = client.list(Some(2)).try_collect::<Vec<_>>().await.unwrap();
        res.sort_unstable_by_key(|x| x.key());

        assert_eq!(res.len(), 3);

        assert_eq!(res[0].value(), None);
        assert_eq!(res[1].value(), Some(&v2.data));
        assert_eq!(res[2].value(), Some(&v3.data));

        let mut res = client.list(Some(3)).try_collect::<Vec<_>>().await.unwrap();
        res.sort_unstable_by_key(|x| x.key());

        assert_eq!(res[0].value(), Some(&v1.data));
        assert_eq!(res[1].value(), Some(&v2.data));
        assert_eq!(res[2].value(), Some(&v3.data));
    }
}
