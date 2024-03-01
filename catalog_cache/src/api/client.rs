//! Client for the cache HTTP API

use crate::api::list::{ListDecoder, ListEntry, MAX_VALUE_SIZE};
use crate::api::{RequestPath, GENERATION, GENERATION_NOT_MATCH};
use crate::{CacheKey, CacheValue};
use bytes::{Buf, Bytes};
use futures::prelude::*;
use futures::stream::BoxStream;
use hyper::client::connect::dns::{GaiResolver, Name};
use hyper::service::Service;
use metric::DurationHistogram;
use reqwest::dns::{Resolve, Resolving};
use reqwest::{Client, Response, StatusCode, Url};
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Creating client: {source}"))]
    Client { source: reqwest::Error },

    #[snafu(display("Put Reqwest error: {source}"))]
    Put { source: reqwest::Error },

    #[snafu(display("Get Reqwest error: {source}"))]
    Get { source: reqwest::Error },

    #[snafu(display("List Reqwest error: {source}"))]
    List { source: reqwest::Error },

    #[snafu(display("Health Reqwest error: {source}"))]
    Health { source: reqwest::Error },

    #[snafu(display("Missing generation header"))]
    MissingGeneration,

    #[snafu(display("Invalid generation value"))]
    InvalidGeneration,

    #[snafu(display("Not modified"))]
    NotModified,

    #[snafu(display("Error decoding list stream: {source}"), context(false))]
    ListStream { source: crate::api::list::Error },
}

/// Result type for [`CatalogCacheClient`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The type returned by [`CatalogCacheClient::list`]
pub type ListStream = BoxStream<'static, Result<ListEntry>>;

/// Builder for [`CatalogCacheClient`].
#[derive(Debug)]
pub struct CatalogCacheClientBuilder {
    connect_timeout: Duration,
    get_request_timeout: Duration,
    put_request_timeout: Duration,
    list_request_timeout: Duration,
    endpoint: Url,
    registry: Arc<metric::Registry>,
}

impl CatalogCacheClientBuilder {
    /// Build client.
    pub fn build(self) -> Result<CatalogCacheClient> {
        let Self {
            connect_timeout,
            get_request_timeout,
            put_request_timeout,
            list_request_timeout,
            endpoint,
            registry,
        } = self;

        // Note: do NOT set `.timeout` here because we set the timeout per request type.
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .dns_resolver(Arc::new(Resolver::new(&registry)))
            .http2_prior_knowledge()
            .http2_adaptive_window(true)
            .build()
            .context(ClientSnafu)?;

        Ok(CatalogCacheClient {
            endpoint,
            client,
            get_request_timeout,
            put_request_timeout,
            list_request_timeout,
        })
    }

    /// Set a timeout for only the connect phase of a `Client`.
    pub fn connect_timeout(self, connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            ..self
        }
    }

    /// Set timetout for `GET` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    pub fn get_request_timeout(self, get_request_timeout: Duration) -> Self {
        Self {
            get_request_timeout,
            ..self
        }
    }

    /// Set timetout for `PUT` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    pub fn put_request_timeout(self, put_request_timeout: Duration) -> Self {
        Self {
            put_request_timeout,
            ..self
        }
    }

    /// Set timetout for `LIST` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    ///
    /// Given the non-trivial amount of data that this may transfer, this should be set higher than the
    /// [`get_request_timeout`](Self::get_request_timeout).
    pub fn list_request_timeout(self, list_request_timeout: Duration) -> Self {
        Self {
            list_request_timeout,
            ..self
        }
    }
}

/// A client for accessing a remote catalog cache
#[derive(Debug)]
pub struct CatalogCacheClient {
    client: Client,
    endpoint: Url,
    get_request_timeout: Duration,
    put_request_timeout: Duration,
    list_request_timeout: Duration,
}

impl CatalogCacheClient {
    /// Set up builder with the given remote endpoint.
    pub fn builder(endpoint: Url, registry: Arc<metric::Registry>) -> CatalogCacheClientBuilder {
        CatalogCacheClientBuilder {
            connect_timeout: Duration::from_secs(2),
            get_request_timeout: Duration::from_secs(1),
            put_request_timeout: Duration::from_secs(1),
            // We use a longer timeout for list request as they may transfer a non-trivial amount of data
            list_request_timeout: Duration::from_secs(20),
            endpoint,
            registry,
        }
    }

    /// Retrieve the given value from the remote cache, if present
    pub async fn get(&self, key: CacheKey) -> Result<Option<CacheValue>> {
        self.get_if_modified(key, None).await
    }

    /// Retrieve the given value from the remote cache, if present
    ///
    /// Returns [`Error::NotModified`] if value exists and matches `generation`
    pub async fn get_if_modified(
        &self,
        key: CacheKey,
        generation: Option<u64>,
    ) -> Result<Option<CacheValue>> {
        let url = format!("{}{}", self.endpoint, RequestPath::Resource(key));
        let mut req = self.client.get(url).timeout(self.get_request_timeout);
        if let Some(generation) = generation {
            req = req.header(&GENERATION_NOT_MATCH, generation)
        }

        let resp = req.send().await.context(GetSnafu)?;
        let resp = match resp.status() {
            StatusCode::NOT_FOUND => return Ok(None),
            StatusCode::NOT_MODIFIED => return Err(Error::NotModified),
            _ => resp.error_for_status().context(GetSnafu)?,
        };

        let generation = resp
            .headers()
            .get(&GENERATION)
            .context(MissingGenerationSnafu)?;

        let generation = generation
            .to_str()
            .ok()
            .and_then(|v| v.parse().ok())
            .context(InvalidGenerationSnafu)?;

        let data = resp.bytes().await.context(GetSnafu)?;

        Ok(Some(CacheValue::new(data, generation)))
    }

    /// Upsert the given key-value pair to the remote cache
    ///
    /// Returns false if the value had a generation less than or equal to
    /// an existing value
    pub async fn put(&self, key: CacheKey, value: &CacheValue) -> Result<bool> {
        let url = format!("{}{}", self.endpoint, RequestPath::Resource(key));

        let response = self
            .client
            .put(url)
            .timeout(self.put_request_timeout)
            .header(&GENERATION, value.generation)
            .body(value.data.clone())
            .send()
            .await
            .context(PutSnafu)?
            .error_for_status()
            .context(PutSnafu)?;

        Ok(matches!(response.status(), StatusCode::OK))
    }

    /// List the contents of the remote cache
    ///
    /// Values larger than `max_value_size` will not be returned inline, with only the key
    /// and generation returned instead. Defaults to [`MAX_VALUE_SIZE`]
    pub fn list(&self, max_value_size: Option<usize>) -> ListStream {
        let size = max_value_size.unwrap_or(MAX_VALUE_SIZE);
        let url = format!("{}{}?size={size}", self.endpoint, RequestPath::List);
        let fut = self
            .client
            .get(url)
            .timeout(self.list_request_timeout)
            .send();

        futures::stream::once(fut.map_err(|source| Error::List { source }))
            .and_then(move |response| futures::future::ready(list_stream(response, size)))
            .try_flatten()
            .boxed()
    }
}

struct ListStreamState {
    response: Response,
    current: Bytes,
    decoder: ListDecoder,
}

impl ListStreamState {
    fn new(response: Response, max_value_size: usize) -> Self {
        Self {
            response,
            current: Default::default(),
            decoder: ListDecoder::new().with_max_value_size(max_value_size),
        }
    }
}

fn list_stream(
    response: Response,
    max_value_size: usize,
) -> Result<impl Stream<Item = Result<ListEntry>>> {
    let resp = response.error_for_status().context(ListSnafu)?;
    let state = ListStreamState::new(resp, max_value_size);
    Ok(stream::try_unfold(state, |mut state| async move {
        loop {
            if state.current.is_empty() {
                match state.response.chunk().await.context(ListSnafu)? {
                    Some(new) => state.current = new,
                    None => break,
                }
            }

            let to_read = state.current.len();
            let read = state.decoder.decode(&state.current)?;
            state.current.advance(read);
            if read != to_read {
                break;
            }
        }
        Ok(state.decoder.flush()?.map(|entry| (entry, state)))
    }))
}

/// A custom [`Resolve`] that collects [`ResolverMetrics`]
#[derive(Debug, Clone)]
struct Resolver {
    resolver: GaiResolver,
    metrics: Arc<ResolverMetrics>,
}

impl Resolver {
    fn new(registry: &metric::Registry) -> Self {
        Self {
            resolver: GaiResolver::new(),
            metrics: Arc::new(ResolverMetrics::new(registry)),
        }
    }
}

impl Resolve for Resolver {
    fn resolve(&self, name: Name) -> Resolving {
        let mut s = self.clone();
        let metrics = Arc::clone(&self.metrics);
        let start = Instant::now();
        Box::pin(async move {
            let r = s.resolver.call(name).await;
            metrics.record(start.elapsed());
            r.map(|addrs| Box::new(addrs) as _)
                .map_err(|e| Box::new(e) as _)
        })
    }
}

#[derive(Debug)]
struct ResolverMetrics {
    duration: DurationHistogram,
}

impl ResolverMetrics {
    fn new(registry: &metric::Registry) -> Self {
        let duration = registry.register_metric::<DurationHistogram>(
            "dns_request_duration",
            "Time to perform a DNS request",
        );

        Self {
            duration: duration.recorder(&[("client", "catalog")]),
        }
    }

    fn record(&self, duration: Duration) {
        self.duration.record(duration);
    }
}
