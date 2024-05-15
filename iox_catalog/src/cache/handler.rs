//! Actual caching logic.

use base64::Engine;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::{Backoff, BackoffConfig};
use catalog_cache::{
    api::quorum::{Error as QuorumError, QuorumCatalogCache},
    CacheKey, CacheValue,
};

use metric::{DurationHistogram, U64Counter};
use observability_deps::tracing::{debug, warn};
use trace::ctx::SpanContext;
use trace::span::{Span, SpanEvent, SpanExt, SpanRecorder};

use crate::cache::snapshot::SnapshotKey;
use crate::{
    cache::loader::Loader,
    interface::{Catalog, Error, Result},
};

use super::snapshot::Snapshot;

#[derive(Debug)]
struct CacheMetric {
    count: U64Counter,
    duration: DurationHistogram,
}

impl CacheMetric {
    fn new(registry: &metric::Registry, variant: &'static str, result: &'static str) -> Self {
        let count = registry.register_metric::<U64Counter>(
            "iox_catalog_cache_get",
            "Number of GET operations to the catalog cache service",
        );

        let duration = registry.register_metric::<DurationHistogram>(
            "iox_catalog_cache_get_duration",
            "Distribution of GET operations to the catalog cache service",
        );

        let attributes = &[("variant", variant), ("result", result)];
        Self {
            count: count.recorder(attributes),
            duration: duration.recorder(attributes),
        }
    }

    fn record(&self, duration: Duration) {
        self.count.inc(1);
        self.duration.record(duration);
    }
}

/// Shared state between [`CacheHandlerCatalog`] and [`CacheHandlerRepos`].
#[derive(Debug)]
struct CacheHandlerInner<T>
where
    T: Snapshot,
{
    backing: Arc<dyn Catalog>,
    get_hit: CacheMetric,
    get_miss: CacheMetric,
    cache: Arc<QuorumCatalogCache>,
    backoff_config: Arc<BackoffConfig>,
    loader: Loader<T::Key, T>,
}

/// Abstract cache access.
#[derive(Debug)]
pub(crate) struct CacheHandlerCatalog<T>
where
    T: Snapshot,
{
    inner: Arc<CacheHandlerInner<T>>,
}

impl<T> CacheHandlerCatalog<T>
where
    T: Snapshot,
{
    pub(crate) fn new(
        backing: Arc<dyn Catalog>,
        registry: &metric::Registry,
        cache: Arc<QuorumCatalogCache>,
        backoff_config: Arc<BackoffConfig>,
    ) -> Self {
        Self {
            inner: Arc::new(CacheHandlerInner {
                backing,
                get_hit: CacheMetric::new(registry, T::NAME, "hit"),
                get_miss: CacheMetric::new(registry, T::NAME, "miss"),
                cache,
                backoff_config,
                loader: Loader::default(),
            }),
        }
    }

    pub(crate) fn repos(&self, etag_min_payload_size: usize) -> CacheHandlerRepos<T> {
        CacheHandlerRepos {
            inner: Arc::clone(&self.inner),
            etag_min_payload_size,
            span_ctx: None,
        }
    }
}

/// Abstract cache access.
#[derive(Debug)]
pub(crate) struct CacheHandlerRepos<T>
where
    T: Snapshot,
{
    inner: Arc<CacheHandlerInner<T>>,
    span_ctx: Option<SpanContext>,
    etag_min_payload_size: usize,
}

impl<T> CacheHandlerRepos<T>
where
    T: Snapshot,
{
    /// Set span context.
    pub(crate) fn set_span_context(&mut self, span_ctx: Option<SpanContext>) {
        self.span_ctx = span_ctx;
    }

    /// Get span recorder.
    fn span_recorder(&self, action: &'static str) -> SpanRecorder {
        let mut recorder = SpanRecorder::new(self.span_ctx.child_span(action));
        recorder.set_metadata("snapshot_type", T::NAME);
        recorder
    }

    /// Get data from quorum cache.
    ///
    /// This method implements retries.
    async fn get_quorum(
        &self,
        key: CacheKey,
        span: Option<Span>,
    ) -> Result<Option<CacheValue>, QuorumError> {
        let mut span_recorder = SpanRecorder::new(span);
        let mut backoff = Backoff::new(&self.inner.backoff_config);

        // Note: We don't use retry_with_backoff as some retries are expected in the event
        // of racing writers or only two available replicas. We should only log if
        // the deadline expires
        let res = loop {
            let mut span_recorder = span_recorder.child("retry_round");
            match self.inner.cache.get(key).await {
                Ok(val) => {
                    span_recorder.ok("ok");
                    break Ok(val);
                }
                Err(e @ QuorumError::Quorum { .. }) => {
                    span_recorder.event(SpanEvent::new(format!("quorum error: {}", e)));

                    match backoff.next() {
                        None => {
                            span_recorder.error("Deadline exceeded".to_owned());
                            break Err(e);
                        }
                        Some(delay) => {
                            span_recorder.ok(format!("sleep {}s", delay.as_secs_f32()));
                            tokio::time::sleep(delay).await
                        }
                    }
                }
                Err(e) => {
                    span_recorder.error(e.to_string());
                    break Err(e);
                }
            }
        };

        match res {
            Ok(val) => {
                span_recorder.ok("ok");
                Ok(val)
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                Err(e)
            }
        }
    }

    /// Perform a snapshot
    ///
    /// If `refresh` is false will potentially await an existing snapshot request
    async fn do_snapshot(&self, k: T::Key, refresh: bool, span: Option<Span>) -> Result<T> {
        let mut span_recorder = SpanRecorder::new(span);
        let backing = Arc::clone(&self.inner.backing);
        let cache = Arc::clone(&self.inner.cache);
        let min_etag = self.etag_min_payload_size;

        let fut = async move {
            let snapshot = T::snapshot(backing.as_ref(), k).await?;
            let generation = snapshot.generation();
            let data = snapshot.to_bytes();

            let mut value = CacheValue::new(data, generation);

            if value.data().len() >= min_etag {
                let digest = ring::digest::digest(&ring::digest::SHA256, value.data());
                value = value.with_etag(base64::prelude::BASE64_STANDARD.encode(digest));
            }

            debug!(what = T::NAME, key = k.get(), generation, "refresh");
            cache.put(k.to_key(), value).await.map_err(|e| {
                warn!(
                    what=T::NAME,
                    key=k.get(),
                    generation,
                    %e,
                    "quorum write failed",
                );

                e
            })?;

            Ok(snapshot)
        };

        let snapshot_res = match refresh {
            true => self.inner.loader.refresh(k, fut).await,
            false => self.inner.loader.load(k, fut).await,
        };
        match snapshot_res {
            Ok(snapshot) => {
                span_recorder.ok("ok");
                Ok(snapshot)
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                Err(e.into())
            }
        }
    }

    /// Refresh cached value of given snapshot.
    ///
    /// This requests a new snapshot and performs a quorum-write.
    ///
    /// Note that this also performs a snapshot+write if the data was NOT cached yet.
    pub(crate) async fn refresh(&self, key: T::Key) -> Result<()> {
        let mut span_recorder = self.span_recorder("refresh");
        match self
            .do_snapshot(key, true, span_recorder.child_span("snapshot"))
            .await
        {
            Ok(_) => {
                span_recorder.ok("ok");
                Ok(())
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                Err(e)
            }
        }
    }

    /// Warm up cached value.
    pub(crate) async fn warm_up(&self, key: T::Key) -> Result<()> {
        let mut span_recorder = self.span_recorder("warm up");
        match self
            .do_snapshot(key, false, span_recorder.child_span("snapshot"))
            .await
        {
            Ok(_) => {
                span_recorder.ok("ok");
                Ok(())
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                Err(e)
            }
        }
    }

    /// Get snapshot.
    ///
    /// This first tries to quorum-read the data. If the data does not exist yet, this will perform a
    /// [refresh](Self::refresh).
    pub(crate) async fn get(&self, k: T::Key) -> Result<T> {
        let mut span_recorder = self.span_recorder("get");
        let start = Instant::now();
        let key = k.to_key();

        match self
            .get_quorum(key, span_recorder.child_span("get_quorum"))
            .await
        {
            Ok(Some(val)) => {
                debug!(
                    what = T::NAME,
                    key = k.get(),
                    status = "HIT",
                    generation = val.generation(),
                    "get",
                );
                self.inner.get_hit.record(start.elapsed());
                span_recorder.ok("HIT");

                return T::from_cache_value(val);
            }
            Ok(None) => {
                debug!(what = T::NAME, key = k.get(), status = "MISS", "get",);
                span_recorder.event(SpanEvent::new("MISS"));
            }
            Err(e @ QuorumError::Quorum { .. }) => {
                warn!(
                    what = T::NAME,
                    key = k.get(),
                    elapsed=start.elapsed().as_secs_f64(),
                    %e,
                    "deadline expired for quorum read, obtaining fresh snapshot",
                );
                span_recorder.event(SpanEvent::new("deadline expired"));
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                return Err(Error::External {
                    source: Box::new(e),
                });
            }
        }

        self.inner.get_miss.record(start.elapsed());

        match self
            .do_snapshot(k, false, span_recorder.child_span("snapshot"))
            .await
        {
            Ok(x) => {
                span_recorder.ok("got new snapshot");
                Ok(x)
            }
            Err(e) => {
                span_recorder.error(e.to_string());
                Err(e)
            }
        }
    }
}
