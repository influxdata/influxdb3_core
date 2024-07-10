//! Actual caching logic.

use base64::Engine;
use futures::FutureExt;
use iox_time::TimeProvider;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use backoff::{Backoff, BackoffConfig};
use catalog_cache::{
    api::quorum::{Error as QuorumError, QuorumCatalogCache},
    CacheKey, CacheValue,
};

use metric::DurationHistogram;
use observability_deps::tracing::{debug, warn};
use trace::{
    ctx::SpanContext,
    span::{Span, SpanEvent, SpanExt, SpanRecorder},
};

use crate::{
    cache::{
        batcher::Batcher,
        loader::Loader,
        snapshot::{Snapshot, SnapshotKey},
    },
    interface::{Catalog, Error, Result},
};

#[derive(Debug)]
struct CacheMetrics {
    get_hit: DurationHistogram,
    get_miss: DurationHistogram,
    put: DurationHistogram,
}

impl CacheMetrics {
    fn new(registry: &metric::Registry, variant: &'static str) -> Self {
        let metric = registry.register_metric::<DurationHistogram>(
            "iox_catalog_cache_op",
            "Distribution of operations to the catalog cache service",
        );

        Self {
            get_hit: metric.recorder(&[("variant", variant), ("op", "get"), ("result", "hit")]),
            get_miss: metric.recorder(&[("variant", variant), ("op", "get"), ("result", "miss")]),
            put: metric.recorder(&[("variant", variant), ("op", "put")]),
        }
    }
}

/// Shared state between [`CacheHandlerCatalog`] and [`CacheHandlerRepos`].
#[derive(Debug)]
struct CacheHandlerInner<T>
where
    T: Snapshot,
{
    backing: Arc<dyn Catalog>,
    metrics: Arc<CacheMetrics>,
    cache: Arc<QuorumCatalogCache>,
    backoff_config: Arc<BackoffConfig>,
    loader: Loader<T::Key, T>,
    etag_min_payload_size: usize,
    batcher: Batcher<T::Key, Option<SpanContext>>,
}

impl<T> CacheHandlerInner<T>
where
    T: Snapshot,
{
    /// Get data from quorum cache.
    ///
    /// This method implements retries.
    async fn get_quorum(
        &self,
        key: CacheKey,
        span: Option<Span>,
    ) -> Result<Option<CacheValue>, QuorumError> {
        let mut span_recorder = SpanRecorder::new(span);
        let mut backoff = Backoff::new(&self.backoff_config);

        // Note: We don't use retry_with_backoff as some retries are expected in the event
        // of racing writers or only two available replicas. We should only log if
        // the deadline expires
        let res = loop {
            let mut span_recorder = span_recorder.child("retry_round");
            match self.cache.get(key).await {
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
        let backing = Arc::clone(&self.backing);
        let cache = Arc::clone(&self.cache);
        let min_etag = self.etag_min_payload_size;
        let metrics = Arc::clone(&self.metrics);

        let fut = async move {
            let snapshot = T::snapshot(backing.as_ref(), k).await?;
            let generation = snapshot.generation();
            let data = snapshot.to_bytes();

            let mut value = CacheValue::new(data, generation);

            if value.data().len() >= min_etag {
                let digest = ring::digest::digest(&ring::digest::SHA256, value.data());
                value = value.with_etag(base64::prelude::BASE64_STANDARD.encode(digest));
            }

            let start = Instant::now();
            debug!(what = T::NAME, key = k.get(), generation, "refresh");
            let put_res = cache.put(k.to_key(), value).await.map_err(|e| {
                warn!(
                    what=T::NAME,
                    key=k.get(),
                    generation,
                    %e,
                    "quorum write failed",
                );

                e
            });
            metrics.put.record(start.elapsed());
            put_res?;

            Ok(snapshot)
        };

        let snapshot_res = match refresh {
            true => self.loader.refresh(k, fut).await,
            false => self.loader.load(k, fut).await,
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
        etag_min_payload_size: usize,
        time_provider: Arc<dyn TimeProvider>,
        batch_delay: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(CacheHandlerInner {
                backing,
                metrics: Arc::new(CacheMetrics::new(registry, T::NAME)),
                cache,
                backoff_config,
                loader: Loader::default(),
                etag_min_payload_size,
                batcher: Batcher::new(time_provider, batch_delay),
            }),
        }
    }

    pub(crate) fn repos(&self) -> CacheHandlerRepos<T> {
        CacheHandlerRepos {
            inner: Arc::clone(&self.inner),
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

    /// Refresh cached value of given snapshot.
    ///
    /// This requests a new snapshot and performs a quorum-write.
    ///
    /// Note that this also performs a snapshot+write if the data was NOT cached yet.
    pub(crate) async fn refresh(&self, key: T::Key) -> Result<()> {
        self.refresh_inner(key, false).await
    }

    pub(crate) async fn refresh_batched(&self, key: T::Key) -> Result<()> {
        self.refresh_inner(key, true).await
    }

    /// Shared implementation of [`refresh`](Self::refresh) and [`refresh_batched`](Self::refresh_batched)
    async fn refresh_inner(&self, key: T::Key, batched: bool) -> Result<()> {
        let mut span_recorder = self.span_recorder(if batched {
            "refresh batched"
        } else {
            "refresh"
        });

        let inner = Arc::clone(&self.inner);
        let batch = self.inner.batcher.batch(
            key,
            Box::new(move |span_contexts| {
                let inner = Arc::clone(&inner);
                let span_contexts = span_contexts.clone();

                async move {
                    let mut span_recorder = SpanRecorder::new(
                        span_contexts
                            .iter()
                            .filter_map(|ctx| ctx.as_ref().cloned())
                            .next()
                            .map(|ctx| ctx.child("batched refresh")),
                    );

                    for ctx in &span_contexts {
                        if let Some(ctx) = ctx.as_ref() {
                            span_recorder.link(ctx);
                        }
                    }

                    match inner
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
                .boxed()
            }),
            span_recorder.span().map(|span| span.ctx.clone()),
        );
        if !batched {
            batch.rush();
        }

        match batch.await {
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
            .inner
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
            .inner
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
                self.inner.metrics.get_hit.record(start.elapsed());
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
                    source: Arc::new(e),
                });
            }
        }

        self.inner.metrics.get_miss.record(start.elapsed());

        match self
            .inner
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
