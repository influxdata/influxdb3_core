use std::{ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use metric::U64Counter;
use object_store::{
    path::Path, DynObjectStore, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result,
};
use tokio::runtime::Handle;

use crate::cache_system::{
    cache::{Cache, CacheState},
    hook::{chain::HookChain, limit::MemoryLimiter, observer::ObserverHook},
    interfaces::HasSize,
    reactor::{ticker, Reactor, TriggerExt},
};

const CACHE_NAME: &str = "object_store";

#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

impl CacheValue {
    async fn fetch(store: &DynObjectStore, location: &Path) -> Result<Self> {
        let res = store.get(location).await?;
        let meta = res.meta.clone();
        let data = res.bytes().await?;
        Ok(Self { data, meta })
    }
}

impl HasSize for CacheValue {
    fn size(&self) -> usize {
        let Self { data, meta } = self;
        let ObjectMeta {
            location,
            last_modified: _,
            size: _,
            e_tag,
            version,
        } = meta;

        data.len()
            + location.as_ref().len()
            + e_tag.as_ref().map(|s| s.capacity()).unwrap_or_default()
            + version.as_ref().map(|s| s.capacity()).unwrap_or_default()
    }
}

#[derive(Debug)]
struct HitMetrics {
    cached: U64Counter,
    miss: U64Counter,
    miss_already_loading: U64Counter,
}

impl HitMetrics {
    fn new(metrics: &metric::Registry) -> Self {
        let m = metrics.register_metric::<U64Counter>(
            "object_store_in_mem_cache_access",
            "Counts acccesses to object store in mem cache",
        );
        Self {
            cached: m.recorder(&[("status", "cached")]),
            miss: m.recorder(&[("status", "miss")]),
            miss_already_loading: m.recorder(&[("status", "miss_already_loading")]),
        }
    }
}

/// Parameters for [`MemCacheObjectStore`].
#[derive(Debug)]
pub struct MemCacheObjectStoreParams<'a> {
    /// Underlying, uncached object store.
    pub inner: Arc<DynObjectStore>,

    /// Memory limit in bytes.
    pub memory_limit: usize,

    /// After an OOM event triggered an emergency GC (= garbage collect) run, how how should we wait until the next
    /// run?
    ///
    /// If there is another OOM event during the cooldown period, new elements will NOT be cached.
    pub oom_throttle: Duration,

    /// How often should the GC (= garbage collector) remove unused elements and elements that failed.
    pub gc_interval: Duration,

    /// Metric registry for metrics.
    pub metrics: &'a metric::Registry,

    /// Tokio runtime handle for the background task that drives the GC (= garbage collector).
    pub handle: &'a Handle,
}

impl<'a> MemCacheObjectStoreParams<'a> {
    /// Build store from parameters.
    pub fn build(self) -> MemCacheObjectStore {
        let Self {
            inner,
            memory_limit,
            oom_throttle,
            gc_interval,
            metrics,
            handle,
        } = self;

        let memory_limiter = MemoryLimiter::new(memory_limit);
        let oom_notify = memory_limiter.oom();

        let cache = Arc::new(Cache::new(Arc::new(HookChain::new([
            Arc::new(memory_limiter) as _,
            Arc::new(ObserverHook::new(
                CACHE_NAME,
                metrics,
                Some(memory_limit as u64),
            )) as _,
        ]))));

        let cache_captured = Arc::downgrade(&cache);
        let reactor = Reactor::new(
            [
                oom_notify
                    .boxed()
                    .throttle(oom_throttle)
                    .observe(CACHE_NAME, "oom", metrics),
                ticker(gc_interval).observe(CACHE_NAME, "gc", metrics),
            ],
            Box::new(move || {
                if let Some(cache) = cache_captured.upgrade() {
                    cache.prune();
                }
            }),
            handle,
        );

        MemCacheObjectStore {
            store: inner,
            hit_metrics: HitMetrics::new(metrics),
            cache,
            reactor,
        }
    }
}

#[derive(Debug)]
pub struct MemCacheObjectStore {
    store: Arc<DynObjectStore>,
    hit_metrics: HitMetrics,
    cache: Arc<Cache<Path, CacheValue>>,
    // reactor must just kept alive
    #[allow(dead_code)]
    reactor: Reactor,
}

impl MemCacheObjectStore {
    async fn get_or_fetch(&self, location: &Path) -> Result<Arc<CacheValue>> {
        let (fut, state) = self.cache.get_or_fetch(location, |location| {
            let location = location.clone();
            let store = Arc::clone(&self.store);

            async move {
                CacheValue::fetch(&store, &location)
                    .await
                    .map_err(|e| Arc::new(e) as _)
            }
        });

        match state {
            CacheState::WasCached => match fut.peek() {
                None => &self.hit_metrics.miss_already_loading,
                Some(_) => &self.hit_metrics.cached,
            },
            CacheState::NewEntry => &self.hit_metrics.miss,
        }
        .inc(1);

        fut.await.map_err(|e| Error::Generic {
            store: "mem_cache",
            source: Box::new(e),
        })
    }
}

impl std::fmt::Display for MemCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemCache({})", self.store)
    }
}

#[async_trait]
impl ObjectStore for MemCacheObjectStore {
    async fn put(&self, _location: &Path, _bytes: PutPayload) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart(&self, _location: &Path) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented)
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let v = self.get_or_fetch(location).await?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter([Ok(v.data.clone())]).boxed()),
            meta: v.meta.clone(),
            range: 0..v.data.len(),
            attributes: Default::default(),
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // be rather conservative
        if any_options_set(options) {
            return Err(Error::NotImplemented);
        }

        self.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        Ok(self
            .get_ranges(location, &[range])
            .await?
            .into_iter()
            .next()
            .expect("requested one range"))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let v = self.get_or_fetch(location).await?;

        ranges
            .iter()
            .map(|range| {
                if range.end > v.data.len() {
                    return Err(Error::Generic {
                        store: "mem_cache",
                        source: format!(
                            "Range end ({}) out of bounds, object size is {}",
                            range.end,
                            v.data.len()
                        )
                        .into(),
                    });
                }
                if range.start > range.end {
                    return Err(Error::Generic {
                        store: "mem_cache",
                        source: format!(
                            "Range end ({}) is before range start ({})",
                            range.end, range.start
                        )
                        .into(),
                    });
                }
                Ok(v.data.slice(range.clone()))
            })
            .collect()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let v = self.get_or_fetch(location).await?;

        Ok(v.meta.clone())
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .and_then(|_| futures::future::err(Error::NotImplemented))
            .boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        futures::stream::iter([Err(Error::NotImplemented)]).boxed()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        futures::stream::iter([Err(Error::NotImplemented)]).boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }
}

/// Returns `true` if ANY options are set.
fn any_options_set(options: GetOptions) -> bool {
    let GetOptions {
        if_match,
        if_none_match,
        if_modified_since,
        if_unmodified_since,
        range,
        version,
        head,
    } = options;

    if if_match.is_some() {
        return true;
    }

    if if_none_match.is_some() {
        return true;
    }

    if if_modified_since.is_some() {
        return true;
    }

    if if_unmodified_since.is_some() {
        return true;
    }

    if range.is_some() {
        return true;
    }

    if version.is_some() {
        return true;
    }

    if head {
        return true;
    }

    false
}
