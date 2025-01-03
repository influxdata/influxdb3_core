use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use metric::U64Counter;
use object_store::{
    path::Path, AttributeValue, Attributes, DynObjectStore, Error, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};

use crate::cache_system::{s3_fifo_cache::S3FifoCache, Cache};
use crate::{
    attributes::ATTR_CACHE_STATE,
    cache_system::{
        hook::{chain::HookChain, observer::ObserverHook},
        CacheState, HasSize,
    },
    object_store_helpers::{any_options_set, dyn_error_to_object_store_error},
};

const CACHE_NAME: &str = "object_store";
const STORE_NAME: &str = "mem_cache";

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
    pub memory_limit: NonZeroUsize,

    /// Metric registry for metrics.
    pub metrics: &'a metric::Registry,

    /// The relative size (in percentage) of the "small" S3-FIFO queue.
    pub s3fifo_main_threshold: usize,

    /// Size of S3-FIFO ghost set in bytes.
    pub s3_fifo_ghost_memory_limit: NonZeroUsize,
}

impl MemCacheObjectStoreParams<'_> {
    /// Build store from parameters.
    pub fn build(self) -> MemCacheObjectStore {
        let Self {
            inner,
            memory_limit,
            metrics,
            s3fifo_main_threshold,
            s3_fifo_ghost_memory_limit,
        } = self;

        let cache = Arc::new(S3FifoCache::new(
            memory_limit.get(),
            s3_fifo_ghost_memory_limit.get(),
            s3fifo_main_threshold as f64 / 100.0,
            Arc::new(HookChain::new([Arc::new(ObserverHook::new(
                CACHE_NAME,
                metrics,
                Some(memory_limit.get() as u64),
            )) as _])),
            metrics,
        ));

        MemCacheObjectStore {
            store: inner,
            hit_metrics: HitMetrics::new(metrics),
            cache,
        }
    }
}

#[derive(Debug)]
pub struct MemCacheObjectStore {
    store: Arc<DynObjectStore>,
    hit_metrics: HitMetrics,
    cache: Arc<dyn Cache<Path, CacheValue>>,
}

impl MemCacheObjectStore {
    async fn get_or_fetch(&self, location: &Path) -> Result<(Arc<CacheValue>, CacheState)> {
        let captured_store = Arc::clone(&self.store);
        let (res, state) = self
            .cache
            .get_or_fetch(
                &Arc::new(location.clone()),
                Box::new(|location| {
                    let location = location.clone();

                    async move {
                        CacheValue::fetch(&captured_store, &location)
                            .await
                            .map_err(|e| Arc::new(e) as _)
                    }
                    .boxed()
                }),
            )
            .await;

        match state {
            CacheState::WasCached => &self.hit_metrics.cached,
            CacheState::NewEntry => &self.hit_metrics.miss,
            CacheState::AlreadyLoading => &self.hit_metrics.miss_already_loading,
        }
        .inc(1);

        res.map(|val| (val, state))
            .map_err(|e| dyn_error_to_object_store_error(e, STORE_NAME))
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
        let (v, state) = self.get_or_fetch(location).await?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter([Ok(v.data.clone())]).boxed()),
            meta: v.meta.clone(),
            range: 0..v.data.len(),
            attributes: Attributes::from_iter([(ATTR_CACHE_STATE, AttributeValue::from(state))]),
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // be rather conservative
        if any_options_set(&options) {
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
        let (v, _state) = self.get_or_fetch(location).await?;

        ranges
            .iter()
            .map(|range| {
                if range.end > v.data.len() {
                    return Err(Error::Generic {
                        store: STORE_NAME,
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
                        store: STORE_NAME,
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
        let (v, _state) = self.get_or_fetch(location).await?;

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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use object_store::memory::InMemory;

    use crate::{gen_store_tests, object_store_cache_tests::Setup};

    use super::*;

    struct TestSetup {
        store: Arc<DynObjectStore>,
        inner: Arc<DynObjectStore>,
    }

    impl Setup for TestSetup {
        fn new() -> futures::future::BoxFuture<'static, Self> {
            async move {
                let inner = Arc::new(InMemory::new());

                let store = Arc::new(
                    MemCacheObjectStoreParams {
                        inner: Arc::clone(&inner) as _,
                        memory_limit: NonZeroUsize::MAX,
                        s3_fifo_ghost_memory_limit: NonZeroUsize::MAX,
                        metrics: &metric::Registry::new(),
                        s3fifo_main_threshold: 25,
                    }
                    .build(),
                );

                Self { store, inner }
            }
            .boxed()
        }

        fn inner(&self) -> &Arc<DynObjectStore> {
            &self.inner as _
        }

        fn outer(&self) -> &Arc<DynObjectStore> {
            &self.store
        }
    }

    gen_store_tests!(TestSetup);
}
