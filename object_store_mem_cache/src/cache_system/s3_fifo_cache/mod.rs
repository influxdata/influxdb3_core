use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};
use object_store_metrics::cache_state::CacheState;
use std::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

// for benchmarks
pub use s3_fifo::{S3Config, S3Fifo};

use super::{
    ArcResult, Cache, CacheFn, DynError, HasSize,
    hook::{EvictResult, Hook},
    utils::{CatchUnwindDynErrorExt, TokioTask},
};

mod fifo;
mod ordered_set;
mod s3_fifo;

type CacheFut<V> = Shared<BoxFuture<'static, Result<(Arc<V>, CacheState), DynError>>>;

enum S3CacheResponse<V> {
    NewEntry(CacheFut<V>),
    AlreadyLoading(CacheFut<V>),
    WasCached(ArcResult<V>),
}

#[derive(Debug)]
pub struct S3FifoCache<K, V>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    cache: Arc<S3Fifo<K, V>>,
    gen_counter: AtomicU64,
    unresolved_futures: Arc<DashMap<Arc<K>, CacheFut<V>>>,
    hook: Arc<dyn Hook<K>>,
}

impl<K, V> S3FifoCache<K, V>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    pub fn new(
        max_memory_size: usize,
        max_ghost_memory_size: usize,
        move_to_main_threshold: f64,
        hook: Arc<dyn Hook<K>>,
        metric_registry: &metric::Registry,
    ) -> Self {
        Self {
            cache: Arc::new(S3Fifo::new(
                S3Config {
                    max_memory_size,
                    max_ghost_memory_size,
                    move_to_main_threshold,
                    hook: Arc::clone(&hook),
                },
                metric_registry,
            )),
            gen_counter: AtomicU64::new(0),
            unresolved_futures: Arc::new(DashMap::default()),
            hook,
        }
    }

    fn get_or_fetch_impl<F, Fut>(&self, k: &K, f: F) -> S3CacheResponse<V>
    where
        F: FnOnce(&K) -> Fut,
        Fut: Future<Output = Result<V, DynError>> + Send + 'static,
    {
        // fast path
        if let Some(entry) = self.cache.get(k) {
            return S3CacheResponse::WasCached(Ok(Arc::clone(entry.value())));
        }

        // slow path
        let generation = self.gen_counter.fetch_add(1, Ordering::Relaxed);
        let k = Arc::new(k.clone());
        let fut = f(&k);
        let hook_captured = Arc::clone(&self.hook);
        let cache_captured = Arc::downgrade(&self.cache);
        let unresolved_futures_captured = Arc::downgrade(&self.unresolved_futures);
        let k_captured = Arc::clone(&k);
        let fut = async move {
            // check cache first because between the "fast path" and inserting the future in the "slow path", a very
            // fast loader might have been succeeded
            {
                if let Some(cache) = cache_captured.upgrade() {
                    match cache.get(&k_captured) {
                        None => {}
                        Some(v) => {
                            return Ok((Arc::clone(v.value()), CacheState::AlreadyLoading));
                        }
                    }
                }
            }

            // now we can actually inform the hook
            hook_captured.insert(generation, &k_captured);

            // run actual future
            let fetch_res = fut.catch_unwind_dyn_error().await.map(Arc::new);

            // store results
            match &fetch_res {
                Ok(v) => {
                    if let Some(cache) = cache_captured.upgrade() {
                        // NOTES:
                        // - Don't involve hook here because the cache is doing that for us correctly, even if the key is
                        //   already stored.
                        // - Tell tokio that this is potentially expensive. This is due to the fact that inserting new values
                        //   may free existing ones and the relevant allocator accounting can be rather pricey.
                        let k = Arc::clone(&k_captured);
                        let v = Arc::clone(v);
                        tokio::task::spawn_blocking(move || {
                            cache.get_or_put(k, v, generation);
                        })
                        .await
                        .expect("never fails");
                    } else {
                        // notify "fetched" and instantly evict, because underlying cache is gone (during shutdown)
                        let size = v.size();
                        hook_captured.fetched(generation, &k_captured, Ok(size));
                        hook_captured.evict(generation, &k_captured, EvictResult::Fetched { size });
                    }
                }
                Err(e) => {
                    hook_captured.fetched(generation, &k_captured, Err(e));
                    hook_captured.evict(generation, &k_captured, EvictResult::Failed);
                }
            }

            // forget this very future
            // NOTE: do this AFTER storing the result!
            if let Some(unresolved_futures) = unresolved_futures_captured.upgrade() {
                unresolved_futures.remove(&k_captured);
            }

            // return to the caller(s) (potentially multiple that wait for this future)
            fetch_res.map(|v| (v, CacheState::NewEntry))
        };

        match self.unresolved_futures.entry(Arc::clone(&k)) {
            dashmap::Entry::Occupied(o) => {
                // race, entry was created in the meantime, this is fine, just use the existing one
                S3CacheResponse::AlreadyLoading(o.get().clone())
            }
            dashmap::Entry::Vacant(v) => {
                // the entry wasn't know yet
                //
                // Note that it might be within the actual cache in the meantime, so we have to check for that
                // within the future again to prevent needless loading.
                let fut = TokioTask::spawn(fut).boxed().shared();
                v.insert(fut.clone());
                S3CacheResponse::NewEntry(fut)
            }
        }
    }
}

#[async_trait]
impl<K, V> Cache<K, V> for S3FifoCache<K, V>
where
    K: Clone + Debug + Eq + Hash + HasSize + Send + Sync + 'static,
    V: Debug + HasSize + Send + Sync + 'static,
{
    async fn get_or_fetch(&self, k: &K, f: CacheFn<K, V>) -> (ArcResult<V>, CacheState) {
        match self.get_or_fetch_impl(k, f) {
            S3CacheResponse::NewEntry(fut) => {
                let res = fut.await;

                // Due to the lock gap between the "unresolved futures" and the cache, the future may determine that
                // data was already cached/loading. We try to read the from the inner state.
                match res {
                    Ok((v, state)) => (Ok(v), state),
                    Err(e) => (Err(e), CacheState::NewEntry),
                }
            }
            S3CacheResponse::AlreadyLoading(fut) => {
                let res = fut.await;

                // we know it was "already loading", so the inner state has lower priority
                let res = res.map(|(v, _state)| v);

                (res, CacheState::AlreadyLoading)
            }
            S3CacheResponse::WasCached(res) => (res, CacheState::WasCached),
        }
    }

    fn get(&self, k: &K) -> Option<ArcResult<V>> {
        self.cache.get(k).map(|entry| Ok(Arc::clone(entry.value())))
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    fn prune(&self) {
        // Intentionally unimplemented, S3Fifo handles its own pruning
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cache_system::{
        hook::test_utils::{NoOpHook, TestHook, TestHookRecord},
        test_utils::{TestSetup, TestValue, gen_cache_tests, runtime_shutdown},
    };

    #[test]
    fn test_runtime_shutdown() {
        runtime_shutdown(setup());
    }

    #[tokio::test]
    async fn test_ghost_set_is_limited() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>>::new(
            10,
            10,
            0.1,
            Arc::new(NoOpHook::default()),
            &metric::Registry::new(),
        );
        let k1 = Arc::from("x");

        let (res, state) = cache
            .get_or_fetch(
                &k1,
                Box::new(|_| futures::future::ready(Ok(Arc::from("value"))).boxed()),
            )
            .await;
        assert_eq!(state, CacheState::NewEntry);
        res.unwrap();

        assert_ne!(Arc::strong_count(&k1), 1);

        for i in 0..100 {
            let k = Arc::from(i.to_string());
            let (res, state) = cache
                .get_or_fetch(
                    &k,
                    Box::new(|_| futures::future::ready(Ok(Arc::from("value"))).boxed()),
                )
                .await;
            assert_eq!(state, CacheState::NewEntry);
            res.unwrap();
        }

        assert_eq!(Arc::strong_count(&k1), 1);
    }

    #[tokio::test]
    async fn test_evict_previously_heavy_used_key() {
        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, TestValue>::new(
            10,
            10,
            0.5,
            Arc::clone(&hook) as _,
            &metric::Registry::new(),
        );

        let k_heavy = Arc::from("heavy");

        // make it heavy
        for _ in 0..2 {
            let (res, _state) = cache
                .get_or_fetch(
                    &k_heavy,
                    Box::new(|_| futures::future::ready(Ok(TestValue(5))).boxed()),
                )
                .await;
            res.unwrap();
        }

        // add new keys
        let k_new = Arc::from("new");

        // make them heavy enough to evict old data
        for _ in 0..2 {
            let (res, _state) = cache
                .get_or_fetch(
                    &k_new,
                    Box::new(|_| futures::future::ready(Ok(TestValue(5))).boxed()),
                )
                .await;
            res.unwrap();
        }

        // old heavy key is gone
        assert!(cache.get(&k_heavy).is_none());

        assert_eq!(
            hook.records(),
            vec![
                TestHookRecord::Insert(0, Arc::clone(&k_heavy)),
                TestHookRecord::Fetched(0, Arc::clone(&k_heavy), Ok(71)),
                TestHookRecord::Insert(1, Arc::clone(&k_new)),
                TestHookRecord::Fetched(1, Arc::clone(&k_new), Ok(67)),
                TestHookRecord::Evict(0, Arc::clone(&k_heavy), EvictResult::Fetched { size: 71 }),
            ],
        );
    }

    gen_cache_tests!(setup);

    fn setup() -> TestSetup {
        let observer = Arc::new(TestHook::default());
        TestSetup {
            cache: Arc::new(S3FifoCache::new(
                10_000,
                10_000,
                0.25,
                Arc::clone(&observer) as _,
                &metric::Registry::new(),
            )),
            observer,
        }
    }
}
