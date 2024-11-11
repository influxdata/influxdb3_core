use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::{future::BoxFuture, FutureExt};
use std::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use super::{
    hook::{EvictResult, Hook, HookDecision},
    reactor::reaction::Reaction,
    utils::{CatchUnwindDynErrorExt, TokioTask},
    ArcResult, Cache, CacheFn, CacheFut, CacheState, DynError, HasSize,
};

/// Result of future that fetched a value.
///
/// Can be converted from/to [`u8`] so we can stuff that into a [`AtomicU8`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchStatus {
    NotReady,
    Success,
    Failure,
}

impl From<FetchStatus> for u8 {
    fn from(status: FetchStatus) -> Self {
        match status {
            FetchStatus::NotReady => 0,
            FetchStatus::Success => 1,
            FetchStatus::Failure => 2,
        }
    }
}

impl From<u8> for FetchStatus {
    fn from(status: u8) -> Self {
        match status {
            0 => Self::NotReady,
            1 => Self::Success,
            2 => Self::Failure,
            _ => unreachable!(),
        }
    }
}

/// Return type for [`HookLimitedCache::get_or_fetch_impl`]
enum GetOrFetchRes<V> {
    /// New entry.
    New(CacheFut<V>),

    /// Known entry, may or may not be loaded.
    Known(CacheFut<V>),
}

/// State of a cached entry.
///
/// Will record an eviction event when dropped.
#[derive(Debug)]
pub struct CacheEntryState<K> {
    gen: u64,
    key: K,
    hook: Arc<dyn Hook<K>>,
    fetch_size: AtomicUsize,
    fetch_status: AtomicU8,
}

impl<K> Drop for CacheEntryState<K> {
    fn drop(&mut self) {
        let fetch_status = FetchStatus::from(self.fetch_status.load(Ordering::SeqCst));
        match fetch_status {
            FetchStatus::NotReady => self.hook.evict(self.gen, &self.key, EvictResult::Unfetched),
            FetchStatus::Success => self.hook.evict(
                self.gen,
                &self.key,
                EvictResult::Fetched {
                    size: self.fetch_size.load(Ordering::SeqCst),
                },
            ),
            FetchStatus::Failure => self.hook.evict(self.gen, &self.key, EvictResult::Failed),
        }
    }
}

#[derive(Debug)]
pub struct CacheEntry<K, V> {
    fut: CacheFut<V>,
    used: AtomicBool,
    state: Arc<CacheEntryState<K>>,
}

/// Cache that maps a key to a failable value future.
#[derive(Debug)]
pub struct HookLimitedCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    gen_counter: AtomicU64,
    hook: Arc<dyn Hook<K>>,
    cache: Arc<DashMap<K, CacheEntry<K, V>>>,
}

#[async_trait]
impl<K, V> Cache<K, V> for HookLimitedCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by a background tokio task and will make progress even when you do not poll the resulting
    /// future.
    async fn get_or_fetch(&self, k: &K, f: CacheFn<K, V>) -> (ArcResult<V>, CacheState) {
        match self.get_or_fetch_impl(k, f) {
            GetOrFetchRes::New(fut) => {
                let res = fut.await;
                (res, CacheState::NewEntry)
            }
            GetOrFetchRes::Known(fut) => match fut.peek() {
                None => {
                    let res = fut.await;
                    (res, CacheState::AlreadyLoading)
                }
                Some(res) => (res.clone(), CacheState::WasCached),
            },
        }
    }

    /// Get the cached value and return `None` if was not cached.
    ///
    /// Entries that are currently being loaded also result in `None`.
    fn get(&self, k: &K) -> Option<ArcResult<V>> {
        self.cache
            .get(k)
            .map(|entry| {
                entry.used.store(true, Ordering::SeqCst);
                entry.fut.clone()
            })
            .and_then(|fut| fut.peek().cloned())
    }

    /// Get number of entries in the cache.
    fn len(&self) -> usize {
        self.cache.len()
    }

    /// Return true if the cache is empty
    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    fn prune(&self) {
        self.cache.retain(|_k, entry| {
            let fetch_status = FetchStatus::from(entry.state.fetch_status.load(Ordering::SeqCst));
            (fetch_status != FetchStatus::Failure) && entry.used.swap(false, Ordering::Relaxed)
        });
    }
}

impl<K, V> HookLimitedCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    /// Create new, empty cache.
    pub fn new(hook: Arc<dyn Hook<K>>) -> Self {
        Self {
            gen_counter: AtomicU64::new(0),
            hook,
            cache: Default::default(),
        }
    }

    fn get_or_fetch_impl<F, Fut>(&self, k: &K, f: F) -> GetOrFetchRes<V>
    where
        F: FnOnce(&K) -> Fut,
        Fut: Future<Output = Result<V, DynError>> + Send + 'static,
    {
        // try fast path
        if let Some(entry) = self.cache.get(k) {
            entry.used.store(true, Ordering::SeqCst);
            return GetOrFetchRes::Known(entry.fut.clone());
        };

        // slow path
        // set up cache entry BEFORE acquiring the map entry
        let state = Arc::new(CacheEntryState {
            gen: self.gen_counter.fetch_add(1, Ordering::Relaxed),
            key: k.clone(),
            hook: Arc::clone(&self.hook),
            fetch_size: AtomicUsize::new(0),
            fetch_status: AtomicU8::new(FetchStatus::NotReady.into()),
        });
        let state_captured = Arc::clone(&state);
        let cache_captured = Arc::downgrade(&self.cache);
        let fut = f(k);
        let fut = async move {
            let fetch_res = fut.catch_unwind_dyn_error().await;

            let hook_input = fetch_res
                .as_ref()
                .map(|v| {
                    let size = v.size();
                    state_captured.fetch_size.store(size, Ordering::SeqCst);
                    size
                })
                .map_err(Arc::clone);
            let hook_decision = state_captured.hook.fetched(
                state_captured.gen,
                &state_captured.key,
                hook_input.as_ref().map(|size| *size),
            );
            match hook_decision {
                HookDecision::Keep => {}
                HookDecision::Evict => {
                    if let Some(cache) = cache_captured.upgrade() {
                        cache.remove_if(&state_captured.key, |_key, entry| {
                            entry.state.gen == state_captured.gen
                        });
                    }
                }
            }

            match fetch_res {
                Ok(v) => {
                    state_captured
                        .fetch_status
                        .store(FetchStatus::Success.into(), Ordering::SeqCst);
                    Ok(Arc::new(v))
                }
                Err(e) => {
                    state_captured
                        .fetch_status
                        .store(FetchStatus::Failure.into(), Ordering::SeqCst);
                    Err(e)
                }
            }
        };
        let fut = TokioTask::spawn(fut).boxed().shared();
        let cache_entry = CacheEntry {
            fut: fut.clone(),
            used: AtomicBool::new(true),
            state,
        };
        match self.cache.entry(k.clone()) {
            Entry::Occupied(o) => {
                // race, entry was created in the meantime, this is fine, just use the existing one
                let entry = o.get();
                entry.used.store(true, Ordering::Relaxed);
                GetOrFetchRes::Known(entry.fut.clone())
            }
            Entry::Vacant(v) => {
                let gen = cache_entry.state.gen;
                v.insert(cache_entry);
                self.hook.insert(gen, k);
                GetOrFetchRes::New(fut)
            }
        }
    }
}

impl<K, V> Reaction for HookLimitedCache<K, V>
where
    K: Debug + Clone + Eq + Hash + Send + Sync + 'static,
    V: Debug + HasSize + Send + Sync + 'static,
{
    fn exec(&self) -> BoxFuture<'_, Result<(), DynError>> {
        Box::pin(async {
            self.prune();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use tokio::sync::Barrier;

    use super::*;
    use crate::cache_system::{
        hook::test_utils::TestHookRecord,
        test_utils::{
            assert_converge_eq, gen_cache_tests, runtime_shutdown, AssertPendingFutureExt,
            TestSetup, TestValue,
        },
    };

    #[tokio::test]
    async fn test_evict_by_observer() {
        let TestSetup { cache, observer } = TestSetup::get_hook_limited();

        observer.mock_next_fetch(HookDecision::Evict);
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(
            &"k1",
            Box::new(|_k| async move {
                barrier_captured.wait().await;
                Ok(TestValue(1001))
            }
            .boxed())
        ));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let ((res, state), _) = tokio::join!(fut, barrier.wait());
        assert_eq!(state, CacheState::NewEntry);
        res.unwrap();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 }),
            ]
        );

        // entry is gone
        let (_res, state) = cache
            .get_or_fetch(
                &"k1",
                Box::new(|_k| async move { Ok(TestValue(1002)) }.boxed()),
            )
            .await;
        assert_eq!(state, CacheState::NewEntry);

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 }),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(1, "k1", Ok(1002)),
            ]
        );
    }

    #[tokio::test]
    async fn test_prune_never_fully_loaded() {
        let TestSetup { cache, observer } = TestSetup::get_hook_limited();

        {
            let barrier = Arc::new(Barrier::new(2));
            let barrier_captured = Arc::clone(&barrier);
            let mut fut = std::pin::pin!(cache.get_or_fetch(
                &"k1",
                Box::new(|_k| async move {
                    barrier_captured.wait().await;
                    Ok(TestValue(1001))
                }
                .boxed())
            ));
            fut.assert_pending().await;
            assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);
        }

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_converge_eq(
            || observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Evict(0, "k1", EvictResult::Unfetched),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_while_load_blocked() {
        let TestSetup { cache, observer } = TestSetup::get_hook_limited();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(
            &"k1",
            Box::new(|_k| async move {
                barrier_captured.wait().await;
                Ok(TestValue(1001))
            }
            .boxed())
        ));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_res.unwrap(), Arc::new(TestValue(1001)));

        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 })
            ]
        );
    }

    #[tokio::test]
    async fn test_concurrent_key_observation() {
        let TestSetup { cache, observer } = TestSetup::get_hook_limited();

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_1);
        let mut fut_1 = std::pin::pin!(cache.get_or_fetch(
            &"k1",
            Box::new(|_k| async move {
                barrier_captured.wait().await;
                Ok(TestValue(1001))
            }
            .boxed())
        ));
        fut_1.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_2);
        let mut fut_2 = std::pin::pin!(cache.get_or_fetch(
            &"k1",
            Box::new(|_k| async move {
                barrier_captured.wait().await;
                Ok(TestValue(1002))
            }
            .boxed())
        ));
        fut_2.assert_pending().await;
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
            ]
        );

        let (_, (fut_1_res, state)) = tokio::join!(barrier_1.wait(), fut_1);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_1_res.unwrap(), Arc::new(TestValue(1001)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 })
            ]
        );

        let (_, (fut_2_res, state)) = tokio::join!(barrier_2.wait(), fut_2);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_2_res.unwrap(), Arc::new(TestValue(1002)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 }),
                TestHookRecord::Fetched(1, "k1", Ok(1002)),
            ]
        );
    }

    #[test]
    fn test_runtime_shutdown() {
        runtime_shutdown(TestSetup::get_hook_limited());
    }

    gen_cache_tests!(false);
}
