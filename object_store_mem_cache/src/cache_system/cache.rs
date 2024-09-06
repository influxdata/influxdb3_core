use std::{
    future::Future,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::{mapref::entry::Entry, DashMap};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};

use crate::cache_system::{
    hook::Hook,
    interfaces::{ArcResult, DynError, HasSize},
};

use super::{
    hook::{EvictResult, HookDecision},
    reactor::reaction::Reaction,
    utils::{CatchUnwindDynErrorExt, TokioTask},
};

type CacheFut<V> = Shared<BoxFuture<'static, ArcResult<V>>>;

/// State that provides more information about [`get_or_fetch`](Cache::get_or_fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    /// Entry was already part of the cache and fully fetched..
    WasCached,

    /// Entry was already part of the cache but did not finish loading.
    AlreadyLoading,

    /// A new entry was created.
    NewEntry,
}

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

/// Return type for [`Cache::get_or_fetch_impl`]
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
struct CacheEntryState<K> {
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
struct CacheEntry<K, V> {
    fut: CacheFut<V>,
    used: AtomicBool,
    state: Arc<CacheEntryState<K>>,
}

/// Cache that maps a key to a failable value future.
#[derive(Debug)]
pub struct Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    gen_counter: AtomicU64,
    hook: Arc<dyn Hook<K>>,
    cache: Arc<DashMap<K, CacheEntry<K, V>>>,
}

impl<K, V> Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    /// Create new, empty cache.
    pub fn new(hook: Arc<dyn Hook<K>>) -> Self {
        Self {
            gen_counter: AtomicU64::new(0),
            hook,
            cache: Default::default(),
        }
    }

    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by a background tokio task and will make progress even when you do not poll the resulting
    /// future.
    pub async fn get_or_fetch<F, Fut>(&self, k: &K, f: F) -> (ArcResult<V>, CacheState)
    where
        F: FnOnce(&K) -> Fut + Send,
        Fut: Future<Output = Result<V, DynError>> + Send + 'static,
    {
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

    /// Get the cached value and return `None` if was not cached.
    ///
    /// Entries that are currently being loaded also result in `None`.
    pub fn get(&self, k: &K) -> Option<ArcResult<V>> {
        self.cache
            .get(k)
            .map(|entry| {
                entry.used.store(true, Ordering::SeqCst);
                entry.fut.clone()
            })
            .and_then(|fut| fut.peek().cloned())
    }

    /// Get number of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Return true if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    pub fn prune(&self) {
        self.cache.retain(|_k, entry| {
            let fetch_status = FetchStatus::from(entry.state.fetch_status.load(Ordering::SeqCst));
            (fetch_status != FetchStatus::Failure) && entry.used.swap(false, Ordering::Relaxed)
        });
    }
}

impl<K, V> Reaction for Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
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
    use std::time::Duration;

    use futures_concurrency::future::FutureExt as _;
    use tokio::sync::Barrier;

    use crate::cache_system::{
        hook::test_utils::{TestHook, TestHookRecord},
        test_utils::{assert_converge_eq, AssertPendingFutureExt, FutureObserver},
        utils::str_err,
    };

    use super::*;

    #[tokio::test]
    async fn test_happy_path() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
            barrier_captured.wait().await;
            Ok(TestValue(1001))
        }
        .boxed()));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let ((res, state), _) = tokio::join!(fut, barrier.wait());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap(), Arc::new(TestValue(1001)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        cache.prune();
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
    async fn test_panic_loader() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
            barrier_captured.wait().await;
            panic!("foo")
        }
        .boxed()));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let ((res, state), _) = tokio::join!(fut, barrier.wait());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap_err().to_string(), "panic: foo");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("panic: foo".to_owned()))
            ]
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("panic: foo".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );
    }

    #[tokio::test]
    async fn test_error_path_loader() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
            barrier_captured.wait().await;
            Err(str_err("my error"))
        }
        .boxed()));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let ((res, state), _) = tokio::join!(fut, barrier.wait());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap_err().to_string(), "my error");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("my error".to_owned()))
            ]
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("my error".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );
    }

    #[tokio::test]
    async fn test_evict_by_observer() {
        let TestSetup { cache, observer } = TestSetup::default();

        observer.mock_next_fetch(HookDecision::Evict);
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
            barrier_captured.wait().await;
            Ok(TestValue(1001))
        }
        .boxed()));
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
            .get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1002)) }.boxed())
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
    async fn test_error_path_loader_and_evict_by_observer() {
        let TestSetup { cache, observer } = TestSetup::default();

        observer.mock_next_fetch(HookDecision::Evict);
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
            barrier_captured.wait().await;
            Err(str_err("my error"))
        }
        .boxed()));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let ((res, state), _) = tokio::join!(fut, barrier.wait());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap_err().to_string(), "my error");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("my error".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed),
            ]
        );

        // entry is gone
        let (_res, state) = cache
            .get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1002)) }.boxed())
            .await;
        assert_eq!(state, CacheState::NewEntry);

        // pruning still works
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("my error".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(1, "k1", Ok(1002)),
            ]
        );
    }

    #[tokio::test]
    async fn test_get_keeps_key_alive() {
        let TestSetup { cache, observer } = TestSetup::default();

        let (res, state) = cache
            .get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed())
            .await;
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap(), Arc::new(TestValue(1001)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        let (res, state) = cache
            .get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed())
            .await;
        assert_eq!(state, CacheState::WasCached);
        assert_eq!(res.unwrap(), Arc::new(TestValue(1001)));

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        cache.prune();
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
    async fn test_already_loading() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut_1 = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        }));
        fut_1.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let mut fut_2 = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            { async move { Ok(TestValue(1002)) } }.boxed()
        }));
        fut_2.assert_pending().await;

        let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut_1);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_res.unwrap(), Arc::new(TestValue(1001)));

        let (fut_res, state) = fut_2.await;
        assert_eq!(state, CacheState::AlreadyLoading);
        assert_eq!(fut_res.unwrap(), Arc::new(TestValue(1001)));

        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
            ]
        );
    }

    #[tokio::test]
    async fn test_prune_never_fully_loaded() {
        let TestSetup { cache, observer } = TestSetup::default();

        {
            let barrier = Arc::new(Barrier::new(2));
            let barrier_captured = Arc::clone(&barrier);
            let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| async move {
                barrier_captured.wait().await;
                Ok(TestValue(1001))
            }
            .boxed()));
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
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        }));
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
    async fn test_drop_while_load_blocked() {
        let TestSetup { cache, observer: _ } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        {
            let barrier_captured = Arc::clone(&barrier);
            let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", move |_k| {
                {
                    let barrier = Arc::clone(&barrier_captured);
                    async move {
                        barrier.wait().await;
                        Ok(TestValue(1001))
                    }
                }
                .boxed()
            }));
            fut.assert_pending().await;
        }

        drop(cache);

        // abort takes a while
        assert_converge_eq(|| Arc::strong_count(&barrier), 1).await;
    }

    #[tokio::test]
    async fn test_concurrent_key_observation() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_1);
        let mut fut_1 = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        }));
        fut_1.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_2);
        let mut fut_2 = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1002))
                }
            }
            .boxed()
        }));
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

    /// Ensure that we don't wake every single consumer for every single IO interaction.
    #[tokio::test]
    async fn test_perfect_waking_one_consumer() {
        let TestSetup { cache, observer } = TestSetup::default();

        const N_IO_STEPS: usize = 10;
        let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
        let barriers_captured = Arc::clone(&barriers);
        let mut fut = cache
            .get_or_fetch(&"k1", |_k| {
                async move {
                    for barrier in barriers_captured.iter() {
                        barrier.wait().await;
                    }
                    Ok(TestValue(1001))
                }
                .boxed()
            })
            .boxed();
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let fut_io = async {
            for barrier in barriers.iter() {
                barrier.wait().await;
            }
        };

        let fut = FutureObserver::new(fut, "fut");
        let stats = fut.stats();
        let fut_io = FutureObserver::new(fut_io, "fut_io");

        // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
        // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
        let ((res, state), ()) = fut.join(fut_io).await;
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(res.unwrap(), Arc::new(TestValue(1001)),);

        // polled once for to determine that all of them are pending, and then once when we finally got
        // the result
        assert_eq!(stats.polled(), 2);

        // it seems that we wake during the final poll (which is unnecessary, because we are about to return `Ready`).
        // Not perfect, but "good enough".
        assert_eq!(stats.woken(), 2);
    }

    /// Ensure that we don't wake every single consumer for every single IO interaction.
    #[tokio::test]
    async fn test_perfect_waking_two_consumers() {
        let TestSetup { cache, observer } = TestSetup::default();

        const N_IO_STEPS: usize = 10;
        let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
        let barriers_captured = Arc::clone(&barriers);
        let mut fut_1 = cache
            .get_or_fetch(&"k1", |_k| {
                async move {
                    for barrier in barriers_captured.iter() {
                        barrier.wait().await;
                    }
                    Ok(TestValue(1001))
                }
                .boxed()
            })
            .boxed();
        fut_1.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let mut fut_2 = cache
            .get_or_fetch(&"k1", |_k| async move { unreachable!() }.boxed())
            .boxed();
        fut_2.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let fut_io = async {
            for barrier in barriers.iter() {
                barrier.wait().await;
            }
        };

        let fut_1 = FutureObserver::new(fut_1, "fut_1");
        let stats_1 = fut_1.stats();
        let fut_2 = FutureObserver::new(fut_2, "fut_2");
        let stats_2 = fut_2.stats();
        let fut_io = FutureObserver::new(fut_io, "fut_io");

        // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
        // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
        let (((res_1, state_1), (res_2, state_2)), ()) = fut_1.join(fut_2).join(fut_io).await;
        assert_eq!(state_1, CacheState::NewEntry);
        assert_eq!(state_2, CacheState::AlreadyLoading);
        assert_eq!(res_1.unwrap(), Arc::new(TestValue(1001)),);
        assert_eq!(res_2.unwrap(), Arc::new(TestValue(1001)),);

        // polled once for to determine that all of them are pending, and then once when we finally got
        // the result
        assert_eq!(stats_1.polled(), 2);
        assert_eq!(stats_2.polled(), 2);

        // It seems that we wake during the final poll of the first future (which is unnecessary, because we are about to return `Ready`).
        // Not perfect, but "good enough".
        assert_eq!(stats_1.woken(), 2);
        assert_eq!(stats_2.woken(), 1);
    }

    #[test]
    #[allow(clippy::async_yields_async)]
    fn test_runtime_shutdown() {
        let TestSetup { cache, observer: _ } = TestSetup::default();

        let rt_1 = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let cache_captured = &cache;
        let mut fut = rt_1
            .block_on(async move {
                cache_captured.get_or_fetch(&"k1", |_k| {
                    async move {
                        barrier_captured.wait().await;
                        panic!("foo")
                    }
                    .boxed()
                })
            })
            .boxed();
        rt_1.block_on(async {
            fut.assert_pending().await;
        });

        rt_1.shutdown_timeout(Duration::from_secs(1));

        let rt_2 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt_2
            .block_on(async move {
                let ((res, _), _) = tokio::join!(fut, barrier.wait());
                res
            })
            .unwrap_err();

        assert_eq!(err.to_string(), "Runtime was shut down");

        rt_2.shutdown_timeout(Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_get_ok() {
        let TestSetup { cache, observer } = TestSetup::default();

        // entry does NOT exist yet
        assert!(cache.get(&"k1").is_none());
        assert_eq!(observer.records(), vec![],);

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        }));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        // data is loading but NOT ready yet
        assert!(cache.get(&"k1").is_none());
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_res.unwrap(), Arc::new(TestValue(1001)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        // data ready and loaded
        assert_eq!(
            cache.get(&"k1").unwrap().unwrap(),
            Arc::new(TestValue(1001))
        );
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );

        // test keep alive
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );
        assert_eq!(
            cache.get(&"k1").unwrap().unwrap(),
            Arc::new(TestValue(1001))
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001))
            ]
        );
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", EvictResult::Fetched { size: 1001 })
            ]
        );

        // data gone
        assert!(cache.get(&"k1").is_none());
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
    async fn test_get_err() {
        let TestSetup { cache, observer } = TestSetup::default();

        // entry does NOT exist yet
        assert!(cache.get(&"k1").is_none());
        assert_eq!(observer.records(), vec![],);

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = std::pin::pin!(cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Err(str_err("err"))
                }
            }
            .boxed()
        }));
        fut.assert_pending().await;
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        // data is loading but NOT ready yet
        assert!(cache.get(&"k1").is_none());
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut);
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut_res.unwrap_err().to_string(), "err");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned()))
            ]
        );

        // data ready and loaded
        assert_eq!(cache.get(&"k1").unwrap().unwrap_err().to_string(), "err");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned()))
            ]
        );

        // errors/failed entries are NOT kept alive
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );

        // data gone
        assert!(cache.get(&"k1").is_none());
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );
    }

    struct TestSetup {
        cache: Cache<&'static str, TestValue>,
        observer: Arc<TestHook<&'static str>>,
    }

    impl Default for TestSetup {
        fn default() -> Self {
            let observer = Arc::new(TestHook::default());
            Self {
                cache: Cache::new(Arc::clone(&observer) as _),
                observer,
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TestValue(usize);

    impl HasSize for TestValue {
        fn size(&self) -> usize {
            self.0
        }
    }
}
