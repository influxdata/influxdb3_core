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
use tokio::task::JoinHandle;

use crate::cache_system::{
    hook::Hook,
    interfaces::{ArcResult, DynError, HasSize},
};

pub(crate) type CacheFut<V> = Shared<BoxFuture<'static, ArcResult<V>>>;

/// State that provides more information about [`get_or_fetch`](Cache::get_or_fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CacheState {
    /// Entry was already part of the cache.
    ///
    /// The value may or may not be fetched yet. You may use [`Shared::peek`] to determine that.
    WasCached,

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

/// State of a cached entry.
///
/// Will record an eviction event when dropped.
#[derive(Debug)]
struct CacheEntryState<K> {
    gen: u64,
    key: K,
    hook: Arc<dyn Hook<K = K>>,
    fetch_size: AtomicUsize,
    fetch_status: AtomicU8,
}

impl<K> Drop for CacheEntryState<K> {
    fn drop(&mut self) {
        let fetch_status = FetchStatus::from(self.fetch_status.load(Ordering::SeqCst));
        match fetch_status {
            FetchStatus::NotReady => self.hook.evict(self.gen, &self.key, &None),
            FetchStatus::Success => self.hook.evict(
                self.gen,
                &self.key,
                &Some(Ok(self.fetch_size.load(Ordering::SeqCst))),
            ),
            FetchStatus::Failure => self.hook.evict(self.gen, &self.key, &Some(Err(()))),
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
pub(crate) struct Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    gen_counter: AtomicU64,
    hook: Arc<dyn Hook<K = K>>,
    cache: DashMap<K, CacheEntry<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    /// Create new, empty cache.
    pub(crate) fn new(hook: Arc<dyn Hook<K = K>>) -> Self {
        Self {
            gen_counter: AtomicU64::new(0),
            hook,
            cache: Default::default(),
        }
    }

    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by the users. If you want to make sure that the future makes progress even when nobody is
    /// actively polling it, consider using a [`JoinSet`](tokio::task::JoinSet).
    pub(crate) fn get_or_fetch<F, Fut>(&self, k: &K, f: F) -> (CacheFut<V>, CacheState)
    where
        F: FnOnce(&K) -> Fut,
        Fut: Future<Output = Result<V, DynError>> + Send + 'static,
    {
        // try fast path
        if let Some(entry) = self.cache.get(k) {
            entry.used.store(true, Ordering::SeqCst);
            return (entry.fut.clone(), CacheState::WasCached);
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
        let fut = f(k);
        let fut = async move {
            let fetch_res = fut.await;

            let hook_input = fetch_res
                .as_ref()
                .map(|v| {
                    let size = v.size();
                    state_captured.fetch_size.store(size, Ordering::SeqCst);
                    size
                })
                .map_err(Arc::clone);
            let hook_res =
                state_captured
                    .hook
                    .fetched(state_captured.gen, &state_captured.key, &hook_input);

            let res = match fetch_res {
                Ok(v) => match hook_res {
                    Ok(()) => Ok(v),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };

            match res {
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
        let fut = AbortOnDrop(tokio::task::spawn(fut)).boxed().shared();
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
                (entry.fut.clone(), CacheState::WasCached)
            }
            Entry::Vacant(v) => {
                let gen = cache_entry.state.gen;
                v.insert(cache_entry);
                self.hook.insert(gen, k);
                (fut, CacheState::NewEntry)
            }
        }
    }

    /// get the cached value and return none if was not cached
    pub(crate) async fn get(&self, k: &K) -> Option<CacheFut<V>> {
        self.cache.get(k).map(|entry| {
            entry.used.store(true, Ordering::SeqCst);
            entry.fut.clone()
        })
    }

    /// Get number of entries in the cache.
    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    pub(crate) fn prune(&self) {
        self.cache.retain(|_k, entry| {
            let fetch_status = FetchStatus::from(entry.state.fetch_status.load(Ordering::SeqCst));
            (fetch_status != FetchStatus::Failure) && entry.used.swap(false, Ordering::Relaxed)
        });
    }
}

struct AbortOnDrop<T>(JoinHandle<Result<T, DynError>>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, DynError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => Err(Arc::from(
                Box::<dyn std::error::Error + Send + Sync>::from("Runtime was shut down"),
            )),
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures::stream::{FuturesUnordered, StreamExt};
    use tokio::sync::Barrier;

    use crate::cache_system::{
        hook::test_utils::{TestHook, TestHookRecord},
        test_utils::{str_err, FutureObserver},
    };

    use super::*;

    #[tokio::test]
    async fn test_happy_path() {
        let TestSetup { cache, observer } = TestSetup::default();

        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(fut.await.unwrap(), Arc::new(TestValue(1001)));
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
                TestHookRecord::Evict(0, "k1", Some(Ok(1001)))
            ]
        );
    }

    #[tokio::test]
    async fn test_error_path_loader() {
        let TestSetup { cache, observer } = TestSetup::default();

        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Err(str_err("my error")) }.boxed());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(fut.await.unwrap_err().to_string(), "my error");
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
                TestHookRecord::Evict(0, "k1", Some(Err(())))
            ]
        );
    }

    #[tokio::test]
    async fn test_error_path_observer() {
        let TestSetup { cache, observer } = TestSetup::default();

        observer.mock_next_fetch(Err("other error"));
        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(fut.await.unwrap_err().to_string(), "other error");
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
            ]
        );

        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", Some(Err(()))),
            ]
        );
    }

    #[tokio::test]
    async fn test_error_path_loader_and_observer() {
        let TestSetup { cache, observer } = TestSetup::default();

        observer.mock_next_fetch(Err("other error"));
        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Err(str_err("my error")) }.boxed());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(fut.await.unwrap_err().to_string(), "my error");
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
                TestHookRecord::Evict(0, "k1", Some(Err(())))
            ]
        );
    }

    #[tokio::test]
    async fn test_get_keeps_key_alive() {
        let TestSetup { cache, observer } = TestSetup::default();

        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed());
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(fut.await.unwrap(), Arc::new(TestValue(1001)));
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

        let (fut, state) =
            cache.get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed());
        assert_eq!(state, CacheState::WasCached);
        assert_eq!(
            fut.peek().cloned().unwrap().unwrap(),
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
                TestHookRecord::Evict(0, "k1", Some(Ok(1001)))
            ]
        );
    }

    #[tokio::test]
    async fn test_prune_never_polled() {
        let TestSetup { cache, observer } = TestSetup::default();

        drop(cache.get_or_fetch(&"k1", |_k| async move { Ok(TestValue(1001)) }.boxed()));
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_converge_eq(
            || observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Evict(0, "k1", None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_while_load_blocked() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let (fut, state) = cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        });
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        let (_, fut_res) = tokio::join!(barrier.wait(), fut);
        assert_eq!(fut_res.unwrap(), Arc::new(TestValue(1001)));

        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", Some(Ok(1001)))
            ]
        );
    }

    #[tokio::test]
    async fn test_drop_while_load_blocked() {
        let TestSetup { cache, observer: _ } = TestSetup::default();

        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let (fut, state) = cache.get_or_fetch(&"k1", move |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        });
        assert_eq!(state, CacheState::NewEntry);

        drop(fut);
        drop(cache);

        // abort takes a while
        assert_converge_eq(|| Arc::strong_count(&barrier), 1).await;
    }

    #[tokio::test]
    async fn test_concurrent_key_observation() {
        let TestSetup { cache, observer } = TestSetup::default();

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_1);
        let (fut_1, state) = cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1001))
                }
            }
            .boxed()
        });
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        cache.prune();
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1"),]);

        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier_2);
        let (fut_2, state) = cache.get_or_fetch(&"k1", |_k| {
            {
                let barrier = Arc::clone(&barrier_captured);
                async move {
                    barrier.wait().await;
                    Ok(TestValue(1002))
                }
            }
            .boxed()
        });
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
            ]
        );

        let (_, fut_1_res) = tokio::join!(barrier_1.wait(), fut_1);
        assert_eq!(fut_1_res.unwrap(), Arc::new(TestValue(1001)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", Some(Ok(1001)))
            ]
        );

        let (_, fut_2_res) = tokio::join!(barrier_2.wait(), fut_2);
        assert_eq!(fut_2_res.unwrap(), Arc::new(TestValue(1002)));
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Insert(1, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(1001)),
                TestHookRecord::Evict(0, "k1", Some(Ok(1001))),
                TestHookRecord::Fetched(1, "k1", Ok(1002)),
            ]
        );
    }

    /// Ensure that we don't wake every single consumer for every single IO interaction.
    #[tokio::test]
    async fn test_perfect_waking() {
        let TestSetup { cache, observer } = TestSetup::default();

        const N_IO_STEPS: usize = 10;
        let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
        let barriers_captured = Arc::clone(&barriers);
        let (fut_1, state) = cache.get_or_fetch(&"k1", |_k| {
            async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
                Ok(TestValue(1001))
            }
            .boxed()
        });
        assert_eq!(state, CacheState::NewEntry);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let (fut_2, state) = cache.get_or_fetch(&"k1", |_k| async move { unreachable!() }.boxed());
        assert_eq!(state, CacheState::WasCached);
        assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

        let fut_io = async {
            for barrier in barriers.iter() {
                barrier.wait().await;
            }
        };

        let fut_1 = FutureObserver::new(fut_1);
        let stats_1 = fut_1.stats();
        let fut_2 = FutureObserver::new(fut_2);
        let stats_2 = fut_2.stats();
        let mut fut_1_2 = FuturesUnordered::from_iter([fut_1, fut_2])
            .map(|res| assert_eq!(res.unwrap(), Arc::new(TestValue(1001))))
            .collect::<()>();

        tokio::select! {
            biased;

            _ = &mut fut_1_2 => {}
            _ = fut_io => {}
        }

        fut_1_2.await;

        // polled once for the biased arm to determine that all of them are pending, and then once when we finally got
        // the result
        assert_eq!(stats_1.polled(), 2);
        assert_eq!(stats_2.polled(), 2);
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn test_panic() {
        let TestSetup { cache, observer: _ } = TestSetup::default();

        let (fut, _state) = cache.get_or_fetch(&"k1", |_k| async move { panic!("foo") }.boxed());

        fut.await.ok();
    }

    #[test]
    fn test_runtime_shutdown() {
        let TestSetup { cache, observer: _ } = TestSetup::default();

        let rt_1 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (fut, _state) = rt_1.block_on(async move {
            cache.get_or_fetch(&"k1", |_k| async move { panic!("foo") }.boxed())
        });

        rt_1.shutdown_timeout(Duration::from_secs(1));

        let rt_2 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt_2.block_on(fut).unwrap_err();

        assert_eq!(err.to_string(), "Runtime was shut down");

        rt_2.shutdown_timeout(Duration::from_secs(1));
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

    async fn assert_converge_eq<F, T>(f: F, expected: T)
    where
        F: Fn() -> T + Send,
        T: Eq + std::fmt::Debug + Send,
    {
        let start = Instant::now();

        loop {
            let actual = f();
            if actual == expected {
                return;
            }
            if start.elapsed() > Duration::from_secs(1) {
                assert_eq!(actual, expected);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
