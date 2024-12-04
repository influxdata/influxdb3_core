use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use futures::{task::ArcWake, FutureExt};

use futures_concurrency::future::FutureExt as _;
use tokio::sync::Barrier;

use crate::cache_system::{
    hook::{
        test_utils::{TestHook, TestHookRecord},
        EvictResult, HookDecision,
    },
    hook_limited::HookLimitedCache,
    s3_fifo_cache::S3FifoCache,
    utils::str_err,
    Cache, CacheState, HasSize,
};

/// The extra size of entries when the S3-FIFO is used.
///
/// This is because the internal bookkeeping of the S3-FIFO implementation is more precise.
const S3_FIFO_EXTRA_SIZE: usize = 56;

/// Statistics about a [`Future`].
pub(crate) struct FutureStats {
    polled: AtomicUsize,
    polled_after_ready: AtomicBool,
    ready: AtomicBool,
    woken: Arc<AtomicUsize>,
    name: &'static str,
}

impl FutureStats {
    /// Number of [polls](Future::poll).
    pub(crate) fn polled(&self) -> usize {
        self.polled.load(Ordering::SeqCst)
    }

    /// Future is [ready](Poll::Ready).
    pub(crate) fn ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// How often the callers [`Waker::wake`](std::task::Waker::wake) was called.
    pub(crate) fn woken(&self) -> usize {
        self.woken.load(Ordering::SeqCst)
    }
}

pub(crate) struct FutureObserver<F>
where
    F: Future,
{
    inner: Pin<Box<F>>,
    stats: Arc<FutureStats>,
}

impl<F> FutureObserver<F>
where
    F: Future,
{
    /// Create new observer.
    pub(crate) fn new(inner: F, name: &'static str) -> Self {
        Self {
            inner: Box::pin(inner),
            stats: Arc::new(FutureStats {
                polled: AtomicUsize::new(0),
                polled_after_ready: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                woken: Arc::new(AtomicUsize::new(0)),
                name,
            }),
        }
    }

    /// Get [statistics](FutureStats) for the contained future.
    pub(crate) fn stats(&self) -> Arc<FutureStats> {
        Arc::clone(&self.stats)
    }
}

impl<F> Future for FutureObserver<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stats.polled.fetch_add(1, Ordering::SeqCst);
        if self.stats.ready() {
            println!("{}: poll after ready", self.stats.name);
            self.stats.polled_after_ready.store(true, Ordering::SeqCst);
        } else {
            println!("{}: poll", self.stats.name);
        }

        let waker = futures::task::waker(Arc::new(WakeObserver {
            inner: cx.waker().clone(),
            woken: Arc::clone(&self.stats.woken),
            name: self.stats.name,
        }));
        let mut cx = Context::from_waker(&waker);

        match self.inner.poll_unpin(&mut cx) {
            Poll::Ready(res) => {
                println!("{}: ready", self.stats.name);
                self.stats.ready.store(true, Ordering::SeqCst);
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct WakeObserver {
    inner: Waker,
    woken: Arc<AtomicUsize>,
    name: &'static str,
}

impl ArcWake for WakeObserver {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        println!("{}: wake", arc_self.name);
        arc_self.woken.fetch_add(1, Ordering::SeqCst);
        arc_self.inner.wake_by_ref();
    }
}

/// Extension for [`Future`] that are helpful for testing.
pub(crate) trait AssertPendingFutureExt {
    /// Ensure that the future is pending.
    async fn assert_pending(&mut self);
}

impl<F> AssertPendingFutureExt for F
where
    F: Future + Send + Unpin,
{
    async fn assert_pending(&mut self) {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            _ = self => {
                panic!("not pending");
            }
        }
    }
}

/// Extension for [`Future`] that are helpful for testing.
pub(crate) trait WithTimeoutFutureExt {
    type Output;

    /// Await future with timeout.
    async fn with_timeout(self) -> Self::Output;
}

impl<F> WithTimeoutFutureExt for F
where
    F: Future + Send,
{
    type Output = F::Output;

    async fn with_timeout(self) -> Self::Output {
        tokio::time::timeout(Duration::from_millis(100), self)
            .await
            .expect("timeout")
    }
}

/// Assert that the result of `f` converges against the given value;
pub(crate) async fn assert_converge_eq<F, T>(f: F, expected: T)
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

pub(crate) async fn test_happy_path(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(move |_k| async move {
            barrier_captured.wait().await;
            Ok(TestValue(test_size))
        }
        .boxed())
    ));
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let ((res, state), _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    if !use_s3fifo {
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(test_size_hook)),
                TestHookRecord::Evict(
                    0,
                    "k1",
                    EvictResult::Fetched {
                        size: test_size_hook
                    }
                )
            ]
        );
    }
}

pub(crate) async fn test_panic_loader(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(|_k| async move {
            barrier_captured.wait().await;
            panic!("foo")
        }
        .boxed())
    ));
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let ((res, state), _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap_err().to_string(), "panic: foo");

    if use_s3fifo {
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("panic: foo".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );

        // data gone
        assert!(cache.get(&"k1").is_none());
    } else {
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

        // data gone
        assert!(cache.get(&"k1").is_none());
    }
}

pub(crate) async fn test_error_path_loader(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(|_k| async move {
            barrier_captured.wait().await;
            Err(str_err("my error"))
        }
        .boxed())
    ));
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let ((res, state), _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap_err().to_string(), "my error");

    if use_s3fifo {
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("my error".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed),
            ]
        );

        // data gone
        assert!(cache.get(&"k1").is_none());
    } else {
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

        // data gone
        assert!(cache.get(&"k1").is_none());
    }
}

#[tokio::test]
async fn test_error_path_loader_and_evict_by_observer() {
    let TestSetup { cache, observer } = TestSetup::get_hook_limited();

    observer.mock_next_fetch(HookDecision::Evict);
    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(|_k| async move {
            barrier_captured.wait().await;
            Err(str_err("my error"))
        }
        .boxed())
    ));
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
        .get_or_fetch(
            &k1,
            Box::new(|_k| async move { Ok(TestValue(1002)) }.boxed()),
        )
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

pub(crate) async fn test_get_keeps_key_alive(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;

    let k1 = Arc::new("k1");
    let (res, state) = cache
        .get_or_fetch(
            &k1,
            Box::new(move |_k| async move { Ok(TestValue(test_size)) }.boxed()),
        )
        .await;
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    let (res, state) = cache
        .get_or_fetch(
            &k1,
            Box::new(move |_k| async move { Ok(TestValue(test_size)) }.boxed()),
        )
        .await;
    assert_eq!(state, CacheState::WasCached);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size)));

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    if !use_s3fifo {
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(test_size_hook)),
                TestHookRecord::Evict(
                    0,
                    "k1",
                    EvictResult::Fetched {
                        size: test_size_hook
                    }
                )
            ]
        );
    }
}

pub(crate) async fn test_already_loading(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let test_size_1 = 1001;
    let test_size_1_hook = test_size_1 + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;
    let test_size_2 = 1002;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let mut fut_1 = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(move |_k| async move {
            barrier_captured.wait().await;
            Ok(TestValue(test_size_1))
        }
        .boxed())
    ));
    fut_1.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let mut fut_2 = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(move |_k| { { async move { Ok(TestValue(test_size_2)) } }.boxed() })
    ));
    fut_2.assert_pending().await;

    let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut_1);
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size_1)));

    let (fut_res, state) = fut_2.await;
    assert_eq!(state, CacheState::AlreadyLoading);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size_1)));

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_1_hook)),
        ]
    );
}

pub(crate) async fn test_drop_while_load_blocked(setup: TestSetup, _use_s3fifo: bool) {
    let TestSetup { cache, observer: _ } = setup;

    let barrier = Arc::new(Barrier::new(2));
    {
        let barrier_captured = Arc::clone(&barrier);
        let k1 = Arc::new("k1");
        let mut fut = std::pin::pin!(cache.get_or_fetch(
            &k1,
            Box::new(move |_k| {
                {
                    let barrier = Arc::clone(&barrier_captured);
                    async move {
                        barrier.wait().await;
                        Ok(TestValue(1001))
                    }
                }
                .boxed()
            })
        ));
        fut.assert_pending().await;
    }

    drop(cache);

    // abort takes a while
    assert_converge_eq(|| Arc::strong_count(&barrier), 1).await;
}

/// Ensure that we don't wake every single consumer for every single IO interaction.
pub(crate) async fn test_perfect_waking_one_consumer(setup: TestSetup, _use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    const N_IO_STEPS: usize = 10;
    let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
    let barriers_captured = Arc::clone(&barriers);
    let k1 = Arc::new("k1");
    let mut fut = cache.get_or_fetch(
        &k1,
        Box::new(|_k| {
            async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
                Ok(TestValue(1001))
            }
            .boxed()
        }),
    );
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
pub(crate) async fn test_perfect_waking_two_consumers(setup: TestSetup, _use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    const N_IO_STEPS: usize = 10;
    let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
    let barriers_captured = Arc::clone(&barriers);
    let k1 = Arc::new("k1");
    let mut fut_1 = cache.get_or_fetch(
        &k1,
        Box::new(|_k| {
            async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
                Ok(TestValue(1001))
            }
            .boxed()
        }),
    );
    fut_1.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let mut fut_2 = cache
        .get_or_fetch(&k1, Box::new(|_k| async move { unreachable!() }.boxed()))
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

#[allow(clippy::async_yields_async)]
pub(crate) fn runtime_shutdown(setup: TestSetup) {
    let TestSetup { cache, observer: _ } = setup;

    let rt_1 = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let cache_captured = &cache;
    let mut fut = rt_1
        .block_on(async move {
            Box::new(async move {
                let k1 = Arc::new("k1");
                cache_captured
                    .get_or_fetch(
                        &k1,
                        Box::new(|_k| {
                            async move {
                                barrier_captured.wait().await;
                                panic!("foo")
                            }
                            .boxed()
                        }),
                    )
                    .await
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

pub(crate) async fn test_get_ok(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;

    // entry does NOT exist yet
    let k1 = Arc::new("k1");
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![],);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(move |_k| async move {
            barrier_captured.wait().await;
            Ok(TestValue(test_size))
        }
        .boxed())
    ));
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    // data is loading but NOT ready yet
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut);
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    // data ready and loaded
    assert_eq!(
        cache.get(&k1).unwrap().unwrap(),
        Arc::new(TestValue(test_size))
    );
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    // test keep alive
    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );
    assert_eq!(
        cache.get(&k1).unwrap().unwrap(),
        Arc::new(TestValue(test_size))
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    if !use_s3fifo {
        cache.prune();
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(test_size_hook)),
                TestHookRecord::Evict(
                    0,
                    "k1",
                    EvictResult::Fetched {
                        size: test_size_hook
                    }
                )
            ]
        );

        // data gone
        assert!(cache.get(&k1).is_none());
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Ok(test_size_hook)),
                TestHookRecord::Evict(
                    0,
                    "k1",
                    EvictResult::Fetched {
                        size: test_size_hook
                    }
                )
            ]
        );
    }
}

pub(crate) async fn test_get_err(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    // entry does NOT exist yet
    let k1 = Arc::new("k1");
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![],);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let mut fut = std::pin::pin!(cache.get_or_fetch(
        &k1,
        Box::new(|_k| async move {
            barrier_captured.wait().await;
            Err(str_err("err"))
        }
        .boxed())
    ));
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    // data is loading but NOT ready yet
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (_, (fut_res, state)) = tokio::join!(barrier.wait(), fut);
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(fut_res.unwrap_err().to_string(), "err");

    if use_s3fifo {
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed),
            ]
        );

        // data gone
        assert!(cache.get(&k1).is_none());
    } else {
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned()))
            ]
        );

        // data ready and loaded
        assert_eq!(cache.get(&k1).unwrap().unwrap_err().to_string(), "err");
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
        assert!(cache.get(&k1).is_none());
        assert_eq!(
            observer.records(),
            vec![
                TestHookRecord::Insert(0, "k1"),
                TestHookRecord::Fetched(0, "k1", Err("err".to_owned())),
                TestHookRecord::Evict(0, "k1", EvictResult::Failed)
            ]
        );
    }
}

pub(crate) async fn test_hook_gen(setup: TestSetup, use_s3fifo: bool) {
    let TestSetup { cache, observer } = setup;

    let test_size_1 = 1001;
    let test_size_2 = 1002;
    let test_size_1_hook = test_size_1 + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;
    let test_size_2_hook = test_size_2 + usize::from(use_s3fifo) * S3_FIFO_EXTRA_SIZE;

    let k1 = Arc::new("k1");
    let k2 = Arc::new("k2");

    let (res, state) = cache
        .get_or_fetch(
            &k1,
            Box::new(move |_k| async move { Ok(TestValue(test_size_1)) }.boxed()),
        )
        .await;
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size_1)));

    let (res, state) = cache
        .get_or_fetch(
            &k2,
            Box::new(move |_k| async move { Ok(TestValue(test_size_2)) }.boxed()),
        )
        .await;
    assert_eq!(state, CacheState::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size_2)));

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_1_hook)),
            TestHookRecord::Insert(1, "k2"),
            TestHookRecord::Fetched(1, "k2", Ok(test_size_2_hook)),
        ]
    );
}

pub(crate) struct TestSetup {
    pub(crate) cache: Arc<dyn Cache<&'static str, TestValue>>,
    pub(crate) observer: Arc<TestHook<&'static str>>,
}

impl TestSetup {
    pub(crate) fn get_hook_limited() -> Self {
        let observer = Arc::new(TestHook::default());
        Self {
            cache: Arc::new(HookLimitedCache::new(Arc::clone(&observer) as _)),
            observer,
        }
    }

    pub(crate) fn get_s3fifo() -> Self {
        let observer = Arc::new(TestHook::default());
        Self {
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TestValue(pub(crate) usize);

impl HasSize for TestValue {
    fn size(&self) -> usize {
        self.0
    }
}

#[macro_export]
macro_rules! gen_cache_tests_impl {
        ($s3fifo:expr, [$($test:ident,)+ $(,)?] $(,)?) => {
            paste::paste! {
                $(
                    #[tokio::test]
                    async fn [<$test _s3fifo _$s3fifo>]() {
                        let setup = if $s3fifo {
                            $crate::cache_system::test_utils::TestSetup::get_s3fifo()
                        } else {
                            $crate::cache_system::test_utils::TestSetup::get_hook_limited()
                        };
                        $crate::cache_system::test_utils::$test(setup, $s3fifo).await;
                    }
                )+
            }
        };
    }

pub(crate) use gen_cache_tests_impl;

macro_rules! gen_cache_tests {
    ($s3fifo:expr) => {
        $crate::cache_system::test_utils::gen_cache_tests_impl!(
            $s3fifo,
            [
                test_happy_path,
                test_panic_loader,
                test_error_path_loader,
                test_get_keeps_key_alive,
                test_already_loading,
                test_drop_while_load_blocked,
                test_get_ok,
                test_get_err,
                test_perfect_waking_two_consumers,
                test_perfect_waking_one_consumer,
                test_hook_gen,
            ],
        );
    };
}
pub(crate) use gen_cache_tests;
