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
