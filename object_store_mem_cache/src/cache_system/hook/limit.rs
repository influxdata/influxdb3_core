//! A memory limiter
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    task::{Context, Poll, Waker},
};

use futures::Stream;
use snafu::Snafu;

use crate::cache_system::{hook::Hook, interfaces::DynError};

/// Error for [`MemoryLimiter`]
#[derive(Debug, Snafu)]
#[allow(missing_docs, missing_copy_implementations)]
pub(crate) enum Error {
    #[snafu(display("Cannot reserve additional {size} bytes for cache containing {current} bytes as would exceed limit of {limit} bytes"))]
    OutOfMemory {
        size: usize,
        current: usize,
        limit: usize,
    },

    #[snafu(display("Cannot reserve additional {size} bytes for cache as request exceeds total memory limit of {limit} bytes"))]
    TooLarge { size: usize, limit: usize },
}

impl From<Error> for DynError {
    fn from(e: Error) -> Self {
        Arc::new(e)
    }
}

/// Result for [`MemoryLimiter`]
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// Memory limiter.
#[derive(Debug)]
pub(crate) struct MemoryLimiter<K>
where
    K: std::fmt::Debug,
{
    current: AtomicUsize,
    limit: usize,
    oom: Arc<OomMailbox>,
    _k: PhantomData<dyn Fn() -> K + Send + Sync + 'static>,
}

impl<K> MemoryLimiter<K>
where
    K: std::fmt::Debug,
{
    /// Create a new [`MemoryLimiter`] limited to `limit` bytes
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            limit,
            oom: Default::default(),
            _k: Default::default(),
        }
    }

    /// Notifier for out-of-memory.
    pub(crate) fn oom(&self) -> OomNotify {
        OomNotify {
            mailbox: Arc::downgrade(&self.oom),
            counter: 0,
        }
    }
}

impl<K> Hook for MemoryLimiter<K>
where
    K: std::fmt::Debug,
{
    type K = K;

    fn fetched(
        &self,
        _gen: u64,
        _k: &Self::K,
        res: &Result<usize, DynError>,
    ) -> Result<(), DynError> {
        let Ok(size) = res else {
            return Ok(());
        };
        let size = *size;

        let limit = self.limit;
        let max = limit
            .checked_sub(size)
            .ok_or(Error::TooLarge { size, limit })?;

        // We can use relaxed ordering as not relying on this to
        // synchronise memory accesses beyond itself
        self.current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                // This cannot overflow as current + size <= limit
                (current <= max).then_some(current + size)
            })
            .map_err(|current| {
                self.oom.notify();

                Error::OutOfMemory {
                    size,
                    current,
                    limit,
                }
            })?;

        Ok(())
    }

    fn evict(&self, _gen: u64, _k: &Self::K, res: &Option<Result<usize, ()>>) {
        if let Some(Ok(size)) = res {
            self.current.fetch_sub(*size, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Default)]
struct OomMailbox {
    counter: AtomicUsize,
    wakers: Mutex<Vec<Waker>>,
}

impl OomMailbox {
    fn notify(&self) {
        let mut guard = self.wakers.lock().expect("not poisoned");

        // bump counter AFTER acquiring lock but before notifying wakers
        self.counter.fetch_add(1, Ordering::SeqCst);

        for waker in guard.drain(..) {
            waker.wake();
        }
    }
}

/// Notification for out-of-memory situations.
///
/// If there was an OOM situation between this and the previous call, this will return immediately, so the caller
/// will never miss an OOM situation.
#[derive(Debug, Clone)]
pub(crate) struct OomNotify {
    mailbox: Weak<OomMailbox>,
    counter: usize,
}

impl Stream for OomNotify {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        let Some(mailbox) = this.mailbox.upgrade() else {
            return Poll::Ready(None);
        };

        let upstream = mailbox.counter.load(Ordering::SeqCst);
        if upstream != this.counter {
            this.counter = upstream;
            Poll::Ready(Some(()))
        } else {
            let mut guard = mailbox.wakers.lock().expect("not poisoned");

            // check upstream again because counter might have changed
            let upstream = mailbox.counter.load(Ordering::SeqCst);
            if upstream != this.counter {
                this.counter = upstream;
                Poll::Ready(Some(()))
            } else {
                guard.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test]
    async fn test_limiter() {
        let limiter = MemoryLimiter::<()>::new(100);
        let oom_counter = OomCounter::new(&limiter);

        limiter.fetched(1, &(), &Ok(20)).unwrap();
        limiter.fetched(2, &(), &Ok(70)).unwrap();
        assert_eq!(oom_counter.get(), 0);

        let err = limiter.fetched(3, &(), &Ok(20)).unwrap_err().to_string();
        assert_eq!(err, "Cannot reserve additional 20 bytes for cache containing 90 bytes as would exceed limit of 100 bytes");
        oom_counter.wait_for(1).await;

        limiter.fetched(4, &(), &Ok(10)).unwrap();
        limiter.fetched(5, &(), &Ok(0)).unwrap();

        let err = limiter.fetched(6, &(), &Ok(1)).unwrap_err().to_string();
        assert_eq!(err, "Cannot reserve additional 1 bytes for cache containing 100 bytes as would exceed limit of 100 bytes");
        oom_counter.wait_for(2).await;

        limiter.evict(7, &(), &Some(Ok(10)));
        limiter.fetched(8, &(), &Ok(10)).unwrap();

        limiter.evict(9, &(), &Some(Ok(100)));

        // Can add single value taking entire range
        limiter.fetched(10, &(), &Ok(100)).unwrap();
        limiter.evict(11, &(), &Some(Ok(100)));

        // Protected against overflow
        let err = limiter
            .fetched(12, &(), &Ok(usize::MAX))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "Cannot reserve additional 18446744073709551615 bytes for cache as request exceeds total memory limit of 100 bytes");
    }

    struct OomCounter {
        _task: JoinSet<()>,
        count: Arc<AtomicUsize>,
    }

    impl OomCounter {
        fn new(limiter: &MemoryLimiter<()>) -> Self {
            let count = Arc::new(AtomicUsize::new(0));
            let count_captured = Arc::clone(&count);

            let mut oom = limiter.oom();

            let mut task = JoinSet::new();
            task.spawn(async move {
                loop {
                    oom.next().await;
                    count_captured.fetch_add(1, Ordering::SeqCst);
                }
            });

            Self { _task: task, count }
        }

        fn get(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }

        async fn wait_for(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if self.get() == expected {
                        return;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }
}
