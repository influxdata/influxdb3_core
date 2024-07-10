use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::FutureExt;

use super::interfaces::DynError;

/// Statistics about a [`Future`].
pub(crate) struct FutureStats {
    polled: AtomicUsize,
}

impl FutureStats {
    /// Number of [polls](Future::poll).
    pub(crate) fn polled(&self) -> usize {
        self.polled.load(Ordering::SeqCst)
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
    pub(crate) fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
            stats: Arc::new(FutureStats {
                polled: AtomicUsize::new(0),
            }),
        }
    }

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
        self.inner.poll_unpin(cx)
    }
}

pub(crate) fn str_err(s: &'static str) -> DynError {
    Box::<dyn std::error::Error + Send + Sync>::from(s).into()
}
