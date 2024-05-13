use std::{
    io::IoSlice,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::{channel::oneshot::Sender, ready};
use iox_time::{Time, TimeProvider};
use object_store::path::Path;
use pin_project::{pin_project, pinned_drop};
use tokio::io::AsyncWrite;

/// Able to perform metrics tracking bytes written
/// with object_store::put_multipart.
#[pin_project(PinnedDrop)]
pub(crate) struct AsyncWriterMetricsWrapper {
    /// current path being written
    location: Path,
    /// current inner [`AsyncWrite`]
    inner: Box<dyn AsyncWrite + Unpin + Send>,

    /// calculate end time
    time_provider: Arc<dyn TimeProvider>,

    /// current subtotaled size, regardless of success or failure
    bytes_attempted_so_far: AtomicU64,
    /// current outcome
    outcome: AtomicBool,
    /// action upon complete
    tx: Option<Sender<(bool, Option<u64>, Time)>>,
}

impl AsyncWriterMetricsWrapper {
    pub(crate) fn new(
        location: &Path,
        inner: Box<dyn AsyncWrite + Unpin + Send>,
        time_provider: Arc<dyn TimeProvider>,
        tx: Sender<(bool, Option<u64>, Time)>,
    ) -> Self {
        Self {
            location: location.clone(),
            inner,
            time_provider,
            bytes_attempted_so_far: Default::default(),
            outcome: AtomicBool::new(true),
            tx: Some(tx),
        }
    }
}

#[async_trait]
impl AsyncWrite for AsyncWriterMetricsWrapper {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let attempted_size = buf.len();
        let this = self.project();
        let res = Box::pin(this.inner).as_mut().poll_write(cx, buf);

        match ready!(res) {
            Ok(size) => {
                this.bytes_attempted_so_far
                    .fetch_add(size as u64, Ordering::AcqRel);
                Poll::Ready(Ok(size))
            }
            Err(e) => {
                this.outcome.fetch_and(false, Ordering::AcqRel);
                this.bytes_attempted_so_far
                    .fetch_add(attempted_size as u64, Ordering::AcqRel);
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = self.project();
        let res = Box::pin(this.inner).as_mut().poll_flush(cx);

        match ready!(res) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) => {
                this.outcome.fetch_and(false, Ordering::AcqRel);
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Box::pin(self.project().inner).as_mut().poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        let buf = bufs
            .iter()
            .find(|b| !b.is_empty())
            .map_or(&[][..], |b| &**b);
        self.poll_write(cx, buf)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[pinned_drop]
impl PinnedDrop for AsyncWriterMetricsWrapper {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let outcome = this.outcome.load(Ordering::Acquire);
        let bytes = this.bytes_attempted_so_far.load(Ordering::Acquire);

        if let Some(tx) = this.tx.take() {
            tx.send((outcome, Some(bytes), this.time_provider.now()))
                .expect("should send object store metrics back from async writer");
        }
    }
}
