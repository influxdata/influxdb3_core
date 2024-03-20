use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::fmt::Debug;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self
            .value
            .fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
        self.max
            .fetch_max(old + amount, std::sync::atomic::Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value
            .fetch_sub(amount, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> crate::exec::context::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }
}
