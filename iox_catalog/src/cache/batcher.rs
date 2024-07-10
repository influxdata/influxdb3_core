use std::{
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Duration,
};

use dashmap::{mapref::entry::Entry, DashMap};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use iox_time::TimeProvider;
use parking_lot::Mutex;

use crate::interface::Error;

type DynDriverGen<M> = dyn Fn(Vec<M>) -> Driver + Send + Sync;
type SharedDriverGen<M> = Arc<DynDriverGen<M>>;

pub(crate) type DriverGen<M> = Box<DynDriverGen<M>>;
pub(crate) type Driver = BoxFuture<'static, Result<(), Error>>;

#[derive(Debug)]
pub(crate) struct Batcher<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    state: Arc<State<K, M>>,
}

impl<K, M> Batcher<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    pub(crate) fn new(time_provider: Arc<dyn TimeProvider>, delay: Duration) -> Self {
        Self {
            state: Arc::new(State {
                time_provider,
                delay,
                pending: DashMap::default(),
            }),
        }
    }

    fn get_pending(&self, key: K, driver_gen: SharedDriverGen<M>) -> Arc<Pending<K, M>> {
        // fast-path
        if let Some(pending) = self.state.pending.get(&key).and_then(|p| p.upgrade()) {
            return pending;
        }

        // slow-path
        let state = Arc::clone(&self.state);
        let metadata_slots = Arc::new(Mutex::new(Some(Vec::<MetadataSlot<M>>::new())));
        let metadata_slots_captured = Arc::clone(&metadata_slots);
        let (rush_tx, rush_rx) = tokio::sync::oneshot::channel();
        let driver = async move {
            tokio::select! {
                _ = state.time_provider.sleep(state.delay) => (),
                _ = rush_rx => (),
            }

            state.pending.remove(&key).expect("still pending");

            let metadata_slots = metadata_slots_captured.lock().take();
            let metadata = metadata_slots
                .expect("only driver")
                .into_iter()
                .filter_map(|MetadataSlot { alive, metadata }| {
                    (alive.strong_count() > 0).then_some(metadata)
                })
                .collect();

            driver_gen(metadata).await
        }
        .boxed()
        .shared();
        let pending = Arc::new(Pending {
            driver,
            metadata_slots,
            rush_tx: Mutex::new(Some(rush_tx)),
            state: Arc::downgrade(&self.state),
            key,
        });
        match self.state.pending.entry(key) {
            Entry::Occupied(mut o) => {
                if let Some(pending_existing) = o.get().upgrade() {
                    // race
                    pending_existing
                } else {
                    // race but then cancelled
                    *o.get_mut() = Arc::downgrade(&pending);
                    pending
                }
            }
            Entry::Vacant(v) => {
                v.insert(Arc::downgrade(&pending));
                pending
            }
        }
    }

    pub(crate) fn batch(&self, key: K, driver_gen: DriverGen<M>, metadata: M) -> Batch<K, M> {
        let metadata_alive = Arc::new(());
        let driver_gen: SharedDriverGen<M> = driver_gen.into();

        // execution of the task is racy, so we need to retry
        loop {
            let pending = self.get_pending(key, Arc::clone(&driver_gen));

            // lock scope
            {
                let mut guard = pending.metadata_slots.lock();
                let Some(slots) = guard.as_mut() else {
                    // task completed before we could enqueue metadata
                    continue;
                };
                slots.push(MetadataSlot {
                    alive: Arc::downgrade(&metadata_alive),
                    metadata,
                });
            }

            let driver = pending.driver.clone();
            return Batch {
                pending,
                driver,
                metadata_alive,
            };
        }
    }
}

#[derive(Debug)]
struct State<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    time_provider: Arc<dyn TimeProvider>,
    delay: Duration,
    pending: DashMap<K, Weak<Pending<K, M>>>,
}

#[derive(Debug)]
struct MetadataSlot<M>
where
    M: Send + 'static,
{
    /// Ref-counter to see if user is still alive
    alive: Weak<()>,
    /// The actual metadata
    metadata: M,
}

#[derive(Debug)]
struct Pending<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    driver: Shared<Driver>,
    metadata_slots: Arc<Mutex<Option<Vec<MetadataSlot<M>>>>>,
    rush_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    state: Weak<State<K, M>>,
    key: K,
}

impl<K, M> Drop for Pending<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            if let Entry::Occupied(o) = state.pending.entry(self.key) {
                if o.get().as_ptr() == self as *const Self {
                    o.remove();
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Batch<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    pending: Arc<Pending<K, M>>,
    // cache driver to avoid excessive `clone` calls and to ensure that wakers aren't lost
    driver: Shared<Driver>,

    /// Keep metadata alive.
    #[allow(dead_code)]
    metadata_alive: Arc<()>,
}

impl<K, M> Batch<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + 'static,
{
    /// Rush this batch and do NOT wait for the delay/timeout.
    ///
    /// You may call this method multiple times, even from multiple links to the same batch.
    pub(crate) fn rush(&self) {
        let rush_tx = {
            let mut guard = self.pending.rush_tx.lock();
            guard.take()
        };

        let Some(rush_tx) = rush_tx else {
            // already rushed
            return;
        };

        // might error if another receiver polled earlier
        rush_tx.send(()).ok();
    }
}

impl<K, M> Future for Batch<K, M>
where
    K: Copy + Eq + Hash + std::fmt::Debug + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.driver.poll_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use iox_time::{MockProvider, Time};

    use crate::cache::test_utils::AssertPendingExt;

    use super::*;

    #[tokio::test]
    async fn test_simple() {
        let Setup {
            batcher,
            time_provider,
            delay,
        } = Setup::new();

        let Driver {
            driver_gen,
            called: called_1a,
        } = Driver::ok();
        let mut batch_1a = batcher.batch(1, driver_gen, "1a");
        batch_1a.assert_pending().await;

        time_provider.inc(delay / 2);

        let Driver {
            driver_gen,
            called: called_2a,
        } = Driver::ok();
        let mut batch_2a = batcher.batch(2, driver_gen, "2a");
        batch_2a.assert_pending().await;

        let Driver {
            driver_gen,
            called: called_1b,
        } = Driver::ok();
        let mut batch_1b = batcher.batch(1, driver_gen, "1b");
        batch_1b.assert_pending().await;

        time_provider.inc(delay / 2);

        batch_1a.await.unwrap();
        batch_2a.assert_pending().await;
        batch_1b.await.unwrap();

        assert_eq!(called_1a.get(), vec!["1a", "1b"]);
        called_2a.assert_not_called();
        called_1b.assert_not_called();
    }

    #[tokio::test]
    async fn test_drop_one_user() {
        let Setup {
            batcher,
            time_provider,
            delay,
        } = Setup::new();

        let Driver {
            driver_gen,
            called: called_1a,
        } = Driver::ok();
        let mut batch_1a = batcher.batch(1, driver_gen, "1a");
        batch_1a.assert_pending().await;

        let Driver {
            driver_gen,
            called: called_1b,
        } = Driver::ok();
        let mut batch_1b = batcher.batch(1, driver_gen, "1b");
        batch_1b.assert_pending().await;
        drop(batch_1b);

        let Driver {
            driver_gen,
            called: called_1c,
        } = Driver::ok();
        let mut batch_1c = batcher.batch(1, driver_gen, "1c");
        batch_1c.assert_pending().await;

        time_provider.inc(delay);

        batch_1a.await.unwrap();
        batch_1c.await.unwrap();

        assert_eq!(called_1a.get(), vec!["1a", "1c"]);
        called_1b.assert_not_called();
        called_1c.assert_not_called();
    }

    #[tokio::test]
    async fn test_drop_all_users() {
        let Setup {
            batcher,
            time_provider,
            delay,
        } = Setup::new();

        let Driver {
            driver_gen,
            called: called_1a,
        } = Driver::ok();
        let mut batch_1a = batcher.batch(1, driver_gen, "1a");
        batch_1a.assert_pending().await;
        drop(batch_1a);

        // make sure we don't leak map entries
        assert!(!batcher.state.pending.contains_key(&1));

        let Driver {
            driver_gen,
            called: called_1b,
        } = Driver::ok();
        let mut batch_1b = batcher.batch(1, driver_gen, "1b");
        batch_1b.assert_pending().await;

        time_provider.inc(delay);

        batch_1b.await.unwrap();

        called_1a.assert_not_called();
        assert_eq!(called_1b.get(), vec!["1b"]);
    }

    #[tokio::test]
    async fn test_rush() {
        let Setup {
            batcher,
            time_provider: _,
            delay: _,
        } = Setup::new();

        let Driver {
            driver_gen,
            called: called_1a,
        } = Driver::ok();
        let mut batch_1a = batcher.batch(1, driver_gen, "1a");
        batch_1a.assert_pending().await;

        let Driver {
            driver_gen,
            called: called_2a,
        } = Driver::ok();
        let mut batch_2a = batcher.batch(2, driver_gen, "2a");
        batch_2a.assert_pending().await;

        let Driver {
            driver_gen,
            called: called_1b,
        } = Driver::ok();
        let batch_1b = batcher.batch(1, driver_gen, "1b");
        batch_1b.rush();

        batch_1a.await.unwrap();
        batch_2a.assert_pending().await;
        batch_1b.await.unwrap();

        assert_eq!(called_1a.get(), vec!["1a", "1b"]);
        called_2a.assert_not_called();
        called_1b.assert_not_called();
    }

    struct Setup {
        batcher: Batcher<u8, &'static str>,
        time_provider: Arc<MockProvider>,
        delay: Duration,
    }

    impl Setup {
        fn new() -> Self {
            let time_provider =
                Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));
            let delay = Duration::from_secs(1);
            let batcher = Batcher::new(Arc::clone(&time_provider) as _, delay);
            Self {
                batcher,
                time_provider,
                delay,
            }
        }
    }

    #[derive(Clone)]
    struct Called(Arc<Mutex<Option<Vec<&'static str>>>>);

    impl Called {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(None)))
        }

        #[track_caller]
        fn set(&self, metadata: Vec<&'static str>) {
            let mut guard = self.0.lock();
            assert!(guard.is_none());
            *guard = Some(metadata);
        }

        #[track_caller]
        fn get(&self) -> Vec<&'static str> {
            self.0.lock().as_ref().expect("called").clone()
        }

        #[track_caller]
        fn assert_not_called(&self) {
            assert!(self.0.lock().is_none());
        }
    }

    struct Driver {
        driver_gen: DriverGen<&'static str>,
        called: Called,
    }

    impl Driver {
        fn ok() -> Self {
            Self::new(Ok(()))
        }

        fn new(res: Result<(), Error>) -> Self {
            let called = Called::new();
            let called_captured = called.clone();
            let driver_gen = Box::new(move |metadata| {
                let called = called_captured.clone();
                let res = res.clone();

                async move {
                    called.set(metadata);

                    res
                }
                .boxed()
            });
            Self { driver_gen, called }
        }
    }
}
