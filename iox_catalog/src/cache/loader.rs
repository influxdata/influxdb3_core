use futures::future::Shared;
use futures::{ready, FutureExt};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use parking_lot::Mutex;
use snafu::Snafu;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

use crate::interface::Error as CatalogError;

#[derive(Debug, Snafu, Clone)]
pub(crate) enum Error {
    #[snafu(display("Loader catalog error: {source}"))]
    Catalog { source: Arc<CatalogError> },

    #[snafu(display("Loader join error: {desc}"))]
    Join { desc: String },
}

impl From<Error> for CatalogError {
    fn from(err: Error) -> Self {
        match &err {
            Error::Catalog { source } => match source.as_ref() {
                Self::External { .. } => Self::External {
                    source: Box::new(err),
                },
                Self::AlreadyExists { descr } => Self::AlreadyExists {
                    descr: descr.clone(),
                },
                Self::LimitExceeded { descr } => Self::LimitExceeded {
                    descr: descr.clone(),
                },
                Self::NotFound { descr } => Self::NotFound {
                    descr: descr.clone(),
                },
                Self::Malformed { descr } => Self::Malformed {
                    descr: descr.clone(),
                },
            },
            Error::Join { .. } => Self::External {
                source: Box::new(err),
            },
        }
    }
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

type LoaderState<K, V> = Mutex<HashMap<K, Weak<LoaderTask<K, V>>>>;

/// Spawns tasks to tokio and allows multiple tasks to wait on their results
#[derive(Debug)]
pub(crate) struct Loader<K: Hash + Eq + Copy, V: Clone + Send> {
    state: Arc<LoaderState<K, V>>,
}

impl<K: Hash + Eq + Copy, V: Clone + Send> Default for Loader<K, V> {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::with_capacity(1024))),
        }
    }
}

impl<K: Hash + Eq + Copy, V: Clone + Send + 'static> Loader<K, V> {
    /// Load a new value
    ///
    /// If there is no in-progress load with the same `key`, will spawn `fut` and await its completion
    /// Otherwise, it will await the completion of the in-progress task
    pub(crate) fn load<Fut>(&self, key: K, fut: Fut) -> LoaderFuture<K, V>
    where
        Fut: Future<Output = Result<V, CatalogError>> + Send + 'static,
    {
        let task = {
            let mut guard = self.state.lock();
            match guard.entry(key) {
                Entry::Occupied(mut o) => match o.get().upgrade() {
                    Some(x) => x,
                    None => {
                        let task = Arc::new(LoaderTask::new(key, Arc::downgrade(&self.state), fut));
                        o.insert(Arc::downgrade(&task));
                        task
                    }
                },
                Entry::Vacant(v) => {
                    let task = Arc::new(LoaderTask::new(key, Arc::downgrade(&self.state), fut));
                    v.insert(Arc::downgrade(&task));
                    task
                }
            }
        };
        task.join()
    }

    /// Load a new value, preempting any in-progress load
    ///
    /// Invoke `fut`, and ensure any subsequent calls to [`Self::load`] will await this result
    pub(crate) fn refresh<Fut>(&self, key: K, fut: Fut) -> LoaderFuture<K, V>
    where
        Fut: Future<Output = Result<V, CatalogError>> + Send + 'static,
    {
        let task = Arc::new(LoaderTask::new(key, Arc::downgrade(&self.state), fut));
        self.state.lock().insert(key, Arc::downgrade(&task));
        task.join()
    }
}

/// Future returned by [`Loader`]
pub(crate) struct LoaderFuture<K: Hash + Eq + Copy, V: Clone + Send> {
    _task: Arc<LoaderTask<K, V>>,
    fut: Shared<TaskFuture<V>>,
}

impl<K: Hash + Eq + Copy, V: Clone + Send> Future for LoaderFuture<K, V> {
    type Output = Result<V>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

/// A task tracked by [`Loader`] that unregisters itself on [`Drop`]
#[derive(Debug)]
struct LoaderTask<K: Hash + Eq + Copy, V: Clone + Send> {
    key: K,
    state: Weak<LoaderState<K, V>>,
    recv: Shared<TaskFuture<V>>,
}

impl<K: Hash + Eq + Copy, V: Clone + Send> Drop for LoaderTask<K, V> {
    fn drop(&mut self) {
        if let Some(loader) = self.state.upgrade() {
            let mut guard = loader.lock();
            if let Entry::Occupied(o) = guard.entry(self.key) {
                if o.get().as_ptr() == self as *const Self {
                    o.remove();
                }
            }
        }
    }
}

impl<K: Hash + Eq + Copy, V: Clone + Send + 'static> LoaderTask<K, V> {
    fn new<Fut>(key: K, state: Weak<LoaderState<K, V>>, fut: Fut) -> Self
    where
        Fut: Future<Output = Result<V, CatalogError>> + Send + 'static,
    {
        Self {
            key,
            state,
            recv: TaskFuture(tokio::spawn(fut)).shared(),
        }
    }

    fn join(self: Arc<Self>) -> LoaderFuture<K, V> {
        LoaderFuture {
            fut: self.recv.clone(),
            _task: self,
        }
    }
}

/// The future spawned by [`Loader`] to the tokio threadpool
#[derive(Debug)]
struct TaskFuture<V>(JoinHandle<Result<V, CatalogError>>);

impl<V> Future for TaskFuture<V> {
    type Output = Result<V>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match ready!(self.0.poll_unpin(cx)) {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(source)) => Err(Error::Catalog {
                source: Arc::new(source),
            }),
            Err(e) => Err(Error::Join {
                desc: e.to_string(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{join, try_join};
    use std::time::Duration;

    async fn panic_fut() -> Result<u64, CatalogError> {
        panic!("should deduplicate")
    }

    async fn ok_fut(generation: u64) -> Result<u64, CatalogError> {
        Ok(generation)
    }

    async fn err_fut() -> Result<u64, CatalogError> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        Err(CatalogError::AlreadyExists {
            descr: "test".to_string(),
        })
    }

    #[tokio::test]
    async fn test_loader() {
        let loader = Loader::default();

        // Two tasks on the same key should be deduplicated
        let v1 = loader.load(0, ok_fut(0));
        let v2 = loader.load(0, panic_fut());

        let (v1, v2) = try_join!(v1, v2).unwrap();
        assert_eq!(v1, v2);

        let v1 = loader.load(0, ok_fut(1)).await.unwrap();
        assert_eq!(v1, 1);

        // Dropping first task shouldn't impact second waiting on its computation
        let v1 = loader.load(0, ok_fut(2));
        let v2 = loader.load(0, panic_fut());

        drop(v1);
        let v = v2.await.unwrap();
        assert_eq!(v, 2);

        // Shouldn't deduplicate if different keys
        let v1 = loader.load(1, ok_fut(3));
        let v2 = loader.load(2, ok_fut(4));
        let (v1, v2) = try_join!(v1, v2).unwrap();
        assert_eq!(v1, 3);
        assert_eq!(v2, 4);

        // Shouldn't deduplicate if refresh
        let v1 = loader.load(1, ok_fut(5));
        let v2 = loader.refresh(1, ok_fut(6));
        let v3 = loader.refresh(1, ok_fut(7));
        let v4 = loader.load(1, ok_fut(8));
        let (v1, v2, v3, v4) = try_join!(v1, v2, v3, v4).unwrap();
        assert_eq!(v1, 5);
        assert_eq!(v2, 6);
        assert_eq!(v3, 7);
        assert_eq!(v4, 7);

        // Should deduplicate errors
        let v1 = loader.load(1, err_fut());
        let v2 = loader.load(1, panic_fut());

        let (v1, v2) = join!(v1, v2);
        assert_eq!(
            v1.unwrap_err().to_string(),
            "Loader catalog error: already exists: test"
        );
        assert_eq!(
            v2.unwrap_err().to_string(),
            "Loader catalog error: already exists: test"
        );
    }
}
