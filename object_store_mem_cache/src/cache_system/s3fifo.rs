use async_trait::async_trait;
use dashmap::{DashMap, DashSet, Entry};
use futures::future::{BoxFuture, FutureExt};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use super::hook::{EvictResult, Hook};
use super::reactor::reaction::Reaction;
use super::utils::{str_err, CatchUnwindDynErrorExt, TokioTask};
use super::{ArcResult, HasSize};
use super::{Cache, CacheFn, CacheFut, CacheState, DynError};

type CacheEntry<K, V> = Arc<S3FifoEntry<K, ArcResult<V>>>;

#[derive(Debug)]
pub struct S3FifoEntry<K, V> {
    pub key: K,
    pub value: V,
    pub freq: AtomicU8,
}

pub(crate) struct S3Fifo<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    main_queue: VecDeque<CacheEntry<K, V>>,
    small_queue: VecDeque<CacheEntry<K, V>>,
    ghost_queue: VecDeque<K>,

    entries: DashMap<K, CacheEntry<K, V>>,
    ghost_entries: DashSet<K>,

    main_memory_size: usize,
    small_memory_size: usize,
    ghost_memory_size: usize,

    max_memory_size: usize,

    hook: Arc<dyn Hook<K>>,

    // Controls when we start evicting from S
    // in an attempt to move more items to M
    move_to_main_threshold: f64,
}

impl<K, V> Debug for S3Fifo<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Fifo")
            .field("main_memory_size", &self.main_memory_size)
            .field("small_memory_size", &self.small_memory_size)
            .field("ghost_memory_size", &self.ghost_memory_size)
            .field("max_memory_size", &self.max_memory_size)
            .finish()
    }
}

impl<K, V> S3Fifo<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    pub(crate) fn new(
        max_memory_size: usize,
        move_to_main_threshold: f64,
        hook: Arc<dyn Hook<K>>,
    ) -> Self {
        Self {
            main_queue: Default::default(),
            small_queue: Default::default(),
            ghost_queue: Default::default(),

            entries: Default::default(),
            ghost_entries: Default::default(),

            main_memory_size: 0,
            small_memory_size: 0,
            ghost_memory_size: 0,

            max_memory_size,
            move_to_main_threshold,
            hook,
        }
    }

    pub(crate) fn get_or_put(&mut self, key: K, value: ArcResult<V>) -> CacheEntry<K, V> {
        // Cache hit
        if let Some(entry) = self.entries.get(&key) {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            return Arc::clone(&entry);
        }

        let entry = Arc::new(S3FifoEntry {
            key: key.clone(),
            value,
            freq: 0.into(),
        });

        self.entries.insert(entry.key.clone(), Arc::clone(&entry));
        if self.ghost_entries.get(&key).is_some() {
            self.remove_from_ghost_queue(&key);
            self.evict();
            self.insert_main(entry)
        } else {
            self.evict();
            self.insert_small(entry)
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<CacheEntry<K, V>> {
        let entry = self.entries.get(key).map(|v| Arc::clone(&v));
        if let Some(entry) = entry {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            Some(entry)
        } else {
            None
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn entry(&self, key: K) -> Option<CacheEntry<K, V>> {
        let entry = self.entries.entry(key);
        match entry {
            Entry::Occupied(o) => Some(Arc::clone(o.get())),
            Entry::Vacant(_) => None,
        }
    }

    fn insert_main(&mut self, entry: CacheEntry<K, V>) -> CacheEntry<K, V> {
        entry.freq.store(0, Ordering::SeqCst);
        self.main_queue.push_back(Arc::clone(&entry));
        self.main_memory_size += Self::get_size(&entry.value);
        entry
    }

    fn insert_small(&mut self, entry: CacheEntry<K, V>) -> CacheEntry<K, V> {
        self.small_queue.push_back(Arc::clone(&entry));
        self.small_memory_size += Self::get_size(&entry.value);
        entry
    }

    fn insert_ghost(&mut self, key: K) {
        // In regular S3 Fifo, this operates on object count.
        // We are restricting the other queues based on memory size,
        // but the qhost queue will not reach the size of the others since it is just the keys.
        //
        // TODO: Decide on metric for emptying ghost queue
        while self.ghost_memory_size >= self.max_memory_size {
            // Insert at back, pop front to maintain FIFO
            self.ghost_queue.pop_front();
        }

        self.ghost_queue.push_back(key.clone());
        self.ghost_entries.insert(key);
    }

    fn evict(&mut self) {
        let small_queue_threshold =
            (self.move_to_main_threshold * self.max_memory_size as f64) as usize;

        while self.small_memory_size + self.main_memory_size >= self.max_memory_size {
            if self.small_memory_size >= small_queue_threshold {
                self.evict_s()
            } else {
                self.evict_m()
            }
        }
    }

    fn evict_s(&mut self) {
        while self.small_memory_size > 0 {
            let tail = self.small_queue.pop_front();
            if let Some(tail) = tail {
                if tail.freq.load(Ordering::SeqCst) > 0 {
                    self.small_memory_size -= Self::get_size(&tail.value);
                    self.insert_main(tail);
                } else {
                    let size = Self::get_size(&tail.value);
                    self.small_memory_size -= size;
                    self.entries.remove(&tail.key);
                    self.insert_ghost(tail.key.clone());
                    self.hook.evict(0, &tail.key, EvictResult::Fetched { size });
                    break;
                }
            } else {
                // If the queue is empty, no more eviction can happen
                return;
            }
        }
    }

    fn evict_m(&mut self) {
        while self.main_memory_size > 0 {
            let tail = self.main_queue.pop_front();
            if let Some(tail) = tail {
                if tail.freq.load(Ordering::SeqCst) > 0 {
                    tail.freq.fetch_sub(1, Ordering::SeqCst);
                    self.main_queue.push_back(tail);
                } else {
                    let size = Self::get_size(&tail.value);
                    self.main_memory_size -= size;
                    self.entries.remove(&tail.key);
                    self.hook.evict(0, &tail.key, EvictResult::Fetched { size });
                    break;
                }
            } else {
                // If the queue is empty, no more eviction can happen
                return;
            }
        }
    }

    fn remove_from_ghost_queue(&mut self, key: &K) {
        let item: Vec<(usize, &K)> = self
            .ghost_queue
            .iter()
            .enumerate()
            .filter(|(_pos, k)| k == &key)
            .collect();

        // assume we only found one
        let item = item[0];

        self.ghost_queue.remove(item.0);
        self.ghost_entries.remove(key);
    }

    fn get_size(res: &ArcResult<V>) -> usize {
        match res {
            Ok(v) => v.size(),
            Err(e) => e.size(),
        }
    }
}

enum S3CacheResponse<V> {
    New(CacheFut<V>),
    Known(CacheFut<V>),
    Resolved(ArcResult<V>),
}

#[derive(Debug)]
pub struct S3FifoCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    cache: Arc<RwLock<S3Fifo<K, V>>>,
    unresolved_futures: Arc<DashMap<K, CacheFut<V>>>,
    hook: Arc<dyn Hook<K>>,
}

impl<K, V> S3FifoCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + Debug + 'static,
    V: HasSize + Send + Sync + Debug + 'static,
{
    pub fn new(
        max_memory_size: usize,
        move_to_main_threshold: f64,
        hook: Arc<dyn Hook<K>>,
    ) -> Self {
        Self {
            cache: Arc::new(RwLock::new(S3Fifo::new(
                max_memory_size,
                move_to_main_threshold,
                Arc::clone(&hook),
            ))),
            unresolved_futures: Arc::new(DashMap::default()),
            hook,
        }
    }

    fn get_or_fetch_impl<F, Fut>(&self, k: &K, f: F) -> S3CacheResponse<V>
    where
        F: FnOnce(&K) -> Fut,
        Fut: Future<Output = Result<V, DynError>> + Send + 'static,
    {
        // Check the two caches first
        if let Some(entry) = self.unresolved_futures.get(k) {
            return S3CacheResponse::Known(entry.value().clone());
        } else if let Some(entry) = self.cache.read().expect("not poisoned").get(k) {
            return S3CacheResponse::Resolved(entry.value.clone());
        }

        let fut = f(k);
        let hook_captured = Arc::clone(&self.hook);
        let cache_captured = Arc::downgrade(&self.cache);
        let unresolved_futures_captured = Arc::downgrade(&self.unresolved_futures);
        let k_captured = k.clone();
        let fut = async move {
            let fetch_res = fut.catch_unwind_dyn_error().await;

            let hook_input = fetch_res.as_ref().map(|v| v.size()).map_err(Arc::clone);

            // Record that a fetch completed
            hook_captured.fetched(
                0, // We don't need the gen in S3Fifo
                &k_captured,
                hook_input.as_ref().map(|size| *size),
            );

            match fetch_res {
                Ok(v) => {
                    let data = Arc::new(v);
                    // This is where we know the size and can actually insert
                    // into the S3Fifo Cache
                    let strong_cache = cache_captured.upgrade();
                    if let Some(strong_cache) = strong_cache {
                        let strong_unresolved_futures = unresolved_futures_captured.upgrade();
                        if let Some(suf) = strong_unresolved_futures {
                            {
                                let mut guard = strong_cache.write().expect("not poisoned");
                                guard.get_or_put(k_captured.clone(), Ok(Arc::clone(&data)));
                            }
                            suf.remove(&k_captured);
                            Ok(data)
                        } else {
                            Err(str_err("Could not upgrade unresolved futures map pointer, has the cache been dropped?"))
                        }
                    } else {
                        Err(str_err(
                            "Could not upgrade S3Fifo cache pointer, has the cache been dropped?",
                        ))
                    }
                }
                Err(e) => {
                    let strong_cache = cache_captured.upgrade();
                    if let Some(strong_cache) = strong_cache {
                        let mut guard = strong_cache.write().expect("not poisoned");
                        guard.get_or_put(k_captured.clone(), Err(Arc::clone(&e)));
                    }
                    Err(e)
                }
            }
        };

        let entry = { self.cache.read().expect("not poisoned").entry(k.clone()) };
        match entry {
            Some(entry) => {
                // race, entry was created in the meantime, this is fine, just use the existing one
                S3CacheResponse::Resolved(entry.value.clone())
            }
            None => {
                let fut = TokioTask::spawn(fut).boxed().shared();
                self.unresolved_futures.insert(k.clone(), fut.clone());
                self.hook.insert(0, k);
                S3CacheResponse::New(fut)
            }
        }
    }
}

#[async_trait]
impl<K, V> Cache<K, V> for S3FifoCache<K, V>
where
    K: Debug + Clone + Eq + Hash + Send + Sync + 'static,
    V: Debug + HasSize + Send + Sync + 'static,
{
    async fn get_or_fetch(&self, k: &K, f: CacheFn<K, V>) -> (ArcResult<V>, CacheState) {
        match self.get_or_fetch_impl(k, f) {
            S3CacheResponse::New(fut) => {
                let res = fut.await;
                (res, CacheState::NewEntry)
            }
            S3CacheResponse::Known(fut) => {
                let res = fut.await;
                (res, CacheState::AlreadyLoading)
            }
            S3CacheResponse::Resolved(res) => (res, CacheState::WasCached),
        }
    }

    fn get(&self, k: &K) -> Option<ArcResult<V>> {
        self.cache
            .read()
            .expect("not poisoned")
            .get(k)
            .map(|entry| entry.value.clone())
    }

    fn len(&self) -> usize {
        self.cache.read().expect("not poisoned").len()
    }

    fn is_empty(&self) -> bool {
        self.cache.read().expect("not poisoned").is_empty()
    }

    fn prune(&self) {
        // Intentionally unimplemented, S3Fifo handles its own pruning
    }
}

impl<K, V> Reaction for S3FifoCache<K, V>
where
    K: Debug + Clone + Eq + Hash + Send + Sync + 'static,
    V: Debug + HasSize + Send + Sync + 'static,
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
    use crate::cache_system::test_utils::{gen_cache_tests, runtime_shutdown, TestSetup};
    #[test]
    fn test_runtime_shutdown() {
        runtime_shutdown(TestSetup::get_hook_limited());
    }

    gen_cache_tests!(true);
}
