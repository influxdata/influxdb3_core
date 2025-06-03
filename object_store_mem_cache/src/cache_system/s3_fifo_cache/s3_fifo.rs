use dashmap::DashMap;
use std::{
    fmt::{Debug, Formatter},
    hash::Hash,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};
use tracker::{LockMetrics, Mutex};

use crate::cache_system::{
    hook::{EvictResult, Hook},
    HasSize,
};

use super::{fifo::Fifo, ordered_set::OrderedSet};

/// Entry within [`S3Fifo`].
#[derive(Debug)]
pub struct S3FifoEntry<K, V>
where
    K: ?Sized,
{
    key: Arc<K>,
    value: Arc<V>,
    generation: u64,
    freq: AtomicU8,
}

impl<K, V> S3FifoEntry<K, V>
where
    K: ?Sized,
{
    pub(crate) fn value(&self) -> &Arc<V> {
        &self.value
    }
}

impl<K, V> HasSize for S3FifoEntry<K, V>
where
    K: HasSize + ?Sized,
    V: HasSize,
{
    fn size(&self) -> usize {
        let Self {
            key,
            value,
            generation: _,
            freq: _,
        } = self;
        key.size() + value.size()
    }
}

pub(crate) type CacheEntry<K, V> = Arc<S3FifoEntry<K, V>>;
type Entries<K, V> = DashMap<Arc<K>, CacheEntry<K, V>>;

/// Implementation of the [S3-FIFO] cache algorithm.
///
/// # Hook Interaction
/// This calls SOME callbacks of the provided [`Hook`]. The caller is expected to call [`Hook::insert`] though. The only
/// method that can trigger hook callbacks is [`get_or_put`](Self::get_or_put). See method docs for more details.
///
///
/// [S3-FIFO]: https://s3fifo.com/
pub struct S3Fifo<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    locked_state: Mutex<LockedState<K, V>>,
    entries: Entries<K, V>,
    config: S3Config<K>,
}

impl<K, V> Debug for S3Fifo<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Fifo")
            .field("entries", &self.entries.len())
            .field("config", &self.config)
            .finish()
    }
}

impl<K, V> S3Fifo<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    /// Create new, empty set.
    pub fn new(config: S3Config<K>, metric_registry: &metric::Registry) -> Self {
        let lock_metrics = Arc::new(LockMetrics::new(metric_registry, &[("lock", "s3fifo")]));
        Self {
            locked_state: lock_metrics.new_mutex(LockedState {
                main: Default::default(),
                small: Default::default(),
                ghost: Default::default(),
            }),
            entries: Default::default(),
            config,
        }
    }

    /// Gets entry from the set, or inserts it if it does not exist yet.
    ///
    /// # Hook Interaction
    /// If the key already exists, this calls [`Hook::evict`] w/ [`EvictResult::Unfetched`] on the provided new data
    /// since the new data is rejected.
    ///
    /// If the key is new, it calls [`Hook::fetched`].
    ///
    /// If inserting a new key leads to eviction of existing data, [`Hook::evict`] w/ [`EvictResult::Fetched`] is
    /// called. [`EvictResult::Failed`] is NOT used since we also account for the size of errrors.
    ///
    /// # Concurrency
    /// Acquires a lock and blocks other calls to [`get_or_put`](Self::get_or_put).
    ///
    /// Does NOT block read methods like [`get`](Self::get), [`len`](Self::len), and [`is_empty`](Self::is_empty),
    /// except for short-lived internal locks within [`DashMap`].
    pub fn get_or_put(&self, key: Arc<K>, value: Arc<V>, generation: u64) -> CacheEntry<K, V> {
        // Lock the state BEFORE checking `self.entries`. We won't prevent concurrent reads with it but we prevent that
        // concurrent writes could check `entries`, find the key absent and then double-insert the data into the locked state.
        let mut guard = self.locked_state.lock();

        // Cache hit
        if let Some(entry) = self.entries.get(&key) {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            self.config
                .hook
                .evict(generation, &key, EvictResult::Unfetched);
            let entry = Arc::clone(&entry);

            // drop guard before we drop key & value so that the work to
            // deallocate the key/value is not done while holding the lock
            // and preventing other operations from proceeding
            drop(guard);
            drop_it(key);
            drop_it(value);
            return entry;
        }

        let entry = Arc::new(S3FifoEntry {
            key,
            value,
            generation,
            freq: 0.into(),
        });

        self.config
            .hook
            .fetched(generation, &entry.key, Ok(entry.size()));
        self.entries
            .insert(Arc::clone(&entry.key), Arc::clone(&entry));
        let evicted = if guard.ghost.remove(&entry.key) {
            let evicted = guard.evict(&self.entries, &self.config);
            guard.main.push_back(Arc::clone(&entry));
            evicted
        } else {
            let evicted = guard.evict(&self.entries, &self.config);
            guard.small.push_back(Arc::clone(&entry));
            evicted
        };

        drop(guard);
        drop_it(evicted);

        entry
    }

    /// Gets entry from the set, returns `None` if the key is NOT stored.
    ///
    /// Note that even when the key is NOT stored as an entry, it may be known as a "ghost" and stored within the
    /// internal "ghost set".
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn get(&self, key: &K) -> Option<CacheEntry<K, V>> {
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

    /// Number of stored entries.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are NO entries within the set.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Calls [`drop`] but isn't inlined, so it is easier to see on profiles.
#[inline(never)]
fn drop_it<T>(t: T) {
    drop(t);
}

/// Immutable config state of [`S3Fifo`]
pub struct S3Config<K>
where
    K: ?Sized,
{
    pub max_memory_size: usize,
    pub max_ghost_memory_size: usize,

    pub hook: Arc<dyn Hook<K>>,

    /// Controls when we start evicting from S in an attempt to move more items to M.
    pub move_to_main_threshold: f64,
}

impl<K> std::fmt::Debug for S3Config<K>
where
    K: ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("max_memory_size", &self.max_memory_size)
            .field("max_ghost_memory_size", &self.max_ghost_memory_size)
            .field("hook", &self.hook)
            .field("move_to_main_threshold", &self.move_to_main_threshold)
            .finish()
    }
}

/// Mutable part of [`S3Fifo`] that is locked for [`get_or_put`](S3Fifo::get_or_put).
struct LockedState<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    main: Fifo<CacheEntry<K, V>>,
    small: Fifo<CacheEntry<K, V>>,
    ghost: OrderedSet<Arc<K>>,
}

impl<K, V> Debug for LockedState<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockedState")
            .field("main", &self.main)
            .field("small", &self.small)
            .field("ghost", &self.ghost)
            .finish()
    }
}

impl<K, V> LockedState<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    fn insert_ghost(&mut self, key: Arc<K>, config: &S3Config<K>, evicted_keys: &mut Vec<Arc<K>>) {
        while self.ghost.memory_size() >= config.max_ghost_memory_size {
            evicted_keys.push(self.ghost.pop_front().expect("ghost queue is NOT empty"));
        }

        self.ghost.push_back(key);
    }

    /// Evict entries, returning any evicted entries.
    fn evict(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
    ) -> (Vec<CacheEntry<K, V>>, Vec<Arc<K>>) {
        let small_queue_threshold =
            (config.move_to_main_threshold * config.max_memory_size as f64) as usize;
        let mut evicted_entries = Vec::with_capacity(8);
        let mut evicted_keys = Vec::with_capacity(8);

        while self.small.memory_size() + self.main.memory_size() >= config.max_memory_size {
            if self.small.memory_size() >= small_queue_threshold {
                self.evict_from_small_queue(
                    entries,
                    config,
                    &mut evicted_entries,
                    &mut evicted_keys,
                );
            } else {
                self.evict_from_main_queue(entries, config, &mut evicted_entries);
            }
        }

        (evicted_entries, evicted_keys)
    }

    /// Evict at most one entry from the "small" queue.
    ///
    /// This scans through the "small" queue and for every entry it either:
    ///
    /// - **move to "main" queue:** If the entry was used, move it to the back of the "main" queue
    /// - **move to "ghost" set:** If the entry was NOT used, remove it from the cache and add its key to the "ghost" set.
    ///
    /// The method returns if an unused entry was removed or if there are no entries left.
    ///
    /// See [S3-FIFO] for the defintion of the different queue/set types.
    ///
    ///
    /// [S3-FIFO]: https://s3fifo.com/
    fn evict_from_small_queue(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
        evicted_entries: &mut Vec<CacheEntry<K, V>>,
        evicted_keys: &mut Vec<Arc<K>>,
    ) {
        while let Some(tail) = self.small.pop_front() {
            if tail.freq.load(Ordering::SeqCst) > 0 {
                self.main.push_back(tail);
            } else {
                let size = tail.size();
                entries.remove(&tail.key);
                self.insert_ghost(Arc::clone(&tail.key), config, evicted_keys);
                config
                    .hook
                    .evict(tail.generation, &tail.key, EvictResult::Fetched { size });
                evicted_entries.push(tail);
                break;
            }
        }
    }

    /// Evict at most one entry from the "main" queue.
    ///
    /// This scans through the "main" queue and for every entry it either:
    ///
    /// - **move to to back:** If the entry was used, move it to the back of the "main" queue. Decrease its usage
    ///   counter by one.
    /// - **move to "ghost" set:** If the entry was NOT used, remove it from the cache and add its key to the "ghost" set.
    ///
    /// The method returns if an unused entry was removed or if there are no entries left.
    ///
    /// See [S3-FIFO] for the defintion of the different queue/set types.
    ///
    ///
    /// [S3-FIFO]: https://s3fifo.com/
    fn evict_from_main_queue(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
        evicted_entries: &mut Vec<CacheEntry<K, V>>,
    ) {
        while let Some(tail) = self.main.pop_front() {
            let was_not_zero = tail
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| f.checked_sub(1))
                .is_ok();

            if was_not_zero {
                self.main.push_back(tail);
            } else {
                let size = tail.size();
                entries.remove(&tail.key);
                config
                    .hook
                    .evict(tail.generation, &tail.key, EvictResult::Fetched { size });
                evicted_entries.push(tail);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use crate::cache_system::hook::test_utils::NoOpHook;

    use super::*;

    #[test]
    // ensure the drop (and deallocation) is done outside the critical section
    fn test_get_or_put_known_drops_key_outside_critical_section() {
        std::thread::scope(|s| {
            let s3 = s3_fifo();

            // prime S3-FIFO with an entry
            let barrier_a = Arc::new(Barrier::new(2));
            s3.get_or_put(DropBarrier::new("k", &barrier_a), Arc::new("v"), 0);

            let barrier_b = Arc::new(Barrier::new(3));
            let handle_1 =
                s.get_or_put_handle(&s3, DropBarrier::new("k", &barrier_b), Arc::new("v"));
            let handle_2 =
                s.get_or_put_handle(&s3, DropBarrier::new("k", &barrier_b), Arc::new("v"));
            let handle_3 = s.spawn(move || barrier_b.wait());
            handle_1.join().unwrap();
            handle_2.join().unwrap();
            handle_3.join().unwrap();

            // drop data
            s.drop_val(s3, barrier_a);
        });
    }

    #[test]
    fn test_get_or_put_known_drops_val_outside_critical_section() {
        std::thread::scope(|s| {
            let s3 = s3_fifo();

            // prime S3-FIFO with an entry
            let barrier_a = Arc::new(Barrier::new(2));
            s3.get_or_put(Arc::new("k"), DropBarrier::new("v", &barrier_a), 0);

            let barrier_b = Arc::new(Barrier::new(3));
            let handle_1 =
                s.get_or_put_handle(&s3, Arc::new("k"), DropBarrier::new("v", &barrier_b));
            let handle_2 =
                s.get_or_put_handle(&s3, Arc::new("k"), DropBarrier::new("v", &barrier_b));
            let handle_3 = s.spawn(move || barrier_b.wait());
            handle_1.join().unwrap();
            handle_2.join().unwrap();
            handle_3.join().unwrap();

            // drop data
            s.drop_val(s3, barrier_a);
        });
    }

    #[derive(Debug)]
    struct DropBarrier<T> {
        payload: T,
        barrier: Arc<Barrier>,
    }

    impl<T> DropBarrier<T> {
        fn new(payload: T, barrier: &Arc<Barrier>) -> Arc<Self> {
            Arc::new(Self {
                payload,
                barrier: Arc::clone(barrier),
            })
        }
    }

    impl<T> PartialEq for DropBarrier<T>
    where
        T: PartialEq,
    {
        fn eq(&self, other: &Self) -> bool {
            self.payload == other.payload
        }
    }

    impl<T> Eq for DropBarrier<T> where T: Eq {}

    impl<T> Hash for DropBarrier<T>
    where
        T: Hash,
    {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.payload.hash(state);
        }
    }

    impl<T> HasSize for DropBarrier<T>
    where
        T: HasSize,
    {
        fn size(&self) -> usize {
            self.payload.size()
        }
    }

    impl<T> Drop for DropBarrier<T> {
        fn drop(&mut self) {
            self.barrier.wait();
        }
    }

    fn s3_fifo<K, V>() -> Arc<S3Fifo<K, V>>
    where
        K: std::fmt::Debug + Eq + Hash + HasSize + Send + Sync + 'static,
        V: HasSize + Send + Sync + 'static,
    {
        Arc::new(S3Fifo::new(
            S3Config {
                max_memory_size: 10,
                max_ghost_memory_size: 10_000,
                hook: Arc::new(NoOpHook::default()),
                move_to_main_threshold: 0.5,
            },
            &metric::Registry::new(),
        ))
    }

    trait ScopeExt {
        type Handle;

        fn drop_val<T>(self, val: T, barrier: Arc<Barrier>);

        fn get_or_put_handle<K, V>(
            &self,
            s3: &Arc<S3Fifo<K, V>>,
            k: Arc<K>,
            v: Arc<V>,
        ) -> Self::Handle
        where
            K: Eq + Hash + HasSize + Send + Sync + 'static,
            V: HasSize + Send + Sync + 'static;
    }

    impl<'scope> ScopeExt for &'scope std::thread::Scope<'scope, '_> {
        type Handle = std::thread::ScopedJoinHandle<'scope, ()>;

        fn drop_val<T>(self, val: T, barrier: Arc<Barrier>) {
            let handle = self.spawn(move || barrier.wait());
            drop(val);
            handle.join().unwrap();
        }

        fn get_or_put_handle<K, V>(
            &self,
            s3: &Arc<S3Fifo<K, V>>,
            k: Arc<K>,
            v: Arc<V>,
        ) -> Self::Handle
        where
            K: Eq + Hash + HasSize + Send + Sync + 'static,
            V: HasSize + Send + Sync + 'static,
        {
            let s3_captured = Arc::clone(s3);
            self.spawn(move || {
                s3_captured.get_or_put(k, v, 0);
            })
        }
    }
}
