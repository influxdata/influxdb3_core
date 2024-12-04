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
pub(crate) struct S3FifoEntry<K, V>
where
    K: ?Sized,
{
    key: Arc<K>,
    value: Arc<V>,
    gen: u64,
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
            gen: _,
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
pub(crate) struct S3Fifo<K, V>
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
    pub(crate) fn new(config: S3Config<K>, metric_registry: &metric::Registry) -> Self {
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
    pub(crate) fn get_or_put(&self, key: Arc<K>, value: Arc<V>, gen: u64) -> CacheEntry<K, V> {
        // Lock the state BEFORE checking `self.entries`. We won't prevent concurrent reads with it but we prevent that
        // concurrent writes could check `entries`, find the key absent and then double-insert the data into the locked state.
        let mut guard = self.locked_state.lock();

        // Cache hit
        if let Some(entry) = self.entries.get(&key) {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            self.config.hook.evict(gen, &key, EvictResult::Unfetched);
            return Arc::clone(&entry);
        }

        let entry = Arc::new(S3FifoEntry {
            key: Arc::clone(&key),
            value,
            gen,
            freq: 0.into(),
        });

        self.config.hook.fetched(gen, &entry.key, Ok(entry.size()));
        self.entries
            .insert(Arc::clone(&entry.key), Arc::clone(&entry));
        if guard.ghost.remove(&key) {
            guard.evict(&self.entries, &self.config);
            guard.main.push_back(Arc::clone(&entry));
            entry
        } else {
            guard.evict(&self.entries, &self.config);
            guard.small.push_back(Arc::clone(&entry));
            entry
        }
    }

    /// Gets entry from the set, returns `None` if the key is NOT stored.
    ///
    /// Note that even when the key is NOT stored as an entry, it may be known as a "ghost" and stored within the
    /// internal "ghost set".
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
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

    /// Number of stored entries.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are NO entries within the set.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Immutable config state of [`S3Fifo`]
pub(crate) struct S3Config<K>
where
    K: ?Sized,
{
    pub(crate) max_memory_size: usize,
    pub(crate) max_ghost_memory_size: usize,

    pub(crate) hook: Arc<dyn Hook<K>>,

    /// Controls when we start evicting from S in an attempt to move more items to M.
    pub(crate) move_to_main_threshold: f64,
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
    fn insert_ghost(&mut self, key: Arc<K>, config: &S3Config<K>) {
        while self.ghost.memory_size() >= config.max_ghost_memory_size {
            self.ghost.pop_front();
        }

        self.ghost.push_back(key);
    }

    fn evict(&mut self, entries: &Entries<K, V>, config: &S3Config<K>) {
        let small_queue_threshold =
            (config.move_to_main_threshold * config.max_memory_size as f64) as usize;

        while self.small.memory_size() + self.main.memory_size() >= config.max_memory_size {
            if self.small.memory_size() >= small_queue_threshold {
                self.evict_s(entries, config)
            } else {
                self.evict_m(entries, config)
            }
        }
    }

    fn evict_s(&mut self, entries: &Entries<K, V>, config: &S3Config<K>) {
        while let Some(tail) = self.small.pop_front() {
            if tail.freq.load(Ordering::SeqCst) > 0 {
                self.main.push_back(tail);
            } else {
                let size = tail.size();
                entries.remove(&tail.key);
                self.insert_ghost(Arc::clone(&tail.key), config);
                config
                    .hook
                    .evict(tail.gen, &tail.key, EvictResult::Fetched { size });
                break;
            }
        }
    }

    fn evict_m(&mut self, entries: &Entries<K, V>, config: &S3Config<K>) {
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
                    .evict(tail.gen, &tail.key, EvictResult::Fetched { size });
                break;
            }
        }
    }
}
