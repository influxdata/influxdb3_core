use indexmap::{Equivalent, IndexSet};
use std::hash::Hash;

use crate::cache_system::HasSize;

/// And ordered set with size accounting.
///
/// # Data Structure
/// This uses an [`IndexSet`] under the hood but instead of [removing](IndexSet::shift_remove) elements right away, it
/// replaces them with tombstones.
///
/// This prevents from [`remove`](Self::remove) from being in `O(n)` (worst case), but reduces it to an amortized
/// `O(1)` (on average).
///
/// In exchange, [`pop_front`](Self::pop_front) is not truly `O(1)` anymore but only in the amortized case. It's now
/// `O(n)` in the worst case but basically only for one request because it would remove all processed tombstones. Since
/// we limit the number of tombstones to 50% of the entries within the [`IndexSet`], the number of processed tombstones
/// amortizes.
///
/// This is a worthwile tradeoff though.
pub(crate) struct OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    /// The core of this data structure.
    set: IndexSet<Entry<T>>,

    /// The memory sizes of all non-tomstone data contained in this set.
    ///
    /// This is affectively the sum of [`HasSize::size`].
    memory_size: usize,

    /// Ever-increasing counter to give every tombstone a unique identity.
    tombstone_counter: u64,

    /// Number of tombstones in the [set](Self::set).
    n_tombstones: usize,
}

impl<T> OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    /// Current amount of memory used by all entries.
    ///
    /// This does NOT include the memory consumption of the underlying [`IndexSet`] or the tombstones. They are
    /// considered rather minor if you store actual data.
    ///
    /// # Runtime Complexity
    /// This is always `O(1)`.
    ///
    /// This data is pre-computed and cached, so this is just a memory read.
    pub(crate) fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// Push new entry back to the set.
    ///
    /// # Panic
    /// The entry MUST NOT be part of the set yet.
    ///
    /// # Runtime Complexity
    /// This is always `O(1)`.
    pub(crate) fn push_back(&mut self, o: T) {
        self.memory_size += o.size();
        let is_new = self.set.insert(Entry::Data(o));
        assert!(is_new);
    }

    /// Pop entry from the front of the set.
    ///
    /// This may be called on and empty set.
    ///
    /// # Runtime Complexity
    /// This amortizes to `O(1)`.
    ///
    /// If there are only tombstones at the start of the set, this may be in `O(n)` though. The good thing is that the
    /// tombstoes will be gone afterwards, so that will be a one-time clean-up.
    pub(crate) fn pop_front(&mut self) {
        loop {
            if self.set.is_empty() {
                break;
            }

            match self.set.shift_remove_index(0).expect("should exist") {
                Entry::Data(o) => {
                    self.memory_size -= o.size();
                    break;
                }
                Entry::Tombstone(_) => {
                    self.n_tombstones -= 1;
                    // need to continue
                }
            }
        }
    }

    /// Remove element from the middle of the set.
    ///
    /// The relative order of the elements is preserved.
    ///
    /// Returns `true` if the entry was part of the set.
    ///
    /// # Runtime Complexity
    /// This amortizes to `O(1)`.
    ///
    /// If the number of tombstones after the removal would be larger than 50% of the entries within the [`IndexSet`],
    /// this will compact the data by removing all the tombstones. That will effectively rewrite the [`IndexSet`] and
    /// has `O(n)` complexity.
    pub(crate) fn remove(&mut self, o: &T) -> bool {
        match self.set.get_index_of(&Entry::Data(o)) {
            Some(idx) => {
                // replace entry w/ tombstone
                self.set.insert(Entry::Tombstone(self.tombstone_counter));
                self.tombstone_counter += 1;
                self.n_tombstones += 1;
                self.set
                    .swap_remove_index(idx)
                    .expect("just got this index");

                // account memory size
                self.memory_size -= o.size();

                // maybe compact data
                // NOTE: Use `>`, NOT `>=` here to prevent compactions for empty containers
                if self.n_tombstones * 2 > self.set.len() {
                    self.set.retain(|entry| matches!(entry, Entry::Data(_)));
                    self.n_tombstones = 0;
                }

                true
            }
            None => false,
        }
    }
}

impl<T> std::fmt::Debug for OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderedSet")
            .field("len", &self.set.len())
            .field("memory_size", &self.memory_size)
            .field("n_tombstones", &self.n_tombstones)
            .finish_non_exhaustive()
    }
}

impl<T> Default for OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    fn default() -> Self {
        Self {
            set: Default::default(),
            memory_size: 0,
            tombstone_counter: 0,
            n_tombstones: 0,
        }
    }
}

/// Underlying entry of [`OrderedSet`].
#[derive(PartialEq, Eq, Hash)]
enum Entry<T>
where
    T: Eq + Hash,
{
    /// Actual data.
    Data(T),

    /// A tombstone with a unique ID.
    ///
    /// The unique ID prevents two twostones from being equal and also spreads the hash values.
    Tombstone(u64),
}

impl<T> Equivalent<Entry<T>> for Entry<&T>
where
    T: Eq + Hash,
{
    fn equivalent(&self, key: &Entry<T>) -> bool {
        let key = match key {
            Entry::Data(o) => Entry::Data(o),
            Entry::Tombstone(idx) => Entry::Tombstone(*idx),
        };
        self.equivalent(&key)
    }
}
