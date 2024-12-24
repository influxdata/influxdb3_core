//! Cache System.
//!
//! # Design
//! There are the following components:
//!
//! - [`Cache`]: The actual cache that maps keys to [shared futures] that return results.
//! - [`Hook`]: Can react to state changes of the cache state. Implements logging but also size limitations.
//! - [`Reactor`]: Drives pruning / garbage-collection decisions of the [`Cache`]. Implemented as background task so
//!       users don't need to drive that themselves.
//!
//! ```text
//!    +-------+                         +------+
//!    | Cache |----(informs & asks)---->| Hook |
//!    +-------+                         +------+
//!        ^                                |
//!        |                                |
//!        |                                |
//! (drives pruning)                        |
//!        |                                |
//!        |                                |
//!        |                                |
//!   +---------+                           |
//!   | Reactor |<-------(informs)----------+
//!   +---------+
//! ```
//!
//!
//! [`Cache`]: self::Cache
//! [`Hook`]: self::hook::Hook
//! [`Reactor`]: self::reactor::Reactor
//! [shared futures]: futures::future::Shared
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use std::{fmt::Debug, hash::Hash};

use std::sync::Arc;

/// Provides size estimations for an immutable object.
pub trait HasSize {
    /// Size in bytes.
    fn size(&self) -> usize;
}

impl HasSize for &'static str {
    fn size(&self) -> usize {
        // no dynamic allocation
        0
    }
}

impl HasSize for str {
    fn size(&self) -> usize {
        self.len()
    }
}

impl HasSize for data_types::ObjectStoreId {
    fn size(&self) -> usize {
        // no dynamic allocation
        0
    }
}

impl HasSize for object_store::path::Path {
    fn size(&self) -> usize {
        self.as_ref().len()
    }
}

impl HasSize for Bytes {
    fn size(&self) -> usize {
        self.len()
    }
}

impl<T> HasSize for Arc<T>
where
    T: HasSize + ?Sized,
{
    fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref()) + self.as_ref().size()
    }
}

/// Dynamic error type.
pub type DynError = Arc<dyn std::error::Error + Send + Sync>;

impl HasSize for DynError {
    fn size(&self) -> usize {
        self.to_string().len()
    }
}

/// Result type with value wrapped into [`Arc`]s.
pub type ArcResult<T> = Result<Arc<T>, DynError>;

impl<T> HasSize for ArcResult<T>
where
    T: HasSize,
{
    fn size(&self) -> usize {
        match self {
            Ok(o) => o.size(),
            Err(e) => e.size(),
        }
    }
}

type CacheFut<V> = Shared<BoxFuture<'static, ArcResult<V>>>;
type CacheFn<K, V> = Box<dyn FnOnce(&K) -> BoxFuture<'static, Result<V, DynError>> + Send>;

#[async_trait]
pub trait Cache<K, V>: Send + Sync + Debug
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: HasSize + Send + Sync + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by a background tokio task and will make progress even when you do not poll the resulting
    /// future.
    async fn get_or_fetch(&self, k: &K, f: CacheFn<K, V>) -> (ArcResult<V>, CacheState);

    /// Get the cached value and return `None` if was not cached.
    ///
    /// Entries that are currently being loaded also result in `None`.
    fn get(&self, k: &K) -> Option<ArcResult<V>>;

    /// Get number of entries in the cache.
    fn len(&self) -> usize;

    /// Return true if the cache is empty
    fn is_empty(&self) -> bool;

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    fn prune(&self);
}

/// State that provides more information about [`get_or_fetch`](Cache::get_or_fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    /// Entry was already part of the cache and fully fetched..
    WasCached,

    /// Entry was already part of the cache but did not finish loading.
    AlreadyLoading,

    /// A new entry was created.
    NewEntry,
}

pub mod hook;
pub mod hook_limited;
pub mod reactor;
pub mod s3_fifo_cache;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_utils;
