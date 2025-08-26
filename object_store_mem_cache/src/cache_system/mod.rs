//! Cache System.
//!
//! # Design
//! There are the following components:
//!
//! - [`Cache`]: The actual cache that maps keys to [shared futures] that return results.
//! - [`Hook`]: Can react to state changes of the cache state. Implements logging but also size limitations.
//! - [`Reactor`]: Drives pruning / garbage-collection decisions of the [`Cache`]. Implemented as background task so
//!   users don't need to drive that themselves.
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
use futures::future::BoxFuture;
use object_store_metrics::cache_state::CacheState;
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

/// Result type for cache requests.
///
/// The value (`V`) must be cloneable, so that they can be shared between multiple consumers of the cache.
pub type CacheRequestResult<V> = Result<V, DynError>;

impl<T> HasSize for CacheRequestResult<T>
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

/// Type of the function that is used to fetch a value for a key.
type CacheFn<V> = Box<dyn FnOnce() -> BoxFuture<'static, CacheRequestResult<V>> + Send>;

#[async_trait]
pub trait Cache<K, V, D>: Send + Sync + Debug
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + HasSize + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by a background tokio task and will make progress even when you do not poll the resulting
    /// future.
    ///
    /// Returns a future that resolves to the value [`CacheRequestResult<V>`].
    /// If data is loading, provides early access data (`D`).
    ///
    /// The early access data (`D`) may be used by metrics, logging, or other purposes.
    fn get_or_fetch(
        &self,
        k: &K,
        f: CacheFn<V>,
        d: D,
    ) -> (
        BoxFuture<'static, CacheRequestResult<V>>,
        Option<D>,
        CacheState,
    );

    /// Get the cached value and return `None` if was not cached.
    ///
    /// Entries that are currently being loaded also result in `None`.
    fn get(&self, k: &K) -> Option<CacheRequestResult<V>>;

    /// Get number of entries in the cache.
    fn len(&self) -> usize;

    /// Return true if the cache is empty
    fn is_empty(&self) -> bool;

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    fn prune(&self);
}

pub mod hook;
pub mod loader;
pub mod reactor;
pub mod s3_fifo_cache;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_utils;
