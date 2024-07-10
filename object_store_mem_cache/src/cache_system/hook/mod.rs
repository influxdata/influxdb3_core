pub(crate) mod chain;
pub(crate) mod limit;
pub(crate) mod observer;

#[cfg(test)]
pub(crate) mod test_utils;

use crate::cache_system::interfaces::DynError;

/// A trait for hooking into cache updates.
///
/// This can be used for:
/// - injecting metrics
/// - maintaining secondary indices
/// - limiting memory usage
/// - ...
///
/// Note: members are invoked under locks and should therefore
/// be short-running and not call back into the cache.
///
/// # Eventual Consistency
/// To simplify accounting and prevent large-scale locking, [`evict`](Self::evict) will only be called after the
/// fetching future is finished. This means that a key may be observed concurrently, one version that is dropped from
/// the cache but that still has a polling future and a new version. Use the generation number to distinguish them.
pub(crate) trait Hook: std::fmt::Debug + Send + Sync {
    type K;

    /// Called before a value is potentially inserted.
    fn insert(&self, _gen: u64, _k: &Self::K) {}

    /// A value was fetched.
    ///
    /// The hook can reject a value using an error.
    fn fetched(
        &self,
        _gen: u64,
        _k: &Self::K,
        _res: &Result<usize, DynError>,
    ) -> Result<(), DynError> {
        Ok(())
    }

    /// A key removed.
    ///
    /// The value is set if it was fetched.
    fn evict(&self, _gen: u64, _k: &Self::K, _res: &Option<Result<usize, ()>>) {}
}
