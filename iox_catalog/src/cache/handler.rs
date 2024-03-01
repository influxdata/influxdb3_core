//! Actual caching logic.

use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::{Backoff, BackoffConfig};
use catalog_cache::{
    api::quorum::{Error as QuorumError, QuorumCatalogCache},
    CacheKey, CacheValue,
};

use metric::{DurationHistogram, U64Counter};
use observability_deps::tracing::{debug, warn};

use crate::cache::snapshot::SnapshotKey;
use crate::{
    cache::loader::Loader,
    interface::{Catalog, Error, Result},
};

use super::snapshot::Snapshot;

#[derive(Debug)]
struct CacheMetric {
    count: U64Counter,
    duration: DurationHistogram,
}

impl CacheMetric {
    fn new(registry: &metric::Registry, variant: &'static str, result: &'static str) -> Self {
        let count = registry.register_metric::<U64Counter>(
            "iox_catalog_cache_get",
            "Number of GET operations to the catalog cache service",
        );

        let duration = registry.register_metric::<DurationHistogram>(
            "iox_catalog_cache_get_duration",
            "Distribution of GET operations to the catalog cache service",
        );

        let attributes = &[("variant", variant), ("result", result)];
        Self {
            count: count.recorder(attributes),
            duration: duration.recorder(attributes),
        }
    }

    fn record(&self, duration: Duration) {
        self.count.inc(1);
        self.duration.record(duration);
    }
}

/// Abstract cache access.
#[derive(Debug)]
pub(crate) struct CacheHandler<T>
where
    T: Snapshot,
{
    backing: Arc<dyn Catalog>,
    get_hit: CacheMetric,
    get_miss: CacheMetric,
    cache: Arc<QuorumCatalogCache>,
    backoff_config: Arc<BackoffConfig>,
    loader: Loader<T::Key, T>,
}

impl<T> CacheHandler<T>
where
    T: Snapshot,
{
    pub(crate) fn new(
        backing: Arc<dyn Catalog>,
        registry: &metric::Registry,
        cache: Arc<QuorumCatalogCache>,
        backoff_config: Arc<BackoffConfig>,
    ) -> Self {
        Self {
            backing,
            get_hit: CacheMetric::new(registry, T::NAME, "hit"),
            get_miss: CacheMetric::new(registry, T::NAME, "miss"),
            cache,
            backoff_config,
            loader: Loader::default(),
        }
    }

    /// Get data from quorum cache.
    ///
    /// This method implements retries.
    async fn get_quorum(&self, key: CacheKey) -> Result<Option<CacheValue>, QuorumError> {
        let mut backoff = Backoff::new(&self.backoff_config);

        // Note: We don't use retry_with_backoff as some retries are expected in the event
        // of racing writers or only two available replicas. We should only log if
        // the deadline expires
        loop {
            match self.cache.get(key).await {
                Ok(val) => return Ok(val),
                Err(e @ QuorumError::Quorum { .. }) => match backoff.next() {
                    None => return Err(e), // Deadline exceeded
                    Some(delay) => tokio::time::sleep(delay).await,
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Perform a snapshot
    ///
    /// If `refresh` is false will potentially await an existing snapshot request
    async fn do_snapshot(&self, k: T::Key, refresh: bool) -> Result<T> {
        let backing = Arc::clone(&self.backing);
        let cache = Arc::clone(&self.cache);

        let fut = async move {
            let snapshot = T::snapshot(backing.as_ref(), k).await?;
            let generation = snapshot.generation();
            let data = snapshot.to_bytes();

            debug!(what = T::NAME, key = k.get(), generation, "refresh",);
            cache
                .put(k.to_key(), CacheValue::new(data, generation))
                .await
                .map_err(|e| {
                    warn!(
                        what=T::NAME,
                        key=k.get(),
                        generation,
                        %e,
                        "quorum write failed",
                    );

                    e
                })?;

            Ok(snapshot)
        };

        let snapshot = match refresh {
            true => self.loader.refresh(k, fut).await?,
            false => self.loader.load(k, fut).await?,
        };
        Ok(snapshot)
    }

    /// Refresh cached value of given snapshot.
    ///
    /// This requests a new snapshot and performs a quorum-write.
    ///
    /// Note that this also performs a snapshot+write if the data was NOT cached yet.
    pub(crate) async fn refresh(&self, key: T::Key) -> Result<()> {
        self.do_snapshot(key, true).await?;
        Ok(())
    }

    /// Warm up cached value.
    pub(crate) async fn warm_up(&self, key: T::Key) -> Result<()> {
        self.do_snapshot(key, false).await?;
        Ok(())
    }

    /// Get snapshot.
    ///
    /// This first tries to quorum-read the data. If the data does not exist yet, this will perform a
    /// [refresh](Self::refresh).
    pub(crate) async fn get(&self, k: T::Key) -> Result<T> {
        let start = Instant::now();
        let key = k.to_key();
        match self.get_quorum(key).await {
            Ok(Some(val)) => {
                debug!(
                    what = T::NAME,
                    key = k.get(),
                    status = "HIT",
                    generation = val.generation(),
                    "get",
                );
                self.get_hit.record(start.elapsed());

                return T::from_cache_value(val);
            }
            Ok(None) => {
                debug!(what = T::NAME, key = k.get(), status = "MISS", "get",);
            }
            Err(e @ QuorumError::Quorum { .. }) => {
                warn!(
                    what = T::NAME,
                    key = k.get(),
                    elapsed=start.elapsed().as_secs_f64(),
                    %e,
                    "deadline expired for quorum read, obtaining fresh snapshot",
                );
            }
            Err(e) => {
                return Err(Error::External {
                    source: Box::new(e),
                })
            }
        }

        self.get_miss.record(start.elapsed());
        self.do_snapshot(k, false).await
    }
}
