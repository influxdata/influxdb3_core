use std::marker::PhantomData;

use metric::{U64Counter, U64Gauge};
use observability_deps::tracing::debug;

use crate::cache_system::{hook::Hook, interfaces::DynError};

#[derive(Debug)]
pub(crate) struct ObserverHook<K>
where
    K: std::fmt::Debug,
{
    _k: PhantomData<dyn Fn() -> K + Send + Sync + 'static>,
    inserted_elements: U64Counter,
    fetched_ok_elements: U64Counter,
    fetched_ok_bytes: U64Counter,
    fetched_err_elements: U64Counter,
    evict_unfetched_elements: U64Counter,
    evict_fetched_elements: U64Counter,
    evict_fetched_bytes: U64Counter,
    evict_failed_elements: U64Counter,
    #[allow(dead_code)]
    limit_bytes: Option<U64Gauge>,
}

impl<K> ObserverHook<K>
where
    K: std::fmt::Debug,
{
    pub(crate) fn new(
        cache: &'static str,
        metrics: &metric::Registry,
        limit_bytes: Option<u64>,
    ) -> Self {
        let metric_elements = metrics.register_metric::<U64Counter>(
            "mem_cache_change_elements",
            "Change of in-mem cache accounted by elements",
        );
        let metric_bytes = metrics.register_metric::<U64Counter>(
            "mem_cache_change_bytes",
            "Change of in-mem cache accounted by bytes",
        );

        Self {
            _k: Default::default(),
            inserted_elements: metric_elements
                .recorder(&[("cache", cache), ("transition", "inserted")]),
            fetched_ok_elements: metric_elements.recorder(&[
                ("cache", cache),
                ("transition", "fetched"),
                ("result", "ok"),
            ]),
            fetched_ok_bytes: metric_bytes.recorder(&[
                ("cache", cache),
                ("transition", "fetched"),
                ("result", "ok"),
            ]),
            fetched_err_elements: metric_elements.recorder(&[
                ("cache", cache),
                ("transition", "fetched"),
                ("result", "err"),
            ]),
            evict_unfetched_elements: metric_elements.recorder(&[
                ("cache", cache),
                ("transition", "evicted"),
                ("state", "unfetched"),
            ]),
            evict_fetched_elements: metric_elements.recorder(&[
                ("cache", cache),
                ("transition", "evicted"),
                ("state", "fetched"),
            ]),
            evict_fetched_bytes: metric_bytes.recorder(&[
                ("cache", cache),
                ("transition", "evicted"),
                ("state", "fetched"),
            ]),
            evict_failed_elements: metric_elements.recorder(&[
                ("cache", cache),
                ("transition", "evicted"),
                ("state", "failed"),
            ]),
            limit_bytes: limit_bytes.map(|limit| {
                let gauge = metrics
                    .register_metric::<U64Gauge>(
                        "mem_cache_limit_bytes",
                        "Limit of in-mem cache accounted by bytes",
                    )
                    .recorder(&[("cache", cache)]);
                gauge.set(limit);
                gauge
            }),
        }
    }
}

impl<K> Hook for ObserverHook<K>
where
    K: std::fmt::Debug,
{
    type K = K;

    fn insert(&self, gen: u64, k: &Self::K) {
        debug!(gen, ?k, "insert");
        self.inserted_elements.inc(1);
    }

    fn fetched(
        &self,
        gen: u64,
        k: &Self::K,
        res: &Result<usize, DynError>,
    ) -> Result<(), DynError> {
        match res.as_ref() {
            Ok(bytes) => {
                let bytes = *bytes;
                debug!(gen, ?k, size_bytes = bytes, "fetched successfully");
                self.fetched_ok_elements.inc(1);
                self.fetched_ok_bytes.inc(bytes as u64);
            }
            Err(e) => {
                debug!(gen, ?k, %e, "failed to fetch");
                self.fetched_err_elements.inc(1);
            }
        }

        Ok(())
    }

    fn evict(&self, gen: u64, k: &Self::K, res: &Option<Result<usize, ()>>) {
        match res.as_ref() {
            None => {
                debug!(gen, ?k, "evict element that was never fetched");
                self.evict_unfetched_elements.inc(1);
            }
            Some(Ok(bytes)) => {
                let bytes = *bytes;
                debug!(
                    gen,
                    ?k,
                    size_bytes = bytes,
                    "evict element that was fetched"
                );
                self.evict_fetched_elements.inc(1);
                self.evict_fetched_bytes.inc(bytes as u64);
            }
            Some(Err(())) => {
                debug!(
                    gen,
                    ?k,
                    "evict element that could not be fetched due to error"
                );
                self.evict_failed_elements.inc(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_system::test_utils::str_err;

    use super::*;

    #[test]
    fn test_new_limit() {
        let registry = metric::Registry::new();
        let hook = ObserverHook::<&'static str>::new("my_cache", &registry, Some(42));

        assert_eq!(
            Metrics::read(&hook),
            Metrics {
                inserted_elements: 0,
                fetched_ok_elements: 0,
                fetched_ok_bytes: 0,
                fetched_err_elements: 0,
                evict_unfetched_elements: 0,
                evict_fetched_elements: 0,
                evict_fetched_bytes: 0,
                evict_failed_elements: 0,
                limit_bytes: Some(42),
            },
        );
    }

    #[test]
    fn test_new_no_limit() {
        let registry = metric::Registry::new();
        let hook = ObserverHook::<&'static str>::new("my_cache", &registry, None);

        assert_eq!(
            Metrics::read(&hook),
            Metrics {
                inserted_elements: 0,
                fetched_ok_elements: 0,
                fetched_ok_bytes: 0,
                fetched_err_elements: 0,
                evict_unfetched_elements: 0,
                evict_fetched_elements: 0,
                evict_fetched_bytes: 0,
                evict_failed_elements: 0,
                limit_bytes: None,
            },
        );
    }

    #[test]
    fn test_insert() {
        let registry = metric::Registry::new();
        let hook = ObserverHook::<&'static str>::new("my_cache", &registry, None);

        hook.insert(1, &"foo");
        hook.insert(2, &"bar");
        assert_eq!(
            Metrics::read(&hook),
            Metrics {
                inserted_elements: 2,
                fetched_ok_elements: 0,
                fetched_ok_bytes: 0,
                fetched_err_elements: 0,
                evict_unfetched_elements: 0,
                evict_fetched_elements: 0,
                evict_fetched_bytes: 0,
                evict_failed_elements: 0,
                limit_bytes: None,
            },
        );
    }

    #[test]
    fn test_fetch() {
        let registry = metric::Registry::new();
        let hook = ObserverHook::<&'static str>::new("my_cache", &registry, None);

        hook.fetched(1, &"foo", &Ok(42)).unwrap();
        hook.fetched(2, &"bar1", &Err(str_err("e1"))).unwrap();
        hook.fetched(3, &"bar2", &Err(str_err("e2"))).unwrap();
        assert_eq!(
            Metrics::read(&hook),
            Metrics {
                inserted_elements: 0,
                fetched_ok_elements: 1,
                fetched_ok_bytes: 42,
                fetched_err_elements: 2,
                evict_unfetched_elements: 0,
                evict_fetched_elements: 0,
                evict_fetched_bytes: 0,
                evict_failed_elements: 0,
                limit_bytes: None,
            },
        );
    }

    #[test]
    fn test_evict() {
        let registry = metric::Registry::new();
        let hook = ObserverHook::<&'static str>::new("my_cache", &registry, None);

        hook.evict(1, &"foo", &None);
        hook.evict(2, &"bar1", &Some(Ok(42)));
        hook.evict(3, &"bar2", &Some(Ok(43)));
        hook.evict(4, &"baz1", &Some(Err(())));
        hook.evict(5, &"baz2", &Some(Err(())));
        hook.evict(6, &"baz3", &Some(Err(())));
        assert_eq!(
            Metrics::read(&hook),
            Metrics {
                inserted_elements: 0,
                fetched_ok_elements: 0,
                fetched_ok_bytes: 0,
                fetched_err_elements: 0,
                evict_unfetched_elements: 1,
                evict_fetched_elements: 2,
                evict_fetched_bytes: 85,
                evict_failed_elements: 3,
                limit_bytes: None,
            },
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Metrics {
        inserted_elements: u64,
        fetched_ok_elements: u64,
        fetched_ok_bytes: u64,
        fetched_err_elements: u64,
        evict_unfetched_elements: u64,
        evict_fetched_elements: u64,
        evict_fetched_bytes: u64,
        evict_failed_elements: u64,
        limit_bytes: Option<u64>,
    }

    impl Metrics {
        fn read<K>(hook: &ObserverHook<K>) -> Self
        where
            K: std::fmt::Debug,
        {
            let ObserverHook {
                _k,
                inserted_elements,
                fetched_ok_elements,
                fetched_ok_bytes,
                fetched_err_elements,
                evict_unfetched_elements,
                evict_fetched_elements,
                evict_fetched_bytes,
                evict_failed_elements,
                limit_bytes,
            } = hook;

            Self {
                inserted_elements: inserted_elements.fetch(),
                fetched_ok_elements: fetched_ok_elements.fetch(),
                fetched_ok_bytes: fetched_ok_bytes.fetch(),
                fetched_err_elements: fetched_err_elements.fetch(),
                evict_unfetched_elements: evict_unfetched_elements.fetch(),
                evict_fetched_elements: evict_fetched_elements.fetch(),
                evict_fetched_bytes: evict_fetched_bytes.fetch(),
                evict_failed_elements: evict_failed_elements.fetch(),
                limit_bytes: limit_bytes.as_ref().map(|g| g.fetch()),
            }
        }
    }
}
