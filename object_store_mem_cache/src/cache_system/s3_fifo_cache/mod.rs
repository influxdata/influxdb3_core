use async_trait::async_trait;
use futures::future::BoxFuture;
use object_store_metrics::cache_state::CacheState;
use std::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

// for benchmarks
pub use s3_fifo::{S3Config, S3Fifo};

use crate::cache_system::DynError;

use super::{
    Cache, CacheFn, CacheRequestResult, HasSize,
    hook::{EvictResult, Hook},
    loader::{Load, Loader},
    utils::CatchUnwindDynErrorExt,
};

mod fifo;
mod ordered_set;
mod s3_fifo;

enum S3CacheResponse<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    /// Respond with [`Load`] which can be used to either await the future ([`Load::into_future`].await),
    /// or access associated data via [`Load::data`].
    NewEntry(Load<V, D>),

    /// Respond with [`Load`] which can be used to either await the future ([`Load::into_future`].await),
    /// or access associated data via [`Load::data`].
    AlreadyLoading(Load<V, D>),

    /// Respond with value (`V`).
    WasCached(CacheRequestResult<V>),
}

/// A cache based upon the [`S3Fifo`] algorithm.
///
/// Caching is based upon a key (`K`) and return a value (`V`).
#[derive(Debug)]
pub struct S3FifoCache<K, V, D>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: Clone + HasSize + Send + Sync + Debug + 'static,
    D: Clone + Send + Sync + 'static,
{
    cache: Arc<S3Fifo<K, V>>,
    gen_counter: Arc<AtomicU64>,
    loader: Loader<K, V, D>,
    hook: Arc<dyn Hook<K>>,
}

impl<K, V, D> S3FifoCache<K, V, D>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: Clone + HasSize + Send + Sync + Debug + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Create a new S3FifoCache from an [`S3Config`].
    pub fn new(config: S3Config<K>, metric_registry: &metric::Registry) -> Self {
        let hook = Arc::clone(&config.hook);
        Self {
            cache: Arc::new(S3Fifo::new(config, metric_registry)),
            gen_counter: Default::default(),
            loader: Default::default(),
            hook,
        }
    }

    /// Create a new S3FifoCache from a snapshot.
    ///
    /// This function deserializes a snapshot and creates a new cache instance
    /// with the restored state.
    ///
    /// # Parameters
    /// - `config`: The S3 FIFO configuration. See [`S3Config`] for details.
    /// - `snapshot_data`: The serialized cache state data, typically created by [`snapshot`](Self::snapshot).
    /// - `shared_seed`: Provides context for bincode's deserialization process.
    ///   See [`S3Fifo::deserialize_snapshot`] for details on how this parameter
    ///   is used with bincode's [`Decode<Context>`](bincode::Decode) trait.
    ///   It's recommended to use a seed (`Q`) which is cheap to clone.
    ///
    /// # Returns
    /// A new [`S3FifoCache`] instance with the restored state.
    ///
    /// # Errors
    /// Returns a [`DynError`] if deserialization fails.
    pub fn new_from_snapshot<Q>(
        config: S3Config<K>,
        metric_registry: &metric::Registry,
        snapshot_data: &[u8],
        shared_seed: &Q,
    ) -> Result<Self, DynError>
    where
        Q: Clone,
        K: bincode::Encode + bincode::Decode<Q>,
        V: bincode::Encode + bincode::Decode<Q>,
    {
        let hook = Arc::clone(&config.hook);
        let cache = Arc::new(S3Fifo::new_from_snapshot::<Q>(
            config,
            metric_registry,
            snapshot_data,
            shared_seed,
        )?);

        Ok(Self {
            cache,
            gen_counter: Default::default(),
            loader: Default::default(),
            hook,
        })
    }

    /// Create a snapshot of the S3Fifo cache state.
    ///
    /// This function serializes the locked_state using bincode, allowing for
    /// persistence and recovery of the cache state.
    ///
    /// # Returns
    /// A `Vec<u8>` containing the serialized cache state.
    ///
    /// # Errors
    /// Returns a [`DynError`] if serialization fails.
    pub fn snapshot(&self) -> Result<Vec<u8>, DynError>
    where
        K: bincode::Encode,
        V: bincode::Encode,
    {
        self.cache.snapshot()
    }

    /// Get the value (`V`) from the cache.
    /// If the entry does not exist, run the future (`F`) that returns a value (`Arc<V>`).
    ///
    /// The value (`V`) is inserted after completion of a future. Some data (`D`) may be accessable earlier,
    /// before the future finishes, using the [`Load::data`].
    fn get_or_fetch_impl<F, Fut>(&self, k: &K, f: F, d: D) -> S3CacheResponse<V, D>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CacheRequestResult<V>> + Send + 'static,
    {
        // fast path to already-featched value
        if let Some(entry) = self.cache.get(k) {
            return S3CacheResponse::WasCached(Ok(entry.value().clone()));
        }

        // slow path
        let gen_counter_captured = Arc::clone(&self.gen_counter);
        let k_captured = Arc::new(k.clone());
        let hook_captured = Arc::clone(&self.hook);
        let cache_captured = Arc::downgrade(&self.cache);
        let fut = move || async move {
            // now we inform the hook of insertion
            let generation = gen_counter_captured.fetch_add(1, Ordering::SeqCst);
            hook_captured.insert(generation, &k_captured);

            // get the actual value (`V`)
            let fut = f();
            let fetch_res = fut.catch_unwind_dyn_error().await;

            // insert into cache
            match &fetch_res {
                Ok(v) => {
                    if let Some(cache) = cache_captured.upgrade() {
                        // NOTES:
                        // - Don't involve hook here because the cache is doing that for us correctly, even if the key is
                        //   already stored.
                        // - Tell tokio that this is potentially expensive. This is due to the fact that inserting new values
                        //   may free existing ones and the relevant allocator accounting can be rather pricey.
                        let k = Arc::clone(&k_captured);
                        let v = v.clone();
                        tokio::task::spawn_blocking(move || {
                            cache.get_or_put(k, v, generation);
                        })
                        .await
                        .expect("never fails");
                    } else {
                        // notify "fetched" and instantly evict, because underlying cache is gone (during shutdown)
                        let size = v.size();
                        hook_captured.fetched(generation, &k_captured, Ok(size));
                        hook_captured.evict(generation, &k_captured, EvictResult::Fetched { size });
                    }
                }
                Err(e) => {
                    hook_captured.fetched(generation, &k_captured, Err(e));
                    hook_captured.evict(generation, &k_captured, EvictResult::Failed);
                }
            }

            fetch_res
        };
        let load = self.loader.load(k.clone(), fut, d);

        // if already loading => then we don't have to spawn a task for insertion into cache (once loading is done).
        if load.already_loading() {
            S3CacheResponse::AlreadyLoading(load)
        } else {
            S3CacheResponse::NewEntry(load)
        }
    }

    /// Returns an iterator of all keys currently in the [`S3Fifo`] cache.
    ///
    /// Note that the keys listed in the cache are those which have returned from the
    /// [`CacheFn`] function, i.e. they are the keys that have been successfully fetched.
    pub fn list(&self) -> impl Iterator<Item = Arc<K>> {
        self.cache.keys()
    }
}

#[async_trait]
impl<K, V, D> Cache<K, V, D> for S3FifoCache<K, V, D>
where
    K: Clone + Debug + Eq + Hash + HasSize + Send + Sync + 'static,
    V: Clone + Debug + HasSize + Send + Sync + 'static,
    D: Clone + Debug + Send + Sync + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Returns a future that resolves to the value [`CacheRequestResult<V>`].
    /// If data is loading, provides early access data (`D`).
    ///
    /// For a given key, this function will return a value immediately if it is already cached.
    /// If it is not cached and not already loading, it will start a new fetch process
    /// using the early access data (`D`) provided by the caller.
    ///
    /// If a fetch process is already in progress, it will return a different early access (`D`)
    /// which is tied to the ongoing fetch process.
    fn get_or_fetch(
        &self,
        k: &K,
        f: CacheFn<V>,
        d: D,
    ) -> (
        BoxFuture<'static, CacheRequestResult<V>>,
        Option<D>,
        CacheState,
    ) {
        match self.get_or_fetch_impl(k, f, d) {
            S3CacheResponse::NewEntry(load) => {
                let early_access = load.data().clone();
                let fut = load.into_future();

                (Box::pin(fut), Some(early_access), CacheState::NewEntry)
            }
            S3CacheResponse::AlreadyLoading(load) => {
                let early_access = load.data().clone();
                let fut = load.into_future();

                (
                    Box::pin(fut),
                    Some(early_access),
                    CacheState::AlreadyLoading,
                )
            }
            S3CacheResponse::WasCached(res) => {
                (Box::pin(async move { res }), None, CacheState::WasCached)
            }
        }
    }

    fn get(&self, k: &K) -> Option<CacheRequestResult<V>> {
        self.cache.get(k).map(|entry| Ok(entry.value().clone()))
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    fn prune(&self) {
        // Intentionally unimplemented, S3Fifo handles its own pruning
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    use crate::cache_system::{
        hook::test_utils::{NoOpHook, TestHook, TestHookRecord},
        s3_fifo_cache::s3_fifo::{
            Version, VersionedSnapshot,
            test_migration::{TestNewLockedState, assert_versioned_snapshot},
        },
        test_utils::{TestSetup, TestValue, gen_cache_tests, runtime_shutdown},
    };

    use futures::future::FutureExt;

    #[test]
    fn test_runtime_shutdown() {
        runtime_shutdown(setup());
    }

    #[tokio::test]
    async fn test_ghost_set_is_limited() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                max_memory_size: 10,
                max_ghost_memory_size: 10,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
            },
            &metric::Registry::new(),
        );
        let k1 = Arc::from("x");

        let (res, _, state) = cache.get_or_fetch(
            &k1,
            Box::new(|| futures::future::ready(Ok(Arc::from("value"))).boxed()),
            (),
        );
        assert_eq!(state, CacheState::NewEntry);
        res.await.unwrap();

        assert_ne!(Arc::strong_count(&k1), 1);

        for i in 0..100 {
            let k = Arc::from(i.to_string());
            let (res, _, state) = cache.get_or_fetch(
                &k,
                Box::new(|| futures::future::ready(Ok(Arc::from("value"))).boxed()),
                (),
            );
            assert_eq!(state, CacheState::NewEntry);
            res.await.unwrap();
        }

        assert_eq!(Arc::strong_count(&k1), 1);
    }

    #[tokio::test]
    async fn test_evict_previously_heavy_used_key() {
        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, Arc<TestValue>, ()>::new(
            S3Config {
                max_memory_size: 10,
                max_ghost_memory_size: 10,
                move_to_main_threshold: 0.5,
                hook: Arc::clone(&hook) as _,
            },
            &metric::Registry::new(),
        );

        let k_heavy = Arc::from("heavy");

        // make it heavy
        for _ in 0..2 {
            let (res, _, _state) = cache.get_or_fetch(
                &k_heavy,
                Box::new(|| futures::future::ready(Ok(Arc::new(TestValue(5)))).boxed()),
                (),
            );
            res.await.unwrap();
        }

        // add new keys
        let k_new = Arc::from("new");

        // make them heavy enough to evict old data
        for _ in 0..2 {
            let (res, _, _state) = cache.get_or_fetch(
                &k_new,
                Box::new(|| futures::future::ready(Ok(Arc::new(TestValue(5)))).boxed()),
                (),
            );
            res.await.unwrap();
        }

        // old heavy key is gone
        assert!(cache.get(&k_heavy).is_none());

        assert_eq!(
            hook.records(),
            vec![
                TestHookRecord::Insert(0, Arc::clone(&k_heavy)),
                TestHookRecord::Fetched(0, Arc::clone(&k_heavy), Ok(71)),
                TestHookRecord::Insert(1, Arc::clone(&k_new)),
                TestHookRecord::Fetched(1, Arc::clone(&k_new), Ok(67)),
                TestHookRecord::Evict(0, Arc::clone(&k_heavy), EvictResult::Fetched { size: 71 }),
            ],
        );
    }

    gen_cache_tests!(setup);

    fn setup() -> TestSetup {
        let observer = Arc::new(TestHook::default());
        TestSetup {
            cache: Arc::new(S3FifoCache::<_, _, ()>::new(
                S3Config {
                    max_memory_size: 10_000,
                    max_ghost_memory_size: 10_000,
                    move_to_main_threshold: 0.25,
                    hook: Arc::clone(&observer) as _,
                },
                &metric::Registry::new(),
            )),
            observer,
        }
    }

    // Snapshot functionality tests
    #[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
    struct SnapshotTestKey(String);

    impl HasSize for SnapshotTestKey {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    #[derive(Debug, Clone, PartialEq, bincode::Encode)]
    struct SnapshotTestValue(Vec<u8>);

    impl HasSize for SnapshotTestValue {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    // bincode Decode implementation for SnapshotTestValue
    impl<Q> bincode::Decode<Q> for SnapshotTestValue {
        fn decode<D: bincode::de::Decoder<Context = Q>>(
            decoder: &mut D,
        ) -> Result<Self, bincode::error::DecodeError> {
            let value: Vec<u8> = bincode::Decode::decode(decoder)?;
            Ok(Self(value))
        }
    }

    #[tokio::test]
    async fn test_snapshot_serialization_roundtrip() {
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());

        // Test with multiple entries to ensure comprehensive serialization
        let test_data = vec![
            (
                SnapshotTestKey("key1".to_string()),
                SnapshotTestValue(vec![10, 20, 30]),
            ),
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Create the cache with size limits such that we populate the ghost set too
        // such that the OrderedSet (de)serialization is tested.
        let key_size = test_data[0].0.size();
        let value_size = Arc::new(test_data[0].1.clone()).size();
        let cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> = S3FifoCache::new(
            S3Config {
                max_memory_size: ((key_size + value_size) as f32 * 2.9).round() as usize,
                max_ghost_memory_size: (key_size as f32 * 1.9).round() as usize,
                move_to_main_threshold: 0.1,
                hook: Arc::clone(&hook),
            },
            &metric_registry,
        );

        // Insert all test data
        for (key, value) in &test_data {
            let value_clone = value.clone();
            let (res, _, _state) = cache.get_or_fetch(
                key,
                Box::new(move || async move { Ok(value_clone) }.boxed()),
                (),
            );
            res.await.unwrap();
        }

        // Create snapshot
        let snapshot = cache.snapshot().unwrap();
        assert!(!snapshot.is_empty(), "Snapshot should contain data");

        // Verify that the last inserted entries is still accessible in the original cache
        assert_eq!(cache.len(), 2, "Cache should contain 2 entries");
        for (key, expected_value) in &test_data[2..] {
            let retrieved_value = cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch for key: {key:?}",
            );
        }

        // assert that we had 1 ghost entry (& therefore these tests cover the OrderedSet serialization)
        assert_eq!(
            cache.cache.ghost_len(),
            1,
            "Cache should have 1 ghost entry"
        );

        // Create a new cache from the snapshot
        let restored_cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> =
            S3FifoCache::new_from_snapshot(
                S3Config {
                    max_memory_size: 1000,
                    max_ghost_memory_size: 500,
                    move_to_main_threshold: 0.1,
                    hook: Arc::clone(&hook),
                },
                &metric_registry,
                &snapshot,
                &(),
            )
            .unwrap();

        // Verify last two entries are preserved in the restored cache
        assert_eq!(
            restored_cache.len(),
            2,
            "Restored cache should contain 2 entries"
        );
        for (key, expected_value) in &test_data[2..] {
            let retrieved_value = restored_cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch in restored cache for key: {key:?}"
            );
        }

        // Verify the cache lengths match
        assert_eq!(
            cache.len(),
            restored_cache.len(),
            "Cache lengths should match after restore"
        );

        // assert that we had 1 ghost entry in restored cache (OrderedSet deserialization)
        assert_eq!(
            restored_cache.cache.ghost_len(),
            1,
            "Restored cache should have 1 ghost entry"
        );
    }

    #[tokio::test]
    async fn test_snapshot_versioning() {
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());

        // Test with multiple entries to ensure comprehensive serialization
        let test_data = vec![
            (
                SnapshotTestKey("key1".to_string()),
                SnapshotTestValue(vec![10, 20, 30]),
            ),
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Create the cache with all entries.
        let cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> = S3FifoCache::new(
            S3Config {
                max_memory_size: 10_000,
                max_ghost_memory_size: 10_000,
                move_to_main_threshold: 0.1,
                hook: Arc::clone(&hook),
            },
            &metric_registry,
        );

        // Insert all test data
        for (key, value) in &test_data {
            let value_clone = value.clone();
            let (res, _, _state) = cache.get_or_fetch(
                key,
                Box::new(move || async move { Ok(value_clone) }.boxed()),
                (),
            );
            res.await.unwrap();
        }

        // Create snapshot
        let snapshot = cache.snapshot().unwrap();
        assert!(!snapshot.is_empty(), "Snapshot should contain data");

        // Snapshot can be deserialized into new version.
        // (This would be the code used in the S3Fifo::deserialize_snapshot).
        let (versioned_snapshot, _): (
            VersionedSnapshot<TestNewLockedState<SnapshotTestKey, SnapshotTestValue>>,
            usize,
        ) = bincode::decode_from_slice_with_context(&snapshot, bincode::config::standard(), ())
            .unwrap();

        // Assert that we migrated to the new/updated Test version.
        assert_versioned_snapshot(&versioned_snapshot, &test_data, Version::Test);
    }

    #[tokio::test]
    async fn test_snapshot_restore_from_v1_static_data() {
        let snapshot_data = &[
            // oldest entry is in the ghost queue (a.k.a. OrderedSet)
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            // two entries are in the Fifo cache queue
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Static snapshot data representing a Version::V1 snapshot
        // of the above `snapshot_data`.
        //
        // The snapshot format for V1 is:
        // [version_byte(1), serialized_locked_state...]
        let v1_snapshot_data: &[u8] = &[
            1, 0, 0, 2, 4, 107, 101, 121, 51, 4, 60, 70, 80, 90, 2, 0, 4, 107, 101, 121, 52, 3,
            100, 110, 120, 3, 0, 159, 1, 1, 4, 107, 101, 121, 50, 28, 0, 0,
        ];

        // Verify that the V1 snapshot data starts with VERSION_V1 (1)
        assert_eq!(
            v1_snapshot_data[0], 1,
            "V1 snapshot should start with VERSION_V1 (1)"
        );

        // Create a new cache from the static V1 snapshot data
        // This tests that S3FifoCache::new_from_snapshot can restore from V1 format
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());
        let restored_cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> =
            S3FifoCache::new_from_snapshot(
                S3Config {
                    max_memory_size: 10_000,
                    max_ghost_memory_size: 10_000,
                    move_to_main_threshold: 0.1,
                    hook: Arc::clone(&hook),
                },
                &metric_registry,
                v1_snapshot_data,
                &(),
            )
            .unwrap();

        // Verify last two entries are preserved in the restored cache
        assert_eq!(
            restored_cache.len(),
            2,
            "Restored cache should contain 2 entries"
        );
        for (key, expected_value) in &snapshot_data[1..] {
            let retrieved_value = restored_cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch in restored cache for key: {key:?}"
            );
        }

        // assert that we had 1 ghost entry in restored cache (OrderedSet deserialization)
        assert_eq!(
            restored_cache.cache.ghost_len(),
            1,
            "Restored cache should have 1 ghost entry"
        );
    }

    #[tokio::test]
    async fn test_get_or_fetch_with_early_access() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, Arc<str>>::new(
            S3Config {
                max_memory_size: 1000,
                max_ghost_memory_size: 500,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
            },
            &metric::Registry::new(),
        );

        let key = Arc::from("test_key");
        let early_access_data = Arc::from("early_data");
        let final_value = Arc::from("final_value");

        /* Test case 1: New entry - should return future and early access data */
        let final_value_clone = Arc::clone(&final_value);
        let (got_fut, got_early_access, got_state) = cache.get_or_fetch(
            &key,
            Box::new(move || {
                let value = Arc::clone(&final_value_clone);
                async move {
                    // Simulate some async work
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Ok(value)
                }
                .boxed()
            }),
            Arc::clone(&early_access_data),
        );

        // Verify return signature for new entry
        assert_eq!(got_state, CacheState::NewEntry);
        assert!(matches!(got_early_access, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        // Await the future and verify it returns the expected value
        let result = got_fut.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), final_value);

        /* Test case 2: Already cached - should return immediate result with no early access */
        let (got_fut, got_early_access, got_state) = cache.get_or_fetch(
            &key,
            Box::new(|| async { panic!("should not be called") }.boxed()),
            Arc::from("unused_early_data"),
        );

        // Verify return signature for cached entry
        assert_eq!(got_state, CacheState::WasCached);
        assert!(got_early_access.is_none());

        // Await the future and verify it returns the cached value
        let result = got_fut.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), final_value);

        /* Test case 3: Already loading - simulate concurrent access */
        let key = Arc::from("new_test_key");
        let early_access_data = Arc::from("new_early_data");
        let final_value = Arc::from("new_final_value");

        // Start first request (this will be loading)
        let final_value_clone = Arc::clone(&final_value);
        let (got_fut_1, got_early_access_1, got_state_1) = cache.get_or_fetch(
            &key,
            Box::new(move || {
                let value = Arc::clone(&final_value_clone);
                async move {
                    // Simulate longer async work
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Ok(value)
                }
                .boxed()
            }),
            Arc::clone(&early_access_data),
        );

        // Start second request while first is still loading
        let different_early_access = Arc::from("different_early_data");
        let (got_fut_2, got_early_access_2, state_2) = cache.get_or_fetch(
            &key,
            Box::new(|| async { panic!("should not be called") }.boxed()),
            different_early_access,
        );

        // Verify return signatures
        assert_eq!(got_state_1, CacheState::NewEntry);
        assert!(matches!(got_early_access_1, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        assert_eq!(state_2, CacheState::AlreadyLoading);
        // The second request should get the early access data from the ongoing load
        assert!(matches!(got_early_access_2, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        // Both futures should resolve to the same value
        let (result1, result2) = tokio::join!(got_fut_1, got_fut_2);
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap(), final_value);
        assert_eq!(result2.unwrap(), final_value);
    }

    #[tokio::test]
    async fn test_list_fn_only_includes_fully_loaded_entries() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                max_memory_size: 1000,
                max_ghost_memory_size: 500,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
            },
            &metric::Registry::new(),
        );

        let key1 = Arc::from("key1".to_string());
        let key2 = Arc::from("key2".to_string());

        // Add first entry
        let (res1, _, _) = cache.get_or_fetch(
            &key1,
            Box::new(|| futures::future::ready(Ok(Arc::from("value1"))).boxed()),
            (),
        );
        res1.await.unwrap();

        // Add second entry
        let (res2, _, _) = cache.get_or_fetch(
            &key2,
            Box::new(|| {
                async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Ok(Arc::from("value2"))
                }
                .boxed()
            }),
            (),
        );

        // List keys in the cache
        let keys: Vec<Arc<str>> = cache.list().map(Arc::unwrap_or_clone).collect();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&key1), "key1 should be in the cache");
        assert!(
            !keys.contains(&key2),
            "key2 should not be in the cache::list() yet, since still loading"
        );

        // wait until second entry is loaded
        res2.await.unwrap();

        // Now both keys should be in the cache::list()
        let keys: Vec<Arc<str>> = cache.list().map(Arc::unwrap_or_clone).collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1), "key1 should be in the cache");
        assert!(keys.contains(&key2), "key2 should be in the cache");
    }
}
