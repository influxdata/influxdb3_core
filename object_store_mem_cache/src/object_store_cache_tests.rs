use std::sync::Arc;

use futures::future::BoxFuture;
use object_store::{path::Path, DynObjectStore, Error, PutPayload};

/// Abstract test setup.
///
/// Should be used together with [`gen_store_tests`].
pub trait Setup: Send {
    /// Initialize test setup.
    ///
    /// You may assume that the resulting object is kept around for the entire test. This may be helpful to keep file
    /// handles etc. around.
    fn new(use_s3fifo: bool) -> BoxFuture<'static, Self>;

    /// Get inner/underlying, uncached store.
    ///
    /// This store MUST support writes.
    fn inner(&self) -> &Arc<DynObjectStore>;

    /// Get outer, cached store.
    ///
    /// This store MUST reject writes.
    fn outer(&self) -> &Arc<DynObjectStore>;
}

pub async fn test_etag<S>(use_s3fifo: bool)
where
    S: Setup,
{
    let setup = S::new(use_s3fifo).await;

    let location_a = Path::parse("x").unwrap();
    let location_b = Path::parse("y").unwrap();

    setup
        .inner()
        .put(&location_a, PutPayload::from_static(b"foo"))
        .await
        .unwrap();
    setup
        .inner()
        .put(&location_b, PutPayload::from_static(b"bar"))
        .await
        .unwrap();

    let etag_a1 = setup
        .outer()
        .head(&location_a)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let etag_a2 = setup
        .outer()
        .head(&location_a)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let etag_b1 = setup
        .outer()
        .head(&location_b)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let etag_b2 = setup
        .outer()
        .head(&location_b)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    assert_eq!(etag_a1, etag_a2);
    assert_eq!(etag_b1, etag_b2);
    assert_ne!(etag_a1, etag_b1);
}

pub async fn test_found<S>(use_s3fifo: bool)
where
    S: Setup,
{
    let setup = S::new(use_s3fifo).await;

    let location_a = Path::parse("x").unwrap();
    let location_b = Path::parse("y").unwrap();

    setup
        .inner()
        .put(&location_a, PutPayload::from_static(b"foo"))
        .await
        .unwrap();
    setup
        .inner()
        .put(&location_b, PutPayload::from_static(b"bar"))
        .await
        .unwrap();

    let data_a = setup
        .outer()
        .get(&location_a)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let data_b = setup
        .outer()
        .get(&location_b)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data_a.as_ref(), b"foo");
    assert_eq!(data_b.as_ref(), b"bar");
}

pub async fn test_not_found<S>(use_s3fifo: bool)
where
    S: Setup,
{
    let setup = S::new(use_s3fifo).await;

    let location = Path::parse("x").unwrap();

    let err = setup.inner().get(&location).await.unwrap_err();
    assert!(
        matches!(err, Error::NotFound { .. }),
        "error should be 'not found' but is: {err}"
    );
}

pub async fn test_reads_cached<S>(use_s3fifo: bool)
where
    S: Setup,
{
    let setup = S::new(use_s3fifo).await;

    let location = Path::parse("x").unwrap();

    setup
        .inner()
        .put(&location, PutPayload::from_static(b"foo"))
        .await
        .unwrap();
    let res_1 = setup.outer().get(&location).await.unwrap();
    assert_eq!(
        CacheState::try_from(res_1.attributes.get(&ATTR_CACHE_STATE).unwrap()).unwrap(),
        CacheState::NewEntry,
    );
    let data_1 = res_1.bytes().await.unwrap();
    assert_eq!(data_1.as_ref(), b"foo");

    setup
        .inner()
        .put(&location, PutPayload::from_static(b"bar"))
        .await
        .unwrap();

    let res_2 = setup.outer().get(&location).await.unwrap();
    assert_eq!(
        CacheState::try_from(res_2.attributes.get(&ATTR_CACHE_STATE).unwrap()).unwrap(),
        CacheState::WasCached,
    );
    let data_2 = res_2.bytes().await.unwrap();
    assert_eq!(data_1, data_2);
}

pub async fn test_writes_rejected<S>(use_s3fifo: bool)
where
    S: Setup,
{
    let setup = S::new(use_s3fifo).await;

    let location = Path::parse("x").unwrap();

    let err = setup
        .outer()
        .put(&location, PutPayload::from_static(b"foo"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotImplemented { .. }),
        "error should be 'not implemented' but is: {err}"
    );
}

#[macro_export]
macro_rules! gen_store_tests_impl {
        ($setup:ident, $s3fifo:expr, [$($test:ident,)+ $(,)?] $(,)?) => {
            paste::paste! {
                $(
                    #[tokio::test]
                    async fn [<$test _s3fifo _$s3fifo>](){
                        $crate::object_store_cache_tests::$test::<$setup>($s3fifo).await;
                    }
                )+
            }
        };
    }

pub use gen_store_tests_impl;

/// Generate tests in current module.
///
/// # Example
/// ```
/// use std::sync::Arc;
/// use futures::future::BoxFuture;
/// use object_store::DynObjectStore;
/// use object_store_mem_cache::object_store_cache_tests::{
///     gen_store_tests,
///     Setup,
/// };
///
/// struct TestSetup {
///     // ...
/// }
///
/// impl Setup for TestSetup {
///     fn new(_: bool) -> BoxFuture<'static, Self> {
///         todo!()
///     }
///
///     fn inner(&self) -> &Arc<DynObjectStore> {
///         todo!()
///     }
///
///     fn outer(&self) -> &Arc<DynObjectStore> {
///         todo!()
///     }
/// }
///
/// gen_store_tests!(TestSetup, false);
/// ```
#[macro_export]
macro_rules! gen_store_tests {
    ($setup:ident, $s3fifo:expr) => {
        $crate::object_store_cache_tests::gen_store_tests_impl!(
            $setup,
            $s3fifo,
            [
                test_etag,
                test_found,
                test_not_found,
                test_reads_cached,
                test_writes_rejected,
            ],
        );
    };
}

pub use gen_store_tests;

use crate::{attributes::ATTR_CACHE_STATE, cache_system::CacheState};
