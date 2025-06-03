use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::{
    path::Path, DynObjectStore, Error, GetResult, GetResultPayload, ObjectMeta, PutPayload,
};

/// Abstract test setup.
///
/// Should be used together with [`gen_store_tests`].
pub trait Setup: Send {
    /// Initialize test setup.
    ///
    /// You may assume that the resulting object is kept around for the entire test. This may be helpful to keep file
    /// handles etc. around.
    fn new() -> BoxFuture<'static, Self>;

    /// Get inner/underlying, uncached store.
    ///
    /// This store MUST be empty.
    fn inner(&self) -> &Arc<MockStore>;

    /// Get outer, cached store.
    ///
    /// This store MUST reject writes.
    fn outer(&self) -> &Arc<DynObjectStore>;
}

fn get_result(data: &'static [u8], path: &Path) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
            Ok(Bytes::from_static(data))
        }))),
        meta: ObjectMeta {
            location: path.clone(),
            last_modified: Default::default(),
            size: data.len(),
            e_tag: Some(format!("etag-{path}")),
            version: None,
        },
        range: 0..data.len(),
        attributes: Default::default(),
    }
}

pub async fn test_etag<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location_a = Path::parse("x").unwrap();
    let location_b = Path::parse("y").unwrap();

    Arc::clone(setup.inner())
        .mock_next(object_store_mock::MockCall::GetOpts {
            params: (location_a.clone(), Default::default()),
            barriers: vec![],
            res: Ok(get_result(b"foo", &location_a)),
        })
        .mock_next(object_store_mock::MockCall::GetOpts {
            params: (location_b.clone(), Default::default()),
            barriers: vec![],
            res: Ok(get_result(b"bar", &location_b)),
        });

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

pub async fn test_found<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location_a = Path::parse("x").unwrap();
    let location_b = Path::parse("y").unwrap();

    Arc::clone(setup.inner())
        .mock_next(object_store_mock::MockCall::GetOpts {
            params: (location_a.clone(), Default::default()),
            barriers: vec![],
            res: Ok(get_result(b"foo", &location_a)),
        })
        .mock_next(object_store_mock::MockCall::GetOpts {
            params: (location_b.clone(), Default::default()),
            barriers: vec![],
            res: Ok(get_result(b"bar", &location_b)),
        });

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

pub async fn test_not_found<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location = Path::parse("x").unwrap();

    Arc::clone(setup.inner()).mock_next(object_store_mock::MockCall::GetOpts {
        params: (location.clone(), Default::default()),
        barriers: vec![],
        res: Err(Error::NotFound {
            path: location.to_string(),
            source: "foo".to_owned().into(),
        }),
    });

    let err = setup.outer().get(&location).await.unwrap_err();
    assert!(
        matches!(err, Error::NotFound { .. }),
        "error should be 'not found' but is: {err}"
    );
}

pub async fn test_reads_cached<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location = Path::parse("x").unwrap();

    Arc::clone(setup.inner()).mock_next(object_store_mock::MockCall::GetOpts {
        params: (location.clone(), Default::default()),
        barriers: vec![],
        res: Ok(get_result(b"foo", &location)),
    });
    let res_1 = setup.outer().get(&location).await.unwrap();
    assert_eq!(
        CacheState::try_from(res_1.attributes.get(&ATTR_CACHE_STATE).unwrap()).unwrap(),
        CacheState::NewEntry,
    );
    let data_1 = res_1.bytes().await.unwrap();
    assert_eq!(data_1.as_ref(), b"foo");

    let res_2 = setup.outer().get(&location).await.unwrap();
    assert_eq!(
        CacheState::try_from(res_2.attributes.get(&ATTR_CACHE_STATE).unwrap()).unwrap(),
        CacheState::WasCached,
    );
    let data_2 = res_2.bytes().await.unwrap();
    assert_eq!(data_1, data_2);
}

pub async fn test_writes_rejected<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location = Path::parse("x").unwrap();

    let err = setup
        .outer()
        .put(&location, PutPayload::from_static(b"foo"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotImplemented),
        "error should be 'not implemented' but is: {err}"
    );
}

pub async fn test_size_hinting<S>()
where
    S: Setup,
{
    let setup = S::new().await;

    let location = Path::parse("x").unwrap();
    let data = b"foo";

    Arc::clone(setup.inner()).mock_next(object_store_mock::MockCall::GetOpts {
        params: (location.clone(), hint_size(data.len()).into()),
        barriers: vec![],
        res: Ok(get_result(data, &location)),
    });

    let data = setup
        .outer()
        .get_opts(&location, hint_size(data.len()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), data);
}

#[macro_export]
macro_rules! gen_store_tests_impl {
        ($setup:ident, [$($test:ident,)+ $(,)?] $(,)?) => {
            $(
                #[tokio::test]
                async fn $test(){
                    $crate::object_store_cache_tests::$test::<$setup>().await;
                }
            )+
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
/// use object_store_mock::MockStore;
///
/// struct TestSetup {
///     // ...
/// }
///
/// impl Setup for TestSetup {
///     fn new() -> BoxFuture<'static, Self> {
///         todo!()
///     }
///
///     fn inner(&self) -> &Arc<MockStore> {
///         todo!()
///     }
///
///     fn outer(&self) -> &Arc<DynObjectStore> {
///         todo!()
///     }
/// }
///
/// gen_store_tests!(TestSetup);
/// ```
#[macro_export]
macro_rules! gen_store_tests {
    ($setup:ident) => {
        $crate::object_store_cache_tests::gen_store_tests_impl!(
            $setup,
            [
                test_etag,
                test_found,
                test_not_found,
                test_reads_cached,
                test_writes_rejected,
                test_size_hinting,
            ],
        );
    };
}

pub use gen_store_tests;

use object_store_metrics::cache_state::{CacheState, ATTR_CACHE_STATE};
use object_store_mock::MockStore;
use object_store_size_hinting::hint_size;
