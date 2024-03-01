//! Integration tests using the client & server.

use std::{
    convert::Infallible,
    fs::create_dir_all,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use assert_matches::assert_matches;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use data_types::{ColumnSet, ParquetFileParams, PartitionId, Timestamp};
use hyper::{
    http,
    service::{make_service_fn, service_fn, Service},
    Body,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet_file::ParquetFilePath;
use tempfile::{tempdir, NamedTempFile, TempDir};

use crate::{
    build_cache_server,
    data_types::{PolicyConfig, WriteHintAck},
    make_client,
    server::mock::create_mock_raw_parquet_file,
    ParquetCacheServer, ParquetCacheServerConfig, WriteHintingObjectStore,
};

/// Handle the waits for a file to exist during testing.
///
/// This is especially important for write-backs where
/// the test is not explicitly awaiting an Ack::Complete.
pub async fn wait_for_file_to_exist(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let now = tokio::time::Instant::now();

    while now.elapsed().as_secs() < 10 {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        if path.exists() {
            return Ok(());
        }
    }
    Err("expected file did not exist within 10 seconds".into())
}

#[derive(Debug)]
pub struct TestParquetCache {
    client: Arc<dyn WriteHintingObjectStore>,
    _server: ParquetCacheServer,
    direct_store: Arc<dyn ObjectStore>,
    local_store_dir: TempDir,
    _remote_store_dir: TempDir,
    _configmap: NamedTempFile,
    _join: tokio::task::JoinHandle<()>,
}

impl TestParquetCache {
    /// Create a new ParquetCache for testing.
    pub async fn new(config: PolicyConfig) -> Self {
        // dirs underlying local & remote object stores
        let remote_store_dir = tempdir().unwrap();
        let local_store_dir = tempdir().unwrap();

        // server addr
        let listener = tokio::net::TcpListener::bind("localhost:0")
            .await
            .expect("listener should have bound to addr");
        let addr = listener.local_addr().unwrap();

        // configmap file
        let mut configmap = NamedTempFile::new().unwrap();
        writeln!(configmap, r#"{{"instances":["{}"],"revision":0}}"#, addr)
            .expect("should write keyspace definition to configmap file");

        // remote object_store
        create_dir_all(remote_store_dir.path()).unwrap();
        let direct_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(remote_store_dir.path())
                .expect("should create fs ObjectStore"),
        );

        // local object_store (a.k.a. parquet cache)
        let catalog = iox_tests::TestCatalog::new();
        let config = ParquetCacheServerConfig {
            keyspace_config_path: configmap.path().to_string_lossy().to_string(),
            hostname: addr.to_string(),
            local_dir: local_store_dir.path().to_string_lossy().to_string(),
            policy_config: config,
            prewarming_table_concurrency: 10,
        };
        let server = build_cache_server(config, Arc::clone(&direct_store), catalog.catalog()).await;
        let _server = server.clone();

        // run cache server
        let make_svc = make_service_fn(move |_socket| {
            let server_captured = server.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: http::Request<Body>| {
                    let mut svc = server_captured.clone();
                    async move {
                        std::future::poll_fn(|cx| svc.poll_ready(cx))
                            .await
                            .expect("server should be ready");

                        svc.call(req).await
                    }
                }))
            }
        });
        let join = tokio::spawn(async {
            hyper::Server::builder(
                hyper::server::conn::AddrIncoming::from_listener(listener).unwrap(),
            )
            .http2_only(true)
            .serve(make_svc)
            .await
            .unwrap()
        });

        // client (a.k.a. object store interface)
        let client = make_client(addr.to_string(), Arc::clone(&direct_store));

        Self {
            client,
            _server,
            direct_store,
            local_store_dir,
            _remote_store_dir: remote_store_dir,
            _configmap: configmap,
            _join: join,
        }
    }

    /// Returns the client object store interface.
    pub fn client(&self) -> Arc<dyn WriteHintingObjectStore> {
        Arc::clone(&self.client)
    }

    /// Returns the remote object store interface.
    ///
    /// Can be used to directly add objects to the remote store.
    pub fn direct_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.direct_store)
    }

    /// Check if the object is in the cache.
    pub fn cache_contains(&self, parquet_file: ParquetFilePath) -> bool {
        let path = self
            .local_store_dir
            .path()
            .join(parquet_file.object_store_path().as_ref());
        path.exists()
    }

    /// Get local sotre path
    pub fn local_store_dir(&self) -> &Path {
        self.local_store_dir.path()
    }
}

fn make_parquet_file_params(
    parquet_file: ParquetFilePath,
    size: usize,
    created_at: DateTime<Utc>,
) -> ParquetFileParams {
    let ts = Timestamp::new(created_at.timestamp_nanos_opt().unwrap());
    ParquetFileParams {
        namespace_id: parquet_file.namespace_id(),
        table_id: parquet_file.table_id(),
        partition_id: PartitionId::new(0),
        partition_hash_id: None,
        object_store_id: parquet_file.object_store_id(),
        min_time: ts - 1000,
        max_time: ts - 1,
        file_size_bytes: size as i64,
        row_count: 1,
        compaction_level: data_types::CompactionLevel::Final,
        created_at: ts,
        column_set: ColumnSet::new(vec![]),
        max_l0_created_at: ts,
    }
}

async fn put_in_remote_store(
    test_cache: Arc<TestParquetCache>,
    parquet_file: ParquetFilePath,
    file_bytes: Vec<u8>,
) {
    let file_size_bytes = file_bytes.len();

    test_cache
        .direct_store()
        .put(&parquet_file.object_store_path(), Bytes::from(file_bytes))
        .await
        .expect("should write to remote store");

    let confirm_in_remote = test_cache
        .direct_store()
        .head(&parquet_file.object_store_path())
        .await;

    assert_matches!(
        confirm_in_remote,
        Ok(object_store::ObjectMeta {
            size,
            ..
        }) if size == file_size_bytes,
        "should be in remote store, instead found {:?}", confirm_in_remote
    );
    assert!(
        !test_cache.cache_contains(parquet_file.clone()),
        "should not be in cache"
    );
}

#[tokio::test]
async fn test_parquet_cache_write_hint_completed() {
    let policy_config = PolicyConfig {
        max_capacity: 3_200_000_000,
        event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
    };
    let test_cache = Arc::new(TestParquetCache::new(policy_config).await);

    // parquet file
    let location = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet".to_string();
    let parquet_file = ParquetFilePath::try_from(&location).unwrap();
    let created_at = Utc::now();
    let (file_bytes, _) = create_mock_raw_parquet_file(parquet_file.clone(), created_at).await;
    let file_params = make_parquet_file_params(parquet_file.clone(), file_bytes.len(), created_at);

    // object in remote store, not local cache
    put_in_remote_store(Arc::clone(&test_cache), parquet_file.clone(), file_bytes).await;

    // write hint
    let resp = test_cache
        .client()
        .write_hint(
            &parquet_file.object_store_path(),
            &file_params,
            WriteHintAck::Completed,
        )
        .await;
    assert!(
        resp.is_ok(),
        "should return OK for write hint, instead found {:?}",
        resp
    );

    // object is in local cache
    assert!(
        test_cache.cache_contains(parquet_file),
        "should be in cache"
    );
}

#[tokio::test]
async fn test_parquet_cache_write_hint_received() {
    let policy_config = PolicyConfig {
        max_capacity: 3_200_000_000,
        event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
    };
    let test_cache = Arc::new(TestParquetCache::new(policy_config).await);

    // parquet file
    let location = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet".to_string();
    let parquet_file = ParquetFilePath::try_from(&location).unwrap();
    let created_at = Utc::now();
    let (file_bytes, _) = create_mock_raw_parquet_file(parquet_file.clone(), created_at).await;
    let file_params = make_parquet_file_params(parquet_file.clone(), file_bytes.len(), created_at);

    // object in remote store, not local cache
    put_in_remote_store(Arc::clone(&test_cache), parquet_file.clone(), file_bytes).await;

    // write hint
    let resp = test_cache
        .client()
        .write_hint(
            &parquet_file.object_store_path(),
            &file_params,
            WriteHintAck::Received,
        )
        .await;
    assert!(
        resp.is_ok(),
        "should return OK for write hint, instead found {:?}",
        resp
    );

    let writeback_local_store_path = PathBuf::from(test_cache.local_store_dir())
        .join(parquet_file.object_store_path().to_string());
    wait_for_file_to_exist(writeback_local_store_path.as_path())
        .await
        .expect("should succeed on writeback");

    // object is in local cache
    assert!(
        test_cache.cache_contains(parquet_file),
        "should be in cache"
    );
}

#[tokio::test]
async fn test_parquet_cache_write_hint_sent() {
    let policy_config = PolicyConfig {
        max_capacity: 3_200_000_000,
        event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
    };
    let test_cache = Arc::new(TestParquetCache::new(policy_config).await);

    // parquet file
    let location = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet".to_string();
    let parquet_file = ParquetFilePath::try_from(&location).unwrap();
    let created_at = Utc::now();
    let (file_bytes, _) = create_mock_raw_parquet_file(parquet_file.clone(), created_at).await;
    let file_params = make_parquet_file_params(parquet_file.clone(), file_bytes.len(), created_at);

    // object in remote store, not local cache
    put_in_remote_store(Arc::clone(&test_cache), parquet_file.clone(), file_bytes).await;

    // write hint
    let resp = test_cache
        .client()
        .write_hint(
            &parquet_file.object_store_path(),
            &file_params,
            WriteHintAck::Sent,
        )
        .await;
    assert!(
        resp.is_ok(),
        "should return OK for write hint, instead found {:?}",
        resp
    );

    let writeback_local_store_path = PathBuf::from(test_cache.local_store_dir())
        .join(parquet_file.object_store_path().to_string());
    wait_for_file_to_exist(writeback_local_store_path.as_path())
        .await
        .expect("should succeed on writeback");

    // object is in local cache
    assert!(
        test_cache.cache_contains(parquet_file),
        "should be in cache"
    );
}
