#![allow(dead_code)]
//! Contains the cache server.
//!
//! The cache server is divided into different tower service layers, as follows:
//!
//! ┌──────────────────────────────────────────────────┐
//! │ CacheService                                     │
//! │                                                  │
//! │   Handles self-issued requests.                  │
//! │   * prewarming of cache.                         │
//! └────────────────────────┬─────────────────────────┘
//!                          │
//!                          │
//! ┌────────────────────────▼─────────────────────────┐
//! │ KeyspaceService                                  │
//! │                                                  │
//! │   Handles consistent hashing of keyspace.        │
//! │   * reads the keyspace definition (configmap).   │
//! │   * confirms request is within keyspace.         │
//! │   * handle `GET /keyspace` requests              │
//! └────────────────────────┬─────────────────────────┘
//!                          │
//!                          │
//! ┌────────────────────────▼─────────────────────────┐
//! │ PreconditionService                              │
//! │                                                  │
//! │   Handles preconditions applied per request.     │
//! │   * object store GetOptions                      │
//! └────────────────────────┬─────────────────────────┘
//!                          │
//!                          │
//! ┌────────────────────────▼─────────────────────────┐
//! │ DataService                                      │
//! │                                                  │
//! │   Handles the READ and WRITE of data.            │
//! │   * cache management (insertion & eviction)      │
//! │   * local store connection                       │
//! │   * performs write-back                          │
//! └──────────────────────────────────────────────────┘
//!

use std::sync::Arc;

use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use tower::ServiceBuilder;

use crate::data_types::PolicyConfig;

use self::{
    cache::{BuildCacheService, CacheService},
    data::DataService,
    keyspace::{BuildKeyspaceService, KeyspaceService},
    precondition::{BuildPreconditionService, PreconditionService},
};

// Layers in the cache server:
mod cache;
mod data;
mod keyspace;
mod precondition;

// Shared server types:
mod error;
pub use error::Error as ServerError;
mod response;

#[cfg(test)]
pub(crate) mod mock;

/// The cache server type.
pub type ParquetCacheServer = CacheService<KeyspaceService<PreconditionService<DataService>>>;

/// Config for cache server.
#[derive(Debug, Clone)]
pub struct ParquetCacheServerConfig {
    /// The path to the config file for the keyspace.
    pub keyspace_config_path: String,
    /// The hostname of the cache instance (k8s pod) running this process.
    pub hostname: String,
    /// The local directory to store data.
    pub local_dir: String,
    /// The policy config for the cache eviction.
    pub policy_config: PolicyConfig,
    /// Prewarming table concurrency (how many tables to concurrently perform catalog queries upon)
    pub prewarming_table_concurrency: usize,
}

/// Build a cache server.
pub async fn build_cache_server(
    config: ParquetCacheServerConfig,
    direct_store: Arc<dyn ObjectStore>,
    catalog: Arc<dyn Catalog>,
) -> ParquetCacheServer {
    let ParquetCacheServerConfig {
        keyspace_config_path: configfile_path,
        hostname: node_hostname,
        local_dir,
        policy_config,
        ..
    } = config.clone();

    ServiceBuilder::new()
        // outermost layer 0
        .layer(BuildCacheService::new(catalog, config))
        // layer 1
        .layer(BuildKeyspaceService {
            configfile_path,
            node_hostname,
        })
        // layer 2
        .layer(BuildPreconditionService)
        // innermost layer 3
        .service(DataService::new(direct_store, policy_config, Some(local_dir)).await)
}

#[cfg(test)]
mod server_integration_tests {
    use std::path::PathBuf;
    use std::{fs::create_dir_all, io::Write, path::Path, time::Duration};

    use bytes::Buf;
    use chrono::Utc;
    use data_types::{ColumnType, ObjectStoreId, ParquetFileParams};
    use http::StatusCode;
    use hyper::Body;
    use iox_tests::TestParquetFileBuilder;
    use object_store::{local::LocalFileSystem, ObjectMeta};
    use parquet_file::ParquetFilePath;
    use serde::Deserialize;
    use serde_json::Deserializer;
    use tempfile::{tempdir, NamedTempFile, TempDir};
    use tower::Service;

    use crate::data_types::{
        GetObjectMetaResponse, InstanceState, KeyspaceResponseBody, ParquetCacheInstanceSet,
        Request, ServiceNode, State, WriteHint, WriteHintRequestBody,
    };
    use crate::server::response::Response as ServerInternalResponse;

    use super::{mock::create_mock_raw_parquet_file, *};

    fn create_fs_direct_store(local_dir: &Path) -> Arc<dyn ObjectStore> {
        create_dir_all(local_dir).unwrap();
        Arc::new(LocalFileSystem::new_with_prefix(local_dir).expect("should create fs ObjectStore"))
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let tmpdir = tempdir().unwrap();
        let direct_store = create_fs_direct_store(tmpdir.path());
        let catalog = iox_tests::TestCatalog::new();

        let config = ParquetCacheServerConfig {
            keyspace_config_path: "/tmp".to_string(),
            hostname: "localhost".to_string(),
            local_dir: tmpdir.path().to_str().unwrap().to_string(),
            policy_config: PolicyConfig::default(),
            prewarming_table_concurrency: 10,
        };

        let mut server = build_cache_server(config, direct_store, catalog.catalog()).await;

        let req = http::Request::get("http://example.com/invalid-path/")
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await;

        // assert expected http response
        assert_matches::assert_matches!(
            resp,
            Err(ServerError::BadRequest(msg)) if msg.contains("invalid path"),
            "expected bad request, instead found {:?}", resp
        );
    }

    const VALID_HOSTNAME: &str = "hostname-a";
    lazy_static::lazy_static! {
        static ref KEYSPACE_DEFINITION: ParquetCacheInstanceSet = ParquetCacheInstanceSet {
            revision: 0,
            // a single node in the keyspace, therefore all keys should hash to this keyspace
            instances: vec![VALID_HOSTNAME].into_iter().map(String::from).collect(),
        };
    }

    const LOCATION: &str = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet";

    async fn setup_service_and_direct_store(
        direct_store: Arc<dyn ObjectStore>,
        cache_tmpdir: TempDir,
        file: &mut NamedTempFile,
    ) -> (ParquetCacheServer, ObjectMeta) {
        let catalog = iox_tests::TestCatalog::new();

        let policy_config = PolicyConfig {
            max_capacity: 3_200_000_000,
            event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
        };

        writeln!(file, "{}", serde_json::json!(*KEYSPACE_DEFINITION))
            .expect("should write keyspace definition to configfile");

        let obj_store_path = object_store::path::Path::from(LOCATION);

        let config = ParquetCacheServerConfig {
            keyspace_config_path: file.path().to_str().unwrap().to_string(),
            hostname: VALID_HOSTNAME.to_string(),
            local_dir: cache_tmpdir.path().to_str().unwrap().to_string(),
            policy_config,
            prewarming_table_concurrency: 10,
        };

        let server = build_cache_server(config, Arc::clone(&direct_store), catalog.catalog()).await;

        // make properly formed object
        let parquet_path = parquet_file::ParquetFilePath::try_from(&LOCATION.to_string())
            .expect("should be valid parquet file path");
        let (bytes, _) = create_mock_raw_parquet_file(parquet_path, chrono::Utc::now()).await;

        // add object to direct store
        direct_store
            .put(&obj_store_path, bytes::Bytes::from(bytes))
            .await
            .expect("should write object to direct store");
        let expected_meta = direct_store
            .head(&obj_store_path)
            .await
            .expect("should have object in direct store");

        // wait until service is ready
        let mut this = server.clone();
        futures::future::poll_fn(move |cx| this.poll_ready(cx))
            .await
            .expect("should not have failed");

        (server, expected_meta)
    }

    async fn confirm_data_exists(expected_meta: ObjectMeta, server: &mut ParquetCacheServer) {
        // issue read metadata
        let req = Request::GetMetadata(expected_meta.location.to_string(), Default::default());
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");

        // assert expected http response for metadata
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body: GetObjectMetaResponse = serde_json::from_reader(
            hyper::body::aggregate(resp.into_body())
                .await
                .expect("should create reader")
                .reader(),
        )
        .expect("should read response body");
        let resp_meta: object_store::ObjectMeta = resp_body.into();
        assert_eq!(
            resp_meta, expected_meta,
            "expected proper metadata, instead found {:?}",
            resp_meta
        );

        // issue read object
        let req = Request::GetObject(expected_meta.location.to_string(), Default::default());
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");

        // assert expected http response for object
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let body = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("reading response body");
        assert_eq!(
            body.len(),
            expected_meta.size,
            "expected data in body, instead found {}",
            std::str::from_utf8(&body).unwrap()
        );
    }

    #[tokio::test]
    async fn test_write_hint_and_read() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, expected_meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // issue write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION.into(),
            hint: WriteHint {
                file_size_bytes: expected_meta.size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");

        // assert expected http response for write-hint
        let expected_resp = ServerInternalResponse::Written;
        assert_eq!(
            resp.status(),
            expected_resp.code(),
            "expected http response status code to match, instead found {:?}",
            resp
        );
        let body = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("reading response body");
        assert_eq!(
            body.len(),
            0,
            "expected empty body, instead found {}",
            std::str::from_utf8(&body).unwrap()
        );

        confirm_data_exists(expected_meta, &mut server).await;
    }

    #[tokio::test]
    async fn test_cache_miss_writeback_and_read() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let local_store_path = PathBuf::from(cache_tmpdir.path());
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, expected_meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // trigger cache miss
        let req = Request::GetMetadata(LOCATION.into(), Default::default());
        let resp = server.call(req.into()).await;
        assert_matches::assert_matches!(
            resp,
            Err(ServerError::CacheMiss),
            "expected cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        let writeback_local_store_path = local_store_path.join(LOCATION);
        crate::tests::wait_for_file_to_exist(writeback_local_store_path.as_path())
            .await
            .expect("should succeed on writeback");

        confirm_data_exists(expected_meta, &mut server).await;
    }

    #[tokio::test]
    async fn test_state_responses() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, _meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // wait until finished warming
        while get_state(&mut server.clone()).await != InstanceState::Running {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // check keyspace status is running, with version 0
        let req = Request::GetState;
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body_json = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("should read response body");
        let mut de = Deserializer::from_slice(&resp_body_json);
        let mut state = State::deserialize(&mut de).expect("valid State object");
        state.state_changed = 0; // ignore the timestamp
        assert_eq!(
            state,
            State {
                state: InstanceState::Running,
                state_changed: 0,
                current_node_set_revision: 0,
                next_node_set_revision: 0,
            },
        );

        // tell keyspace to cool, by changing keyspace definition
        let new_keyspace_definition = serde_json::json!(ParquetCacheInstanceSet {
            revision: 1,
            instances: vec!["another-node"].into_iter().map(String::from).collect(),
        })
        .to_string();
        let mut next_configmap = NamedTempFile::new().unwrap();
        writeln!(next_configmap, "{}", new_keyspace_definition.as_str())
            .expect("should write keyspace definition to configfile");
        next_configmap
            .persist(configfile.path())
            .expect("should overwrite with new configmap");

        // waiting for new_keyspace_definition to load
        // cannot use poll_ready, as it is already returning ready (to accept `GET /state` requests)
        tokio::time::sleep(Duration::from_secs(10)).await;

        // check keyspace status is cooling, with version 1
        let req = Request::GetState;
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body_json = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("should read response body");
        let mut de = Deserializer::from_slice(&resp_body_json);
        let mut state = State::deserialize(&mut de).expect("valid State object");
        state.state_changed = 0; // ignore the timestamp
        assert_eq!(
            state,
            State {
                state: InstanceState::Cooling,
                state_changed: 0,
                current_node_set_revision: 1,
                next_node_set_revision: 1,
            },
        );
    }

    #[tokio::test]
    async fn test_keyspace_nodes() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, _meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // get keyspace nodes
        let req = Request::GetKeyspace;
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body: KeyspaceResponseBody = serde_json::from_reader(
            hyper::body::aggregate(resp.into_body())
                .await
                .expect("should create reader")
                .reader(),
        )
        .expect("should read response body");
        assert_matches::assert_matches!(
            resp_body,
            KeyspaceResponseBody { nodes } if matches!(
                &nodes[..],
                [ServiceNode { id: 0, hostname }] if hostname == VALID_HOSTNAME
            )
        );
    }

    async fn get_state(server: &mut ParquetCacheServer) -> InstanceState {
        let req = Request::GetState;
        let resp = server
            .call(req.into())
            .await
            .expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body_json = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("should read response body");
        let mut de = Deserializer::from_slice(&resp_body_json);
        State::deserialize(&mut de)
            .expect("valid State object")
            .state
    }

    #[tokio::test]
    async fn test_prewarming() {
        // setup
        let local_tmpdir = tempdir().unwrap();
        let catalog = iox_tests::TestCatalog::new();
        let direct_store = catalog.object_store();
        let mut configfile = NamedTempFile::new().unwrap();
        writeln!(configfile, "{}", serde_json::json!(*KEYSPACE_DEFINITION))
            .expect("should write keyspace definition to configfile");

        // create properly encoded parquet files, adding to catalog and remote/direct store
        let mut obj_metas = Vec::new();
        let mut summed_min_capacity = 0_u64;
        for i in 0..100 {
            let now = Utc::now();

            // create a new namespace, table, and partition per object
            let namespace = catalog
                .create_namespace_1hr_retention(format!("ns{}", i).as_str())
                .await;
            let table = namespace.create_table(format!("table{}", i).as_str()).await;
            table.create_column("time", ColumnType::Time).await;
            table.create_column("field", ColumnType::String).await;
            table.create_column("tag", ColumnType::Tag).await;
            let partition = table
                .create_partition(format!("{}", now.format("%Y-%m-%d")).as_str())
                .await;

            // obj store path
            let obj_store_id = ObjectStoreId::new();
            let parquet_file_path = ParquetFilePath::new(
                namespace.namespace.id,
                table.table.id,
                &partition.partition.transition_partition_id(),
                obj_store_id,
            );

            // create parquet file
            let parquet_file = TestParquetFileBuilder::default()
                .with_line_protocol(
                    format!(
                        r#"{},tag=tag field="bananas" {}"#,
                        table.table.name,
                        now.timestamp_nanos_opt().unwrap()
                    )
                    .as_str(),
                )
                .with_creation_time(iox_time::Time::from_date_time(now))
                .with_object_store_id(obj_store_id);
            let inserted_into_catalog = partition.create_parquet_file(parquet_file).await;
            let inserted_into_catalog =
                ParquetFilePath::from(&ParquetFileParams::from(inserted_into_catalog.parquet_file));
            assert_eq!(
                parquet_file_path,
                inserted_into_catalog,
                "should have inserted path {} into catalog, instead found {}",
                parquet_file_path.object_store_path(),
                inserted_into_catalog.object_store_path()
            );

            // add to remote store
            let resp = direct_store
                .get(&parquet_file_path.object_store_path())
                .await;
            assert!(resp.is_ok(), "should be readable from remote store");
            let meta = resp.unwrap().meta;
            summed_min_capacity = summed_min_capacity.checked_add(meta.size as u64).unwrap();
            obj_metas.push(meta);
        }

        // make server
        let config = ParquetCacheServerConfig {
            keyspace_config_path: configfile.path().to_str().unwrap().to_string(),
            hostname: VALID_HOSTNAME.to_string(),
            local_dir: local_tmpdir.path().to_str().unwrap().to_string(),
            policy_config: PolicyConfig {
                max_capacity: summed_min_capacity + 1000,
                event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
            },
            prewarming_table_concurrency: 10,
        };
        let server = build_cache_server(config, direct_store, catalog.catalog()).await;

        // poll_ready -- pending
        assert_eq!(
            get_state(&mut server.clone()).await,
            InstanceState::Pending,
            "should be pending"
        );

        // wait until finished warming (may already be done!)
        while get_state(&mut server.clone()).await != InstanceState::Running {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // confirm all objects are present
        for obj_meta in obj_metas {
            confirm_data_exists(obj_meta, &mut server.clone()).await;
        }
    }
}
