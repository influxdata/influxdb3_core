//! DataService layer
//!
//! Handles the READ & WRITE of data including cache management,
//! write-backs, and read/write to the local store.
//!
//!
//! Write-backs:
//!
//! ┌──────────────────┐                 ┌────────────────┐
//! │  Cache Miss      │                 │  Write-Hint    │
//! └───────┬──────────┘                 └───────┬────────┘
//!         │                                    │
//!         │                          ┌─────────▼───────────┐ exists
//!         │                          │ check cache manager ├─────────► OK
//!         │                          └─────────┬───────────┘
//!         └────────────────┬───────────────────┘
//!                          │
//!                ┌─────────▼───────┐
//!                │ valid path/key? │
//!                └─────────┬───────┘
//!                          │
//!                ┌─────────▼──────────────┐
//!                │ read from remote store │
//!                │  write to local store  │
//!                └──────────┬─────────────┘
//!                           │
//!            ┌──────────────▼──────────────────┐
//!            │ has metadata for cache manager? │
//!            │    (cache miss will not)        ├──────────────┐
//!            └──────────────┬──────────────────┘              │
//!                           │                       ┌─────────▼────────────────┐
//!                           │                       │ Read from parquet footer │
//!                           │                       └─────────┬────────────────┘
//!                 ┌─────────▼────────────┐                    │
//!                 │ Insert cache manager |◄───────────────────┴
//!                 └─────────┬────────────┘
//!                           │
//!                           ▼
//!                          OK
//!
//!
//! The above process works as of this writing, since the cache management
//! only makes use of the metadata within the parquet footer. We could hypothetically
//! remove all of the metadata in the write-hint and only send the object store path/key,
//! if we intend to always include any metadata needed for cache management within
//! the parquet file footer. Although that was not the intent during requirements gathering,
//! it could make sense in order to avoid any catalog requests on cache miss.
//!

mod manager;
mod reads;
mod store;
mod writes;
pub(crate) use writes::Error as WriteError;

use std::{sync::Arc, task::Poll};

use object_store::ObjectStore;
use observability_deps::tracing::error;
use parquet_file::{
    metadata::{derive_min_max_time, TimestampRange},
    ParquetFilePath,
};
use tokio::task::JoinHandle;
use tower::Service;

use self::{
    manager::{CacheManager, CacheManagerValue},
    reads::ReadHandler,
    store::LocalStore,
    writes::WriteHandler,
};
use super::{error::Error, response::Response};
use crate::data_types::{PolicyConfig, Request, WriteHint, WriteHintRequestBody};

/// DataError is a local error type.
/// Tower service layers are expected to convert to [`Error`].
#[derive(Debug, thiserror::Error)]
pub enum DataError {
    #[error("Read error: {0}")]
    Read(#[from] super::data::reads::Error),
    #[error("Write error: {0}")]
    Write(#[from] super::data::writes::Error),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Bad Request: object location does not exist in direct object store")]
    DoesNotExist,
}

/// Service that provides access to the data.
#[derive(Debug, Clone)]
pub struct DataService {
    cache_manager: Arc<CacheManager>,
    read_handler: ReadHandler,
    write_hander: WriteHandler,
    handle: Arc<JoinHandle<()>>,
}

impl DataService {
    pub async fn new(
        direct_store: Arc<dyn ObjectStore>,
        config: PolicyConfig,
        dir: Option<impl ToString + Send>,
    ) -> Self {
        let data_accessor = Arc::new(LocalStore::new(dir));

        // TODO: use a bounded channel
        // Apply back pressure if we can't keep up (a.k.a. the actual eviction from the local store).
        let (evict_tx, evict_rx) = async_channel::unbounded();

        // start background task to evict from local store
        let data_accessor_ = Arc::clone(&data_accessor);
        let handle = tokio::spawn(async move {
            while let Ok(key) = evict_rx.recv().await {
                let _ = data_accessor_.delete_object(&key).await;
            }
        });

        Self {
            read_handler: ReadHandler::new(Arc::clone(&data_accessor)),
            write_hander: WriteHandler::new(Arc::clone(&data_accessor), direct_store),
            cache_manager: Arc::new(CacheManager::new(config, evict_tx)),
            handle: Arc::new(handle),
        }
    }

    async fn create_write_hint(
        &self,
        location: &String,
        location_params: ParquetFilePath,
        file_size_bytes: i64,
    ) -> Result<WriteHint, DataError> {
        let decoded_meta = self.read_handler.read_decoded_metadata(location).await?;

        // derive min/max time
        let schema = decoded_meta
            .read_schema()
            .expect("failed to read encoded schema");
        let stats = decoded_meta
            .read_statistics(&schema)
            .expect("invalid statistics");
        let TimestampRange {
            min: min_time,
            max: max_time,
        } = derive_min_max_time(stats);

        Ok(WriteHint {
            namespace_id: location_params.namespace_id().get(),
            table_id: location_params.table_id().get(),
            min_time: min_time.get(),
            max_time: max_time.get(),
            file_size_bytes,
        })
    }

    async fn write_back(
        &self,
        location: String,
        write_hint: Option<WriteHint>,
    ) -> Result<(), DataError> {
        // confirm valid location
        let location_params = parquet_file::ParquetFilePath::try_from(&location)
            .map_err(|e| DataError::BadRequest(e.to_string()))?;

        // write to local store
        let file_size = write_hint.map(|hint| hint.file_size_bytes);
        let metadata = self.write_hander.write_local(&location, file_size).await?;

        // create write-hint, if necessary
        let params = match write_hint {
            Some(hint) => hint,
            None => {
                self.create_write_hint(&location, location_params, metadata.size as i64)
                    .await?
            }
        };

        // update cache manager
        self.cache_manager
            .insert(location, CacheManagerValue { params, metadata })
            .await;

        Ok(())
    }
}

impl Service<Request> for DataService {
    type Response = Response;
    type Error = Error;
    type Future = super::response::PinnedFuture;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        match req {
            Request::Warmed | Request::GetState | Request::GetKeyspace => {
                unreachable!("`this request should have already been handled in the KeyspaceLayer`")
            }
            Request::GetMetadata(ref obj_location, _) | Request::GetObject(ref obj_location, _) => {
                let this = self.clone();
                let obj_location = obj_location.clone();
                Box::pin(async move {
                    match this.cache_manager.in_cache(&obj_location).await {
                        Ok(_) => match req {
                            Request::GetMetadata(_l, _h) => {
                                let meta = this.cache_manager.fetch_metadata(&obj_location).await?;
                                Ok(Response::Head(meta.into()))
                            }
                            Request::GetObject(_l, _h) => {
                                let stream = this
                                    .read_handler
                                    .read_local(&obj_location)
                                    .await
                                    .map_err(DataError::Read)?;
                                Ok(Response::Data(stream))
                            }
                            _ => unreachable!(),
                        },
                        Err(Error::CacheMiss) => {
                            // trigger write-back on another thread
                            let this_ = this.clone();
                            tokio::spawn(async move {
                                if let Err(error) = this_.write_back(obj_location, None).await {
                                    error!(%error, "write-back failed to perform local-store write");
                                }
                            });

                            // still return immediate response, such that client will use direct_store fallback
                            Err(Error::CacheMiss)
                        }
                        Err(e) => Err(e),
                    }
                })
            }
            Request::WriteHint(WriteHintRequestBody { location, hint, .. }) => {
                let this = self.clone();
                Box::pin(async move {
                    match this.cache_manager.in_cache(&location).await {
                        Ok(_) => Ok(Response::Written),
                        Err(_) => {
                            this.write_back(location, Some(hint)).await?;
                            Ok(Response::Written)
                        }
                    }
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::File, io::Write, ops::Range, path::PathBuf};

    use assert_matches::assert_matches;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use futures::{stream::BoxStream, TryStreamExt};
    use object_store::{
        path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta,
        ObjectStore, PutOptions, PutResult,
    };
    use tempfile::{tempdir, TempDir};
    use tokio::{fs::create_dir_all, io::AsyncWrite};

    use crate::{data_types::GetObjectMetaResponse, server::mock::create_mock_raw_parquet_file};

    use super::*;

    const ONE_SECOND: u64 = 1_000_000_000;

    // refer to valid path in parquet_file::ParquetFilePath
    const LOCATION_F: &str = "0/0/partition_id/00000000-0000-0000-0000-000000000000.parquet";
    const LOCATION_S: &str = "0/0/partition_id/00000000-0000-0000-0000-000000000001.parquet";
    const LOCATION_MISSING: &str = "0/0/partition_id/00000000-0000-0000-0000-000000000002.parquet"; // not in direct store

    lazy_static::lazy_static! {
        static ref LAST_MODIFIED: DateTime<Utc> = Utc::now();
    }

    #[derive(Debug)]
    struct MockData(Bytes, bool /* as_stream */);

    #[derive(Debug)]
    struct MockDirectStore {
        mocked: HashMap<String /* location */, MockData>,
        temp_dir: TempDir,
        parquet_file_size: usize,
    }

    impl MockDirectStore {
        fn new(parquet_file_size: usize) -> Self {
            Self {
                mocked: HashMap::new(),
                temp_dir: tempdir().expect("should create temp dir"),
                parquet_file_size,
            }
        }

        fn put_mock(&mut self, location: String, data: MockData) {
            self.mocked.insert(location, data);
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MockDirectStore {
        async fn get_opts(
            &self,
            location: &Path,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let MockData(bytes, as_stream) = match self.mocked.get(&location.to_string()) {
                Some(data) => data,
                _ => {
                    return Err(object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "not found in remote store".into(),
                    })
                }
            };

            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: *LAST_MODIFIED,
                size: self.parquet_file_size,
                e_tag: Default::default(),
                version: Default::default(),
            };

            let bytes = bytes.to_owned();
            let payload =
                match as_stream {
                    true => GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                        Ok(bytes)
                    }))),
                    false => {
                        let path = self.temp_dir.path().join(location.to_string());
                        create_dir_all(path.parent().unwrap())
                            .await
                            .expect("should create nested path");
                        let mut file =
                            File::create(path.as_path()).expect("should be able to open temp file");
                        file.write_all(&bytes)
                            .expect("should be able to write to temp file");
                        file.flush().expect("should be able to flush temp file");
                        GetResultPayload::File(file, path)
                    }
                };

            Ok(GetResult {
                payload,
                meta,
                range: Range {
                    start: 0,
                    end: self.parquet_file_size,
                },
            })
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _bytes: Bytes,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }
        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
            unimplemented!()
        }
        async fn abort_multipart(
            &self,
            _location: &Path,
            _multipart_id: &MultipartId,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }
        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    impl std::fmt::Display for MockDirectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockDirectStore")
        }
    }

    async fn make_parquet_file(location: impl ToString + Send) -> Bytes {
        let (bytes, _meta) = create_mock_raw_parquet_file(
            ParquetFilePath::try_from(&location.to_string()).expect("valid parquet path"),
            *LAST_MODIFIED,
        )
        .await;
        Bytes::from(bytes)
    }

    async fn make_service(
        temp_dir: PathBuf,
        policy_config: Option<PolicyConfig>,
    ) -> (DataService, usize) {
        // parquet file
        let bytes = make_parquet_file(LOCATION_F).await;
        let parquet_file_size = bytes.len();

        // services consumed by DataService
        let mut direct_store = MockDirectStore::new(parquet_file_size);

        // data returned as file
        direct_store.put_mock(LOCATION_F.to_string(), MockData(bytes.clone(), false));

        // data returned as stream
        direct_store.put_mock(LOCATION_S.to_string(), MockData(bytes, true));

        (
            DataService::new(
                Arc::new(direct_store),
                policy_config.unwrap_or(PolicyConfig {
                    max_capacity: 3_200_000,
                    event_recency_max_duration_nanoseconds: ONE_SECOND * 60 * 2,
                }),
                Some(temp_dir.to_str().unwrap()),
            )
            .await,
            parquet_file_size,
        )
    }

    // note: uses file for write-back
    #[tokio::test]
    #[ignore]
    async fn test_metadata_writeback_on_cache_miss() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) = make_service(PathBuf::from(dir.path()), None).await;

        // return cache miss
        let req = Request::GetMetadata(LOCATION_F.into(), Default::default());
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        let writeback_local_store_path = PathBuf::from(dir.path()).join(LOCATION_F);
        crate::tests::wait_for_file_to_exist(writeback_local_store_path.as_path())
            .await
            .expect("should succeed on writeback");

        // return cache hit
        let req = Request::GetMetadata(LOCATION_F.into(), Default::default());
        let resp = service.call(req).await;
        let expected = GetObjectMetaResponse::from(ObjectMeta {
            location: LOCATION_F.into(),
            size: parquet_file_size,
            last_modified: *LAST_MODIFIED,
            e_tag: Default::default(),
            version: Default::default(),
        });
        assert_matches!(
            resp,
            Ok(Response::Head(meta)) if meta == expected,
            "should return metadata for location, instead found {:?}", resp
        );
    }

    // note: uses file for write-back
    #[tokio::test]
    async fn test_object_writeback_on_cache_miss() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) = make_service(PathBuf::from(dir.path()), None).await;

        // return cache miss
        let req = Request::GetObject(LOCATION_F.into(), Default::default());
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        let writeback_local_store_path = PathBuf::from(dir.path()).join(LOCATION_F);
        crate::tests::wait_for_file_to_exist(writeback_local_store_path.as_path())
            .await
            .expect("should succeed on writeback");

        // return cache hit
        let req = Request::GetObject(LOCATION_F.into(), Default::default());
        let resp = service.call(req).await;
        match resp {
            Ok(Response::Data(stream)) => {
                let data = stream.try_collect::<Vec<Bytes>>().await.unwrap();
                assert_eq!(data.len(), 1, "should have returned 1 streamed chunk");
                assert_eq!(
                    data[0].len(),
                    parquet_file_size,
                    "should have returned matching bytes"
                );
            }
            _ => panic!("should return data for location, instead found {:?}", resp),
        }
    }

    // note: uses stream for write-back
    #[tokio::test]
    async fn test_write_hint() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION_S.into(),
            hint: WriteHint {
                file_size_bytes: parquet_file_size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );

        // return cache hit -- metadata
        let req = Request::GetMetadata(LOCATION_S.into(), Default::default());
        let resp = service.call(req).await;
        let expected = GetObjectMetaResponse::from(ObjectMeta {
            location: LOCATION_S.into(),
            size: parquet_file_size,
            last_modified: *LAST_MODIFIED,
            e_tag: Default::default(),
            version: Default::default(),
        });
        assert_matches!(
            resp,
            Ok(Response::Head(meta)) if meta == expected,
            "should return metadata for location, instead found {:?}", resp
        );

        // return cache hit -- object
        let req = Request::GetObject(LOCATION_S.into(), Default::default());
        let resp = service.call(req).await;
        match resp {
            Ok(Response::Data(stream)) => {
                let data = stream.try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(data.len(), 1, "should have returned 1 streamed chunk");
                assert_eq!(
                    data[0].len(),
                    parquet_file_size,
                    "should have returned matching bytes"
                );
            }
            _ => panic!("should return data for location, instead found {:?}", resp),
        }
    }

    #[tokio::test]
    async fn test_write_hint_fails_for_invalid_path() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: "not_a_valid_path.parquet".into(),
            hint: WriteHint {
                file_size_bytes: parquet_file_size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::Data(DataError::BadRequest(_))),
            "should return failed write-back, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_write_hint_fails_for_incorrect_size() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, _) = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION_S.into(),
            hint: WriteHint {
                file_size_bytes: 12312,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::Data(_)),
            "should error for incorrect file size in write-hint, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_fails_for_nonexistent_object() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        // Fails when looking up in remote store.
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION_MISSING.into(),
            hint: WriteHint {
                file_size_bytes: parquet_file_size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::Data(DataError::Write(writes::Error::DoesNotExist))),
            "should return failed write-back, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_eviction() {
        // get estimated size of parquet file, in order to set PolicyConfig capacity
        let estimated_bytes = make_parquet_file(LOCATION_F).await;

        // setup
        let policy_config = PolicyConfig {
            max_capacity: estimated_bytes.len() as u64 + 20,
            event_recency_max_duration_nanoseconds: ONE_SECOND * 60 * 2,
        };
        let dir = tempdir().expect("should create temp dir");
        let (mut service, parquet_file_size) =
            make_service(PathBuf::from(dir.path()), Some(policy_config)).await;

        // issue write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION_S.into(),
            hint: WriteHint {
                file_size_bytes: parquet_file_size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );

        // return cache hit
        let req = Request::GetMetadata(LOCATION_S.into(), Default::default());
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Head(_)),
            "should return metadata for location, instead found {:?}",
            resp
        );
        service.cache_manager.flush_pending().await;

        // issue 2nd write-hint
        let req = Request::WriteHint(WriteHintRequestBody {
            location: LOCATION_F.into(),
            hint: WriteHint {
                file_size_bytes: parquet_file_size as i64,
                ..Default::default()
            },
            ack_setting: crate::data_types::WriteHintAck::Completed,
        });
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );
        service.cache_manager.flush_pending().await;

        // eviction should have happened
        // should return cache miss
        let req = Request::GetMetadata(LOCATION_S.into(), Default::default());
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // other object should still be in cache
        let req = Request::GetMetadata(LOCATION_F.into(), Default::default());
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Head(_)),
            "should return metadata for location, instead found {:?}",
            resp
        );

        dir.close().expect("should close temp dir");
    }
}
