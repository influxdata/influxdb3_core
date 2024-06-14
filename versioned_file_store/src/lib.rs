// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{io::Read, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::ready, stream::BoxStream, TryStreamExt};
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartId, ObjectMeta, ObjectStore, PutOptions, PutResult,
};
use tokio::io::AsyncWrite;

/// Provides a versioning alternative of [`LocalFileSystem`].
/// The versioning is intended to be analogous to S3.
///
/// Purpose is to provide a testable versioned store in place of the S3 implementation.
/// The [`ObjectStore`] interface itself does not yet address versioning as this paradigm
/// has not been uniformly defined across implementations.
///
/// Caveat: VersionedFileSystemStore is only intended for use with our testing framework.
#[derive(Debug)]
pub struct VersionedFileSystemStore {
    inner: Arc<LocalFileSystem>,
    store_for_versions: Arc<LocalFileSystem>,
}

/// Our end_to_end testing framework, based on either File or VersionedFile stores, passes around
/// the same directory path and recreates the store instance separately for each service.
/// Since each separately created store instances uses the same directory,
/// it has access to the same objects.
///
/// For our versioned store used for testing, we reused the same directory but with a subdirectory of `versioned`.
static VERSION_SUBDIR: &str = "versioned";

impl VersionedFileSystemStore {
    pub fn try_new(
        inner: Arc<LocalFileSystem>,
        mut local_store_dir: PathBuf,
    ) -> object_store::Result<Self> {
        // Use the `versioned` subdirectory for non-current versions.
        local_store_dir.push(VERSION_SUBDIR);
        let path = local_store_dir.as_path();
        std::fs::create_dir_all(path).map_err(|e| object_store::Error::Generic {
            store: "versioned-file",
            source: Box::new(e),
        })?;

        let store_for_versions = Arc::new(LocalFileSystem::new_with_prefix(local_store_dir)?);

        Ok(Self {
            inner,
            store_for_versions,
        })
    }
}

#[async_trait]
impl ObjectStore for VersionedFileSystemStore {
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        // get existing
        let GetResult {
            payload,
            meta: ObjectMeta { size, .. },
            range: _,
        } = self.inner.get(location).await?;
        let mut buf = Vec::with_capacity(size);
        let size_versioned = match payload {
            GetResultPayload::File(mut file, _) => {
                file.read_to_end(&mut buf).expect("should not fail")
            }
            GetResultPayload::Stream(stream) => {
                let bytes = stream.try_collect::<Vec<_>>().await?;
                buf = bytes[..].concat();
                bytes.len()
            }
        };
        assert_eq!(size_versioned, size);

        // place into versioned
        let version_id = uuid::Uuid::new_v4().to_string();
        self.store_for_versions
            .put(&add_version_to_path(version_id, location), buf.into())
            .await?;

        // delete from existing
        self.inner.delete(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if let Some(ver) = &options.version {
            let ver = ver.to_owned();

            self.store_for_versions
                .get_opts(&add_version_to_path(ver.clone(), location), options)
                .await
                .map(|get_result| GetResult {
                    meta: ObjectMeta {
                        location: location.clone(),
                        version: Some(ver),
                        ..get_result.meta
                    },
                    ..get_result
                })
        } else {
            self.inner.get_opts(location, options).await
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let versioned_stream: BoxStream<'_, object_store::Result<ObjectMeta>> =
            Box::pin(self.store_for_versions.list(prefix).map_ok(move |meta| {
                let (version, location) = extract_version_and_path(meta.location);
                ObjectMeta {
                    version: Some(version),
                    location,
                    ..meta
                }
            }));

        let current_stream: BoxStream<'_, object_store::Result<ObjectMeta>> = Box::pin(
            self.inner
                .list(prefix)
                // since the versioned store is nested within the same directory as the current,
                // we need to remove the prefix.
                .try_filter(|meta| ready(!meta.location.to_string().starts_with("versioned/"))),
        );

        Box::pin(futures::stream::select_all(vec![
            versioned_stream,
            current_stream,
        ]))
    }

    // We do not filter out "versioned/" prefix, since [`ObjectStore::list_with_delimiter`] requires an exact path.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let ListResult {
            common_prefixes,
            objects: mut merge_objects,
        } = self.inner.list_with_delimiter(prefix).await?;

        let list_result: Vec<ObjectMeta> =
            self.store_for_versions.list(prefix).try_collect().await?;
        for meta in list_result {
            let (version, location) = extract_version_and_path(meta.location);
            let exact_prefix = Path::from_iter(
                location
                    .parts()
                    .take_while(|p| p.as_ref() != location.filename().unwrap()),
            );

            // the contract of list_with_delimiter() is for exact prefix matches (not recursive)
            if let Some(prefix) = prefix {
                if exact_prefix.eq(prefix) {
                    merge_objects.push(ObjectMeta {
                        version: Some(version),
                        location,
                        ..meta
                    });
                }
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects: merge_objects,
        })
    }

    // Everything below here is pass-thru:

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        // head request doesn't provide a version => treat as pass thru
        self.inner.head(location).await
    }
    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }
    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl std::fmt::Display for VersionedFileSystemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionedFileSystemStore")
    }
}

/// Add version_id as part of store path.
///
/// Since the separate instances of [`VersionedFileSystemStore`] do not share state,
/// and only share access to the same file directory, any shared information (e.g. version_id)
/// must be represented in some way within the store directory.
///
/// We elected to use an amended store path.
fn add_version_to_path(version: String, path: &Path) -> Path {
    let mut path_parts = path.parts().collect::<Vec<_>>();
    let filename = path_parts.pop().expect("should have filename");
    path_parts.push(version.into());
    path_parts.push(filename);
    Path::from_iter(path_parts)
}

fn extract_version_and_path(path: Path) -> (String /* version */, Path) {
    let mut path_parts = path.parts().collect::<Vec<_>>();
    let filename = path_parts.pop().expect("should have filename");
    let version = path_parts.pop().expect("should have version");
    path_parts.push(filename);

    (version.as_ref().into(), Path::from_iter(path_parts))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_versioned_file_system_store() {
        let dir = TempDir::new().unwrap();
        let original_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let store = VersionedFileSystemStore::try_new(original_store, dir.into_path())
            .expect("should make versioned store");

        let path = Path::from("path/my_obj.parquet");
        let data = Bytes::from("all these important words");

        // can put
        let put = store.put(&path, data.clone()).await.unwrap();
        assert!(
            put.version.is_none(),
            "current objects should have no version"
        );

        // can get & head
        let get = store.get(&path).await.expect("should have get result");
        assert_eq!(get.meta.size, data.len());
        let head = store.head(&path).await.expect("should have head result");
        assert_eq!(head.size, data.len());

        // after delete, cannot find
        store
            .delete(&path)
            .await
            .expect("should not error on delete");
        let get = store.get(&path).await;
        assert_matches::assert_matches!(
            get,
            Err(object_store::Error::NotFound { .. }),
            "deleted object should not be found"
        );
        let head = store.head(&path).await;
        assert_matches::assert_matches!(
            head,
            Err(object_store::Error::NotFound { .. }),
            "deleted object should not be found"
        );

        // list all objects + No prefix
        let all_obj = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list should not error");
        assert_eq!(all_obj.len(), 1, "should find 1 obj");
        let ObjectMeta {
            version, location, ..
        } = all_obj.first().unwrap();
        assert!(version.is_some(), "object should now be versioned");
        assert_eq!(location, &path, "location should not change");

        // list all objects + Some(prefix)
        let all_obj = store
            .list(Some(&object_store::path::Path::parse("path").unwrap()))
            .try_collect::<Vec<_>>()
            .await
            .expect("list should not error");
        assert_eq!(all_obj.len(), 1, "should find 1 obj");
        let ObjectMeta {
            version, location, ..
        } = all_obj.first().unwrap();
        assert!(version.is_some(), "object should now be versioned");
        assert_eq!(location, &path, "location should not change");

        // list all objects + Some(wrong-prefix)
        let all_obj = store
            .list(Some(&object_store::path::Path::parse("wrong").unwrap()))
            .try_collect::<Vec<_>>()
            .await
            .expect("list should not error");
        assert_eq!(all_obj.len(), 0, "should find no objects at wrong path");

        // list all version with delimiter + Some(prefix)
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("path").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(all_obj.objects.len(), 1, "should find 1 obj");
        let ObjectMeta {
            version, location, ..
        } = all_obj.objects.first().unwrap();
        assert!(version.is_some(), "object should now be versioned");
        assert_eq!(location, &path, "location should not change");

        // list all version with delimiter + Some(wrong-prefix)
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("wrong").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(
            all_obj.objects.len(),
            0,
            "should find no objects at wrong path"
        );

        // can get with version
        let get = store
            .get_opts(
                &path,
                GetOptions {
                    version: version.clone(),
                    ..Default::default()
                },
            )
            .await;
        assert_matches::assert_matches!(
            get,
            Ok(GetResult { meta, .. }) if meta.size == data.len(),
            "deleted object should be available via versioning"
        );

        // cannot get with wrong version
        let get = store
            .get_opts(
                &path,
                GetOptions {
                    version: Some("foo".into()),
                    ..Default::default()
                },
            )
            .await;
        assert_matches::assert_matches!(
            get,
            Err(object_store::Error::NotFound { .. }),
            "deleted object should not be found with the wrong version"
        );
    }

    #[tokio::test]
    async fn list_with_delimiter_is_not_recursive() {
        let dir = TempDir::new().unwrap();
        let original_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let store = VersionedFileSystemStore::try_new(original_store, dir.into_path())
            .expect("should make versioned store");

        let path = Path::from("path/to/my_obj.parquet");
        let data = Bytes::from("all these important words");

        // put
        let put = store.put(&path, data.clone()).await.unwrap();
        assert!(
            put.version.is_none(),
            "current objects should have no version"
        );

        // No prefix
        // list_with_delimiter is not recursive, requires an exact path
        let all_obj = store
            .list_with_delimiter(None)
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(
            all_obj.objects.len(),
            0,
            "should find 0 objects at the exact path"
        );

        // prefix to ancestor
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("path").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(
            all_obj.objects.len(),
            0,
            "should find 0 objects at the exact path"
        );

        // WORKZ
        // prefix to exact parent
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("path/to").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(
            all_obj.objects.len(),
            1,
            "should find 1 object at the exact pathj"
        );
        let ObjectMeta { location, .. } = all_obj.objects.first().unwrap();
        assert_eq!(location, &path, "location should not change");
    }

    #[tokio::test]
    async fn test_multiple_versions() {
        let dir = TempDir::new().unwrap();
        let original_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let store = VersionedFileSystemStore::try_new(original_store, dir.into_path())
            .expect("should make versioned store");

        let path = Path::from("path/to/my_obj.parquet");
        let data = Bytes::from("all these important words");

        // put #1
        let put = store.put(&path, data.clone()).await.unwrap();
        assert!(
            put.version.is_none(),
            "current objects should have no version"
        );

        // delete
        store
            .delete(&path)
            .await
            .expect("should not error on delete");

        // put #2
        let put = store.put(&path, data.clone()).await.unwrap();
        assert!(
            put.version.is_none(),
            "current objects should have no version"
        );

        // delete
        store
            .delete(&path)
            .await
            .expect("should not error on delete");

        // put #3
        let put = store.put(&path, data.clone()).await.unwrap();
        assert!(
            put.version.is_none(),
            "current objects should have no version"
        );

        // list all objects  => should find all 3
        let all_obj = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list should not error");
        assert_eq!(all_obj.len(), 3, "should find 3 obj");
        let versioned_objs = all_obj
            .into_iter()
            .filter(|meta| meta.version.is_some())
            .collect::<Vec<_>>();
        assert_eq!(versioned_objs.len(), 2, "should find 2 versioned obj");

        // list all version with delimiter + Some(prefix) => should find all 3
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("path/to").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(all_obj.objects.len(), 3, "should find 3 objs");
        let versioned_objs = all_obj
            .objects
            .into_iter()
            .filter(|meta| meta.version.is_some())
            .collect::<Vec<_>>();
        assert_eq!(versioned_objs.len(), 2, "should find 2 versioned obj");

        // list all version with delimiter + Some(incomplete-prefix) => should find none, since not recursive
        let all_obj = store
            .list_with_delimiter(Some(&object_store::path::Path::parse("path").unwrap()))
            .await
            .expect("list_with_delimiter should not error");
        assert_eq!(all_obj.objects.len(), 0, "should find 0 objs");
    }
}
