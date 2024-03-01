use std::sync::Arc;

use object_store::{GetResult, GetResultPayload, ObjectMeta, ObjectStore};
use observability_deps::tracing::warn;

use super::store::LocalStore;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Stream error: {0}")]
    Stream(String),
    #[error("File error: {0}")]
    File(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Bad Request: object location does not exist in direct object store")]
    DoesNotExist,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Handles the WRITE requests (`/write-hint`)
#[derive(Debug, Clone)]
pub struct WriteHandler {
    cache: Arc<LocalStore>,
    direct_store: Arc<dyn ObjectStore>,
}

impl WriteHandler {
    pub fn new(cache: Arc<LocalStore>, direct_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cache,
            direct_store,
        }
    }

    pub async fn write_local(
        &self,
        location: &str,
        file_size_bytes: Option<i64>,
    ) -> Result<ObjectMeta> {
        // get from remote
        let GetResult { meta, payload, .. } = self
            .direct_store
            .get(&location.into())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => Error::DoesNotExist,
                _ => Error::Stream(e.to_string()),
            })?;

        if let Some(file_size_bytes) = file_size_bytes {
            if !(meta.size as i64).eq(&file_size_bytes) {
                warn!(
                    "failed to perform writeback due to file size mismatch: {} != {}",
                    meta.size, file_size_bytes
                );
                return Err(Error::BadRequest(
                    "failed to perform writeback due to file size mismatch".to_string(),
                ));
            }
        };

        // write local
        match payload {
            GetResultPayload::File(_, pathbuf) => self
                .cache
                .move_file_to_cache(pathbuf, &location.into())
                .await
                .map_err(|e| Error::File(e.to_string()))?,
            GetResultPayload::Stream(stream) => self
                .cache
                .write_object(&location.into(), meta.size as i64, stream)
                .await
                .map_err(|e| Error::Stream(e.to_string()))?,
        };

        Ok(meta)
    }
}
