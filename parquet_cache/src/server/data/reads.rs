use std::sync::Arc;

use parquet_file::metadata::DecodedIoxParquetMetaData;

use super::store::{Error as StoreError, LocalStore, StreamedObject};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Store error: {0}")]
    Store(#[from] StoreError),
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Service that handles the READ requests (`GET /object`).
#[derive(Debug, Clone)]
pub struct ReadHandler {
    cache: Arc<LocalStore>,
}

impl ReadHandler {
    pub fn new(cache: Arc<LocalStore>) -> Self {
        Self { cache }
    }

    pub async fn read_local(&self, location: &String) -> Result<StreamedObject> {
        self.cache.read_object(location).await.map_err(Error::Store)
    }

    pub async fn read_decoded_metadata(
        &self,
        location: &String,
    ) -> Result<DecodedIoxParquetMetaData> {
        self.cache
            .read_parquet_metadata(location)
            .await
            .map(DecodedIoxParquetMetaData::from)
            .map_err(Error::Store)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use bytes::Bytes;
    use chrono::Utc;
    use futures::StreamExt;
    use parquet_file::ParquetFilePath;

    use crate::server::mock::create_mock_raw_parquet_file;

    use super::*;

    // avoided PartialEq trait impl during test compilation
    fn format_meta_partial_equals_file_meta(
        file_meta: &parquet::file::metadata::FileMetaData,
        format_meta: &parquet::format::FileMetaData,
    ) -> bool {
        assert_eq!(
            format_meta.version,
            file_meta.version(),
            "expect versions to match"
        );
        assert_eq!(
            format_meta.num_rows,
            file_meta.num_rows(),
            "expect num_rows to match"
        );
        assert_eq!(
            format_meta.created_by,
            file_meta.created_by().map(|fm| fm.to_string()),
            "expected created_by to match"
        );
        // Cannot compare column_orders and kv_meta_values, since are encoded differently.

        true
    }

    #[tokio::test]
    async fn test_decoded_metadata() {
        // setup
        let local_store_dir = tempfile::tempdir().unwrap();
        let local_store = Arc::new(LocalStore::new(Some(
            local_store_dir.path().to_string_lossy(),
        )));
        let read_handler = ReadHandler::new(Arc::clone(&local_store));

        // parquet file data
        let location = "0/0/partition_id/00000000-0000-0000-0000-000020000000.parquet".to_string();
        let created_at = Utc::now();
        let (bytes, parquet_format_filemeta) = create_mock_raw_parquet_file(
            ParquetFilePath::try_from(&location).expect("should parse parquet path"),
            created_at,
        )
        .await;
        let file_size = bytes.len();
        let byte_stream =
            Box::pin(tokio_stream::iter([Bytes::from(bytes)]).map(object_store::Result::Ok));
        local_store
            .write_object(&location, file_size as i64, byte_stream)
            .await
            .expect("should write parquet file to local store");

        // test
        let decoded_metadata = read_handler.read_decoded_metadata(&location).await;
        assert_matches!(
            decoded_metadata,
            Ok(got) if format_meta_partial_equals_file_meta(got.parquet_file_meta(), &parquet_format_filemeta)
        );
    }
}
