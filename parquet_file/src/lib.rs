//! Parquet file generation, storage, and metadata implementations.

#![warn(missing_docs)]
#![allow(clippy::missing_docs_in_private_items)]

use std::str::FromStr;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod chunk;
pub mod metadata;
pub mod serialize;
pub mod storage;
pub mod writer;

use data_types::{
    NamespaceId, ObjectStoreId, ParquetFile, ParquetFileParams, TableId, TransitionPartitionId,
};
use object_store::path::Path;

/// Location of a Parquet file within a namespace's object store.
/// The exact format is an implementation detail and is subject to change, so it's not intended to
/// be able to parse strings back into this format.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ParquetFilePath {
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: TransitionPartitionId,
    object_store_id: ObjectStoreId,
}

impl ParquetFilePath {
    /// Create parquet file path relevant for the storage layout.
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: &TransitionPartitionId,
        object_store_id: ObjectStoreId,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            partition_id: partition_id.clone(),
            object_store_id,
        }
    }

    /// Get namespace ID.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Get table ID.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Get partition ID.
    pub fn partition_id(&self) -> String {
        self.partition_id.to_string()
    }

    /// Get object-store path.
    pub fn object_store_path(&self) -> Path {
        let Self {
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
        } = self;
        Path::from_iter([
            namespace_id.to_string().as_str(),
            table_id.to_string().as_str(),
            partition_id.to_string().as_str(),
            &format!("{object_store_id}.parquet"),
        ])
    }

    /// Get object store ID.
    pub fn object_store_id(&self) -> ObjectStoreId {
        self.object_store_id
    }

    /// Set new object store ID.
    pub fn with_object_store_id(self, object_store_id: ObjectStoreId) -> Self {
        Self {
            object_store_id,
            ..self
        }
    }

    /// extract file uuid from file path: "database_id/table_id/parition_hash_id/file_uuid.parquet"
    pub fn uuid_from_path(file_path: &Path) -> Option<ObjectStoreId> {
        file_path.parts().nth(3).and_then(|file_uuid| {
            let file_uuid = file_uuid.as_ref();
            // check if ".parquet" is at the end, if so, remove it
            let file_uuid = file_uuid.strip_suffix(".parquet").unwrap_or(file_uuid);
            ObjectStoreId::from_str(file_uuid).ok()
        })
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        borrowed.clone()
    }
}

impl From<(&TransitionPartitionId, &crate::metadata::IoxMetadata)> for ParquetFilePath {
    fn from((partition_id, m): (&TransitionPartitionId, &crate::metadata::IoxMetadata)) -> Self {
        Self {
            namespace_id: m.namespace_id,
            table_id: m.table_id,
            partition_id: partition_id.clone(),
            object_store_id: m.object_store_id,
        }
    }
}

impl From<&ParquetFile> for ParquetFilePath {
    fn from(f: &ParquetFile) -> Self {
        Self {
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            partition_id: TransitionPartitionId::from_parts(
                f.partition_id,
                f.partition_hash_id.clone(),
            ),
            object_store_id: f.object_store_id,
        }
    }
}

impl From<&ParquetFileParams> for ParquetFilePath {
    fn from(f: &ParquetFileParams) -> Self {
        let partition_id =
            TransitionPartitionId::from_parts(f.partition_id, f.partition_hash_id.clone());

        Self {
            partition_id,
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            object_store_id: f.object_store_id,
        }
    }
}

impl std::fmt::Display for ParquetFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.object_store_path().as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{PartitionId, PartitionKey, TransitionPartitionId};
    use uuid::Uuid;

    #[test]
    fn parquet_file_absolute_dirs_and_file_path_database_partition_ids() {
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            ObjectStoreId::from_uuid(Uuid::nil()),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/4/00000000-0000-0000-0000-000000000000.parquet",
        );
    }

    #[test]
    fn parquet_file_absolute_dirs_and_file_path_deterministic_partition_ids() {
        let table_id = TableId::new(2);
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            table_id,
            &TransitionPartitionId::new(table_id, &PartitionKey::from("hello there")),
            ObjectStoreId::from_uuid(Uuid::nil()),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/d10f045c8fb5589e1db57a0ab650175c422310a1474b4de619cc2ded48f65b81\
            /00000000-0000-0000-0000-000000000000.parquet",
        );
    }
}
