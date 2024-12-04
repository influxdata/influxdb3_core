//! Parquet file generation, storage, and metadata implementations.

#![warn(missing_docs)]
#![allow(clippy::missing_docs_in_private_items)]

use std::{num::ParseIntError, str::FromStr};

use hex::FromHexError;
use snafu::{OptionExt, ResultExt, Snafu};
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod chunk;
pub mod metadata;
pub mod serialize;
pub mod storage;
pub mod writer;

use data_types::{
    NamespaceId, ObjectStoreId, ParquetFile, ParquetFileParams, PartitionHashId,
    PartitionHashIdError, PartitionId, TableId, TransitionPartitionId,
};
use object_store::path::Path;

/// Location of a Parquet file within a namespace's object store.
/// The exact format is an implementation detail and is subject to change, so it's not intended to
/// be able to parse strings back into this format.
///
/// # Object Store Path
/// [`ParquetFilePath`] can be converted to/from [`object_store::path::Path`]:
///
/// ```
/// # use data_types::{NamespaceId, ObjectStoreId, PartitionId, TableId, TransitionPartitionId};
/// # use parquet_file::ParquetFilePath;
/// # use uuid::Uuid;
/// #
/// let parquet_path = ParquetFilePath::new(
///     NamespaceId::new(1),
///     TableId::new(2),
///     &TransitionPartitionId::Catalog(PartitionId::new(3)),
///     ObjectStoreId::from_uuid(Uuid::nil()),
/// );
///
/// let os_path = parquet_path.object_store_path();
/// assert_eq!(
///     os_path.to_string(),
///     "1/2/3/00000000-0000-0000-0000-000000000000.parquet",
/// );
///
/// let parquet_path2 = ParquetFilePath::try_from(os_path).unwrap();
/// assert_eq!(parquet_path, parquet_path2);
/// ```
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

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ParseError {
    #[snafu(display("Parse namespace ID: Could not find part"))]
    NamespaceIdPartNotFound,

    #[snafu(display("Parse namespace ID: Not an i64 ('{s}'): {source}"))]
    NamespaceIdNotI64 { s: String, source: ParseIntError },

    #[snafu(display("Parse table ID: Could not find part"))]
    TableIdPartNotFound,

    #[snafu(display("Parse table ID: Not an i64 ('{s}'): {source}"))]
    TableIdNotI64 { s: String, source: ParseIntError },

    #[snafu(display("Parse partition ID: Could not find part"))]
    PartitionIdNotFound,

    #[snafu(display(
        "Parse partition ID: String ('{s}') is not an i64 ({int_err}) but also not hex ({hex_err})"
    ))]
    PartitionIdNotI64AndNotHex {
        s: String,
        int_err: ParseIntError,
        #[snafu(source)]
        hex_err: FromHexError,
    },

    #[snafu(display(
        "Parse partition ID: String ('{s}') is not an i64 ({int_err}) but also not the right length hex data ({len_err})"
    ))]
    PartitionIdNotI64AndNotRightLength {
        s: String,
        int_err: ParseIntError,
        #[snafu(source)]
        len_err: PartitionHashIdError,
    },

    #[snafu(display("Parse object store ID: Could not find part"))]
    ObjectStoreIdPartNotFound,

    #[snafu(display("Parse object store ID: String ('{s}') not end in '.parquet'"))]
    ObjectStoreIdWrongSuffix { s: String },

    #[snafu(display("Parse object store ID: Filename ('{name}') a UUID: {source}"))]
    ObjectStoreIdNotAUuid { name: String, source: uuid::Error },

    #[snafu(display("Trailing data: '{s}'"))]
    TrailingData { s: String },
}

impl TryFrom<&Path> for ParquetFilePath {
    type Error = ParseError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let mut parts_it = path.parts();

        let namespace_id_str = parts_it.next().context(NamespaceIdPartNotFoundSnafu)?;
        let namespace_id_str = namespace_id_str.as_ref();
        let namespace_id = NamespaceId::new(i64::from_str(namespace_id_str).context(
            NamespaceIdNotI64Snafu {
                s: namespace_id_str,
            },
        )?);

        let table_id_str = parts_it.next().context(TableIdPartNotFoundSnafu)?;
        let table_id_str = table_id_str.as_ref();
        let table_id = TableId::new(
            i64::from_str(table_id_str).context(TableIdNotI64Snafu { s: table_id_str })?,
        );

        let partition_id_str = parts_it.next().context(PartitionIdNotFoundSnafu)?;
        let partition_id_str = partition_id_str.as_ref();
        let partition_id = match i64::from_str(partition_id_str) {
            Ok(id) => TransitionPartitionId::Catalog(PartitionId::new(id)),
            Err(int_err) => TransitionPartitionId::Deterministic(
                PartitionHashId::try_from(
                    hex::decode(partition_id_str)
                        .with_context(|_| PartitionIdNotI64AndNotHexSnafu {
                            s: partition_id_str,
                            int_err: int_err.clone(),
                        })?
                        .as_slice(),
                )
                .context(PartitionIdNotI64AndNotRightLengthSnafu {
                    s: partition_id_str,
                    int_err,
                })?,
            ),
        };

        let object_store_id_str = parts_it.next().context(ObjectStoreIdPartNotFoundSnafu)?;
        let object_store_id_str = object_store_id_str.as_ref();
        let object_store_id_name = object_store_id_str.strip_suffix(".parquet").context(
            ObjectStoreIdWrongSuffixSnafu {
                s: object_store_id_str,
            },
        )?;
        let object_store_id =
            ObjectStoreId::from_str(object_store_id_name).context(ObjectStoreIdNotAUuidSnafu {
                name: object_store_id_name,
            })?;

        if let Some(part) = parts_it.next() {
            return Err(ParseError::TrailingData {
                s: Path::from_iter(std::iter::once(part).chain(parts_it)).to_string(),
            });
        }

        Ok(Self::new(
            namespace_id,
            table_id,
            &partition_id,
            object_store_id,
        ))
    }
}

impl TryFrom<Path> for ParquetFilePath {
    type Error = ParseError;

    fn try_from(path: Path) -> Result<Self, Self::Error> {
        Self::try_from(&path)
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
    use assert_matches::assert_matches;
    use data_types::{PartitionId, PartitionKey, TransitionPartitionId};
    use uuid::Uuid;

    #[test]
    fn parquet_file_absolute_dirs_and_file_path_database_partition_ids() {
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Catalog(PartitionId::new(4)),
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
            &TransitionPartitionId::deterministic(table_id, &PartitionKey::from("hello there")),
            ObjectStoreId::from_uuid(Uuid::nil()),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/d10f045c8fb5589e1db57a0ab650175c422310a1474b4de619cc2ded48f65b81\
            /00000000-0000-0000-0000-000000000000.parquet",
        );
    }

    #[test]
    fn parse_path() {
        assert_path_parse_roundtrip(
            ParquetFilePath::new(
                NamespaceId::new(1),
                TableId::new(2),
                &TransitionPartitionId::Catalog(PartitionId::new(3)),
                ObjectStoreId::from_uuid(Uuid::nil()),
            ),
            "1/2/3/00000000-0000-0000-0000-000000000000.parquet",
        );
        assert_path_parse_roundtrip(
            ParquetFilePath::new(
                NamespaceId::new(1),
                TableId::new(2),
                &TransitionPartitionId::Deterministic(
                    PartitionHashId::try_from(
                        hex::decode("ebd1041daa7c644c99967b817ae607bdcb754c663f2c415f270d6df720280f7a").unwrap().as_slice(),
                    ).unwrap()
                ),
                ObjectStoreId::from_uuid(Uuid::nil()),
            ),
                    "1/2/ebd1041daa7c644c99967b817ae607bdcb754c663f2c415f270d6df720280f7a/00000000-0000-0000-0000-000000000000.parquet",
        );

        assert_matches!(
            ParquetFilePath::try_from(Path::parse("").unwrap()),
            Err(ParseError::NamespaceIdPartNotFound)
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("x/2/3/00000000-0000-0000-0000-000000000000.parquet").unwrap()
            ),
            Err(ParseError::NamespaceIdNotI64 { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(Path::parse("1").unwrap()),
            Err(ParseError::TableIdPartNotFound)
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/x/3/00000000-0000-0000-0000-000000000000.parquet").unwrap()
            ),
            Err(ParseError::TableIdNotI64 { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(Path::parse("1/2").unwrap()),
            Err(ParseError::PartitionIdNotFound)
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/foo/00000000-0000-0000-0000-000000000000.parquet").unwrap()
            ),
            Err(ParseError::PartitionIdNotI64AndNotHex { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/ebd1041daa7c64/00000000-0000-0000-0000-000000000000.parquet/x")
                    .unwrap()
            ),
            Err(ParseError::PartitionIdNotI64AndNotRightLength { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(Path::parse("1/2/3").unwrap()),
            Err(ParseError::ObjectStoreIdPartNotFound)
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/3/00000000-0000-0000-0000-000000000000/x").unwrap()
            ),
            Err(ParseError::ObjectStoreIdWrongSuffix { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/3/00000000-0000-0000-0000-000000000000.foo/x").unwrap()
            ),
            Err(ParseError::ObjectStoreIdWrongSuffix { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(Path::parse("1/2/3/foo.parquet/x").unwrap()),
            Err(ParseError::ObjectStoreIdNotAUuid { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/3/00000000-0000-0000-0000-000000000000.parquet/x").unwrap()
            ),
            Err(ParseError::TrailingData { .. })
        );
        assert_matches!(
            ParquetFilePath::try_from(
                Path::parse("1/2/3/00000000-0000-0000-0000-000000000000.parquet/x/y").unwrap()
            ),
            Err(ParseError::TrailingData { .. })
        );
    }

    #[track_caller]
    fn assert_path_parse_roundtrip(orig_file: ParquetFilePath, orig_path: &'static str) {
        let orig_path = Path::parse(orig_path).unwrap();
        let actual_path = orig_file.object_store_path();
        assert_eq!(orig_path, actual_path);
        let actual_file = ParquetFilePath::try_from(actual_path).unwrap();
        assert_eq!(actual_file, orig_file);
    }
}
