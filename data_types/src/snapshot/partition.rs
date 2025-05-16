//! Snapshot definition for partitions

use crate::snapshot::list::MessageList;
use crate::snapshot::mask::{BitMask, BitMaskBuilder};
use crate::{
    ColumnId, ColumnSet, CompactionLevelProtoError, NamespaceId, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileSource, Partition, PartitionHashId, PartitionHashIdError,
    PartitionId, PartitionKey, SkippedCompaction, SortKeyIds, TableId, Timestamp,
};
use bytes::Bytes;
use generated_types::influxdata::iox::{
    catalog_cache::v1 as proto, skipped_compaction::v1 as skipped_compaction_proto,
};
use snafu::{OptionExt, ResultExt, Snafu};

/// Error for [`PartitionSnapshot`]
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Error decoding PartitionFile: {source}"))]
    FileDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error encoding ParquetFile: {source}"))]
    FileEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Missing required field {field}"))]
    RequiredField { field: &'static str },

    #[snafu(context(false))]
    CompactionLevel { source: CompactionLevelProtoError },

    #[snafu(context(false))]
    PartitionHashId { source: PartitionHashIdError },

    #[snafu(display("Invalid partition key: {source}"))]
    PartitionKey { source: std::str::Utf8Error },
}

/// Result for [`PartitionSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A snapshot of a partition
///
/// # Soft Deletion
/// This snapshot does NOT contains soft-deleted parquet files.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    /// The [`NamespaceId`]
    namespace_id: NamespaceId,
    /// The [`TableId`]
    table_id: TableId,
    /// The [`PartitionId`]
    partition_id: PartitionId,
    /// The [`PartitionHashId`]
    partition_hash_id: Option<PartitionHashId>,
    /// The generation of this snapshot
    generation: u64,
    /// The partition key
    key: Bytes,
    /// The files
    files: MessageList<proto::PartitionFile>,
    /// The columns for this partition
    columns: ColumnSet,
    /// The sort key ids
    sort_key: SortKeyIds,
    /// The time of a new file
    new_file_at: Option<Timestamp>,
    /// Skipped compaction.
    skipped_compaction: Option<skipped_compaction_proto::SkippedCompaction>,
    /// The time of the last cold compaction
    cold_compact_at: Option<Timestamp>,
    /// The time this Partition was created at, or `None` if this partition was created before this
    /// field existed. Not the time the snapshot was created.
    created_at: Option<Timestamp>,
}

impl PartitionSnapshot {
    /// Create a new [`PartitionSnapshot`] from the provided state
    pub fn encode(
        namespace_id: NamespaceId,
        partition: Partition,
        files: Vec<ParquetFile>,
        skipped_compaction: Option<SkippedCompaction>,
        generation: u64,
    ) -> Result<Self> {
        // Iterate in reverse order as schema additions are normally additive and
        // so the later files will typically have more columns
        let columns = files.iter().rev().fold(ColumnSet::empty(), |mut acc, v| {
            acc.union(&v.column_set);
            acc
        });

        let files = files
            .into_iter()
            .map(|file| {
                let mut mask = BitMaskBuilder::new(columns.len());
                for (idx, _) in columns.intersect(&file.column_set) {
                    mask.set_bit(idx);
                }

                proto::PartitionFile {
                    id: file.id.get(),
                    object_store_uuid: Some(file.object_store_id.get_uuid().into()),
                    min_time: file.min_time.0,
                    max_time: file.max_time.0,
                    file_size_bytes: file.file_size_bytes,
                    row_count: file.row_count,
                    compaction_level: file.compaction_level as _,
                    created_at: file.created_at.0,
                    max_l0_created_at: file.max_l0_created_at.0,
                    column_mask: Some(mask.finish().into()),
                    source: file.source.map(|i| i as i32).unwrap_or_default(),
                    use_numeric_partition_id: Some(file.partition_hash_id.is_none()),
                }
            })
            .collect::<Vec<_>>();

        Ok(Self {
            generation,
            columns,
            namespace_id,
            partition_id: partition.id,
            partition_hash_id: partition.hash_id().cloned(),
            key: partition.partition_key.as_bytes().to_vec().into(),
            files: MessageList::encode(&files).context(FileEncodeSnafu)?,
            sort_key: partition.sort_key_ids().cloned().unwrap_or_default(),
            table_id: partition.table_id,
            new_file_at: partition.new_file_at,
            skipped_compaction: skipped_compaction.map(|sc| sc.into()),
            cold_compact_at: partition.cold_compact_at,
            created_at: partition.created_at(),
        })
    }

    /// Create a new [`PartitionSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Partition, generation: u64) -> Self {
        let table_id = TableId::new(proto.table_id);
        let partition_hash_id = proto
            .partition_hash_id
            .then(|| PartitionHashId::from_raw(table_id, proto.key.as_ref()));

        Self {
            generation,
            table_id,
            partition_hash_id,
            key: proto.key,
            files: MessageList::from(proto.files.unwrap_or_default()),
            namespace_id: NamespaceId::new(proto.namespace_id),
            partition_id: PartitionId::new(proto.partition_id),
            columns: ColumnSet::new(proto.column_ids.into_iter().map(ColumnId::new)),
            sort_key: SortKeyIds::new(proto.sort_key_ids.into_iter().map(ColumnId::new)),
            new_file_at: proto.new_file_at.map(Timestamp::new),
            skipped_compaction: proto.skipped_compaction,
            cold_compact_at: proto.cold_compact_at.map(Timestamp::new),
            created_at: proto.created_at.map(Timestamp::new),
        }
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the [`PartitionId`]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Returns the [`PartitionHashId`] if any
    pub fn partition_hash_id(&self) -> Option<&PartitionHashId> {
        self.partition_hash_id.as_ref()
    }

    /// Returns the file at index `idx`
    pub fn file(&self, idx: usize) -> Result<ParquetFile> {
        let file = self.files.get(idx).context(FileDecodeSnafu)?;

        let uuid = file.object_store_uuid.context(RequiredFieldSnafu {
            field: "object_store_uuid",
        })?;

        let column_set = match file.column_mask {
            Some(mask) => {
                let mask = BitMask::from(mask);
                ColumnSet::new(mask.set_indices().map(|idx| self.columns[idx]))
            }
            None => self.columns.clone(),
        };

        Ok(ParquetFile {
            id: ParquetFileId(file.id),
            namespace_id: self.namespace_id,
            table_id: self.table_id,
            partition_id: self.partition_id,
            partition_hash_id: match file.use_numeric_partition_id {
                // If the Parquet file uses the numeric partition ID, don't set a
                // `partition_hash_id`, regardless of whether the Partition uses a `hash_id`
                Some(true) => None,
                Some(false) => Some(match self.partition_hash_id.clone() {
                    Some(hash_id) => hash_id,
                    // If the Parquet file uses the hash ID but the Partition doesn't yet,
                    // compute it
                    None => self
                        .key()
                        .map(|key| PartitionHashId::new(self.table_id, &key))?,
                }),
                // If the Parquet file doesn't specify whether it uses a hash ID, fall back to
                // whatever the Partition uses
                None => self.partition_hash_id.clone(),
            },
            object_store_id: ObjectStoreId::from_uuid(uuid.into()),
            min_time: Timestamp(file.min_time),
            max_time: Timestamp(file.max_time),
            to_delete: None,
            file_size_bytes: file.file_size_bytes,
            row_count: file.row_count,
            compaction_level: file.compaction_level.try_into()?,
            created_at: Timestamp(file.created_at),
            column_set,
            max_l0_created_at: Timestamp(file.max_l0_created_at),
            source: ParquetFileSource::from_proto(file.source),
        })
    }

    /// Returns an iterator over the files in this snapshot
    pub fn files(&self) -> impl Iterator<Item = Result<ParquetFile>> + '_ {
        (0..self.files.len()).map(|idx| self.file(idx))
    }

    fn key(&self) -> Result<PartitionKey> {
        Ok(std::str::from_utf8(&self.key)
            .context(PartitionKeySnafu)?
            .into())
    }

    /// Returns the [`Partition`] for this snapshot
    pub fn partition(&self) -> Result<Partition> {
        Ok(Partition::new_catalog_only(
            self.partition_id,
            self.partition_hash_id.clone(),
            self.table_id,
            self.key()?,
            self.sort_key.clone(),
            self.new_file_at,
            self.cold_compact_at,
            self.created_at,
        ))
    }

    /// Returns the columns IDs
    pub fn column_ids(&self) -> &ColumnSet {
        &self.columns
    }

    /// Return skipped compaction for this partition, if any.
    pub fn skipped_compaction(&self) -> Option<SkippedCompaction> {
        self.skipped_compaction
            .as_ref()
            .cloned()
            .map(|sc| sc.into())
    }
}

impl From<PartitionSnapshot> for proto::Partition {
    fn from(value: PartitionSnapshot) -> Self {
        Self {
            key: value.key,
            files: Some(value.files.into()),
            namespace_id: value.namespace_id.get(),
            table_id: value.table_id.get(),
            partition_id: value.partition_id.get(),
            partition_hash_id: value.partition_hash_id.is_some(),
            column_ids: value.columns.iter().map(|x| x.get()).collect(),
            sort_key_ids: value.sort_key.iter().map(|x| x.get()).collect(),
            new_file_at: value.new_file_at.map(|x| x.get()),
            skipped_compaction: value.skipped_compaction,
            cold_compact_at: value.cold_compact_at.map(|x| x.get()),
            created_at: value.created_at.map(|x| x.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompactionLevel, PartitionKey};
    use std::str::FromStr;

    #[test]
    fn partition_hash_id_transition_parquet_files_individually() {
        let namespace_id = NamespaceId::new(3);
        let table_id = TableId::new(4);
        let partition_id = PartitionId::new(5);
        let partition_key = PartitionKey::from("arbitrary");
        let expected_partition_hash_id = PartitionHashId::new(table_id, &partition_key);
        let generation = 6;
        let parquet_file_defaults = ParquetFile {
            id: ParquetFileId::new(7),
            namespace_id,
            table_id,
            partition_id,
            partition_hash_id: Some(expected_partition_hash_id.clone()),
            object_store_id: ObjectStoreId::from_str("00000000-0000-0001-0000-000000000000")
                .unwrap(),
            min_time: Timestamp::new(2),
            max_time: Timestamp::new(3),
            to_delete: None,
            file_size_bytes: 4,
            row_count: 5,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(6),
            column_set: ColumnSet::empty(),
            max_l0_created_at: Timestamp::new(6),
            source: None,
        };

        let encode_and_compare = |use_partition_hash_id: bool| {
            // For a partition with or without a hash ID as specified,
            let partition = Partition::new_catalog_only(
                partition_id,
                if use_partition_hash_id {
                    Some(expected_partition_hash_id.clone())
                } else {
                    None
                },
                table_id,
                partition_key.clone(),
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            );
            // Create associated Parquet files:
            let parquet_files = vec![
                // one addressed by numeric ID,
                ParquetFile {
                    partition_hash_id: None,
                    ..parquet_file_defaults.clone()
                },
                // one addressed by hash ID.
                parquet_file_defaults.clone(),
            ];

            // Encode the partition and its Parquet files,
            let encoded_partition = PartitionSnapshot::encode(
                namespace_id,
                partition,
                parquet_files.clone(),
                None,
                generation,
            )
            .unwrap();

            // then ensure accessing each Parquet file returns the same information as was encoded.
            assert_eq!(
                &encoded_partition.file(0).unwrap(),
                &parquet_files[0],
                "use_partition_hash_id: {use_partition_hash_id}"
            );
            assert_eq!(
                &encoded_partition.file(1).unwrap(),
                &parquet_files[1],
                "use_partition_hash_id: {use_partition_hash_id}"
            );
        };

        // Encoding and accessing Parquet files should work whether their associated Partition
        // has a hash ID or not.
        encode_and_compare(true);
        encode_and_compare(false);
    }

    #[test]
    fn decode_old_cached_proto() {
        let partition_key = PartitionKey::from("arbitrary");

        // Create cached proto for three different files:
        //
        // 1. without "use_numeric_partition_id", representing proto cached before the field was
        //    added
        // 2. with "use_numeric_partition_id" = false
        // 3. with "use_numeric_partition_id" = true
        let parquet_file_missing_new_numeric_id_field_proto = proto::PartitionFile {
            id: 1,
            use_numeric_partition_id: None,

            column_mask: Default::default(),
            compaction_level: Default::default(),
            created_at: Default::default(),
            file_size_bytes: Default::default(),
            max_l0_created_at: Default::default(),
            min_time: Default::default(),
            max_time: Default::default(),
            object_store_uuid: Some(ObjectStoreId::new().get_uuid().into()),
            row_count: Default::default(),
            source: Default::default(),
        };
        let parquet_file_new_numeric_id_field_false_proto = proto::PartitionFile {
            id: 2,
            use_numeric_partition_id: Some(false),
            ..parquet_file_missing_new_numeric_id_field_proto.clone()
        };
        let parquet_file_new_numeric_id_field_true_proto = proto::PartitionFile {
            id: 3,
            use_numeric_partition_id: Some(true),
            ..parquet_file_missing_new_numeric_id_field_proto.clone()
        };

        let files = MessageList::encode(&[
            parquet_file_missing_new_numeric_id_field_proto,
            parquet_file_new_numeric_id_field_false_proto,
            parquet_file_new_numeric_id_field_true_proto,
        ])
        .unwrap();
        let files_proto: proto::MessageList = files.into();

        // Create cached proto for two different Partitions:
        //
        // 1. Identified with a hash ID (new style)
        // 2. Identified only with a numeric ID (old style)
        //
        // and add the encoded Parquet file message list to each of them.
        let hash_id_partition_proto = proto::Partition {
            partition_hash_id: true,
            partition_id: 6,

            namespace_id: 4,
            table_id: 5,
            cold_compact_at: Default::default(),
            created_at: Default::default(),
            column_ids: Default::default(),
            files: Some(files_proto.clone()),
            key: partition_key.as_bytes().to_vec().into(),
            new_file_at: Default::default(),
            skipped_compaction: Default::default(),
            sort_key_ids: Default::default(),
        };
        let numeric_id_partition_proto = proto::Partition {
            partition_hash_id: false,
            partition_id: 7,
            ..hash_id_partition_proto.clone()
        };

        let decoded_hash_id_partition = PartitionSnapshot::decode(hash_id_partition_proto, 1);
        let decoded_numeric_id_partition = PartitionSnapshot::decode(numeric_id_partition_proto, 1);

        // For the Parquet file without `use_numeric_partition_id` set, it should be addressed in
        // the same way as its partition is.
        let pf0_hash_id_partition = decoded_hash_id_partition.file(0).unwrap();
        assert_eq!(
            pf0_hash_id_partition.partition_hash_id,
            Some(decoded_hash_id_partition.partition_hash_id.clone().unwrap())
        );
        let pf0_numeric_id_partition = decoded_numeric_id_partition.file(0).unwrap();
        assert_eq!(pf0_numeric_id_partition.partition_hash_id, None);

        // For the Parquet file with `use_numeric_partition_id` set to `false`, it should be
        // addressed with hash ID, regardless of how the partition is addressed.
        let pf1_hash_id_partition = decoded_hash_id_partition.file(1).unwrap();
        assert_eq!(
            pf1_hash_id_partition.partition_hash_id,
            Some(decoded_hash_id_partition.partition_hash_id.clone().unwrap())
        );
        let pf1_numeric_id_partition = decoded_numeric_id_partition.file(1).unwrap();
        assert_eq!(
            pf1_numeric_id_partition.partition_hash_id,
            Some(PartitionHashId::new(
                decoded_numeric_id_partition.table_id,
                &decoded_numeric_id_partition.key().unwrap()
            ))
        );

        // For the Parquet file with `use_numeric_partition_id` set to `true`, it should be
        // addressed with numeric ID, regardless of how the partition is addressed.
        let pf1_hash_id_partition = decoded_hash_id_partition.file(2).unwrap();
        assert_eq!(pf1_hash_id_partition.partition_hash_id, None);
        let pf1_numeric_id_partition = decoded_numeric_id_partition.file(2).unwrap();
        assert_eq!(pf1_numeric_id_partition.partition_hash_id, None);
    }
}
