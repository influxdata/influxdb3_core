use data_types::{
    Column, ColumnId, ColumnSet, ColumnType, CompactionLevel, MaxL0CreatedAt, Namespace,
    NamespaceId, NamespaceVersion, ObjectStoreId, ParquetFile, ParquetFileId, ParquetFileParams,
    ParquetFileSource, Partition, PartitionId, SkippedCompaction, SortKeyIds, Table, TableId,
    Timestamp, partition_template::NamespacePartitionTemplateOverride,
};
use generated_types::google::protobuf as google;
use generated_types::influxdata::iox::catalog::v2 as proto;
use uuid::Uuid;

use crate::util_serialization::{ContextExt, ConvertExt, ConvertOptExt, Error, RequiredExt};

pub(crate) fn serialize_namespace(ns: Namespace) -> proto::Namespace {
    proto::Namespace {
        id: ns.id.get(),
        name: ns.name,
        retention_period_ns: ns.retention_period_ns,
        max_tables: ns.max_tables.get_i32(),
        max_columns_per_table: ns.max_columns_per_table.get_i32(),
        deleted_at: ns.deleted_at.map(|ts| ts.get()),
        partition_template: ns.partition_template.as_proto().cloned(),
        router_version: ns.router_version.get(),
    }
}

pub(crate) fn deserialize_namespace(ns: proto::Namespace) -> Result<Namespace, Error> {
    Ok(Namespace {
        id: NamespaceId::new(ns.id),
        name: ns.name,
        retention_period_ns: ns.retention_period_ns,
        max_tables: ns.max_tables.convert().ctx("max_tables")?,
        max_columns_per_table: ns
            .max_columns_per_table
            .convert()
            .ctx("max_columns_per_table")?,
        deleted_at: ns.deleted_at.map(Timestamp::new),
        partition_template: ns
            .partition_template
            .convert_opt()
            .ctx("partition_template")?
            .unwrap_or_else(NamespacePartitionTemplateOverride::const_default),
        router_version: NamespaceVersion::new(ns.router_version),
    })
}

pub(crate) fn serialize_table(t: Table) -> proto::Table {
    proto::Table {
        id: t.id.get(),
        namespace_id: t.namespace_id.get(),
        name: t.name,
        partition_template: t.partition_template.as_proto().cloned(),
        iceberg_enabled: t.iceberg_enabled,
        deleted_at: t.deleted_at.map(|t| t.get()),
    }
}

pub(crate) fn deserialize_table(t: proto::Table) -> Result<Table, Error> {
    Ok(Table {
        id: TableId::new(t.id),
        namespace_id: NamespaceId::new(t.namespace_id),
        name: t.name,
        partition_template: t.partition_template.convert().ctx("partition_template")?,
        iceberg_enabled: t.iceberg_enabled,
        deleted_at: t.deleted_at.map(Timestamp::new),
    })
}

pub(crate) fn serialize_timestamp(t: iox_time::Time) -> google::Timestamp {
    google::Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}

pub(crate) fn deserialize_timestamp(t: google::Timestamp) -> Result<iox_time::Time, Error> {
    let time = iox_time::Time::from_timestamp(t.seconds, t.nanos as u32);
    time.ok_or(Error::new(format!(
        "timestamp {t:?} could not be converted to iox time"
    )))
}

pub(crate) fn serialize_column_type(t: ColumnType) -> i32 {
    use generated_types::influxdata::iox::column_type::v1 as proto;
    proto::ColumnType::from(t).into()
}

pub(crate) fn deserialize_column_type(t: i32) -> Result<ColumnType, Error> {
    use generated_types::influxdata::iox::column_type::v1 as proto;
    let t: proto::ColumnType = t.convert()?;
    t.convert()
}

pub(crate) fn serialize_column(column: Column) -> proto::Column {
    proto::Column {
        id: column.id.get(),
        table_id: column.table_id.get(),
        name: column.name,
        column_type: serialize_column_type(column.column_type),
    }
}

pub(crate) fn deserialize_column(column: proto::Column) -> Result<Column, Error> {
    Ok(Column {
        id: ColumnId::new(column.id),
        table_id: TableId::new(column.table_id),
        name: column.name,
        column_type: deserialize_column_type(column.column_type)?,
    })
}

pub(crate) fn serialize_sort_key_ids(sort_key_ids: &SortKeyIds) -> proto::SortKeyIds {
    proto::SortKeyIds {
        column_ids: sort_key_ids.iter().map(|c_id| c_id.get()).collect(),
    }
}

pub(crate) fn deserialize_sort_key_ids(sort_key_ids: proto::SortKeyIds) -> SortKeyIds {
    SortKeyIds::new(sort_key_ids.column_ids.into_iter().map(ColumnId::new))
}

pub(crate) fn serialize_partition(partition: Partition) -> proto::Partition {
    let empty_sk = SortKeyIds::new(std::iter::empty());

    proto::Partition {
        id: partition.id.get(),
        hash_id: partition
            .hash_id()
            .map(|id| id.as_bytes().to_vec())
            .unwrap_or_default(),
        partition_key: partition.partition_key.inner().to_owned(),
        table_id: partition.table_id.get(),
        sort_key_ids: Some(serialize_sort_key_ids(
            partition.sort_key_ids().unwrap_or(&empty_sk),
        )),
        new_file_at: partition.new_file_at.map(|ts| ts.get()),
        cold_compact_at: partition.cold_compact_at.map(|ts| ts.get()),
        created_at: partition.created_at().map(|ts| ts.get()),
    }
}

pub(crate) fn deserialize_partition(partition: proto::Partition) -> Result<Partition, Error> {
    Ok(Partition::new_catalog_only(
        PartitionId::new(partition.id),
        (!partition.hash_id.is_empty())
            .then_some(partition.hash_id.as_slice())
            .convert_opt()
            .ctx("hash_id")?,
        TableId::new(partition.table_id),
        partition.partition_key.into(),
        deserialize_sort_key_ids(partition.sort_key_ids.required().ctx("sort_key_ids")?),
        partition.new_file_at.map(Timestamp::new),
        partition.cold_compact_at.map(Timestamp::new),
        partition.created_at.map(Timestamp::new),
    ))
}

pub(crate) fn serialize_skipped_compaction(sc: SkippedCompaction) -> proto::SkippedCompaction {
    proto::SkippedCompaction {
        partition_id: sc.partition_id.get(),
        reason: sc.reason,
        skipped_at: sc.skipped_at.get(),
        estimated_bytes: sc.estimated_bytes,
        limit_bytes: sc.limit_bytes,
        num_files: sc.num_files,
        limit_num_files: sc.limit_num_files,
        limit_num_files_first_in_partition: sc.limit_num_files_first_in_partition,
    }
}

pub(crate) fn deserialize_skipped_compaction(sc: proto::SkippedCompaction) -> SkippedCompaction {
    SkippedCompaction {
        partition_id: PartitionId::new(sc.partition_id),
        reason: sc.reason,
        skipped_at: Timestamp::new(sc.skipped_at),
        estimated_bytes: sc.estimated_bytes,
        limit_bytes: sc.limit_bytes,
        num_files: sc.num_files,
        limit_num_files: sc.limit_num_files,
        limit_num_files_first_in_partition: sc.limit_num_files_first_in_partition,
    }
}

pub(crate) fn serialize_object_store_id(id: ObjectStoreId) -> proto::ObjectStoreId {
    let (high64, low64) = id.get_uuid().as_u64_pair();
    proto::ObjectStoreId { high64, low64 }
}

pub(crate) fn deserialize_object_store_id(id: proto::ObjectStoreId) -> ObjectStoreId {
    ObjectStoreId::from_uuid(Uuid::from_u64_pair(id.high64, id.low64))
}

pub(crate) fn serialize_column_set(set: &ColumnSet) -> proto::ColumnSet {
    proto::ColumnSet {
        column_ids: set.iter().map(|id| id.get()).collect(),
    }
}

pub(crate) fn deserialize_column_set(set: proto::ColumnSet) -> ColumnSet {
    ColumnSet::new(set.column_ids.into_iter().map(ColumnId::new))
}

// See comment in the body for why the use of deprecated `max_l0_created_at` is fine.
#[expect(deprecated)]
pub(crate) fn serialize_parquet_file_params(
    params: &ParquetFileParams,
    created_at: Timestamp,
) -> proto::ParquetFileParams {
    proto::ParquetFileParams {
        namespace_id: params.namespace_id.get(),
        table_id: params.table_id.get(),
        partition_id: params.partition_id.get(),
        partition_hash_id: params
            .partition_hash_id
            .as_ref()
            .map(|id| id.as_bytes().to_vec()),
        object_store_id: Some(serialize_object_store_id(params.object_store_id)),
        min_time: params.min_time.get(),
        max_time: params.max_time.get(),
        file_size_bytes: params.file_size_bytes,
        row_count: params.row_count,
        compaction_level: params.compaction_level as i32,
        created_at: created_at.get(),
        column_set: Some(serialize_column_set(&params.column_set)),

        maybe_max_l0_created_at: Some(
            params
                .max_l0_created_at
                .maybe_computed_timestamp()
                .map(|t| proto::parquet_file_params::MaybeMaxL0CreatedAt::Computed(t.get()))
                .unwrap_or(
                    proto::parquet_file_params::MaybeMaxL0CreatedAt::NotCompacted(
                        Default::default(),
                    ),
                ),
        ),

        // This field is in the process of being deprecated in favor of `maybe_max_l0_created_at`.
        // Services running old code that receive this message may still use this value, but new
        // code will ignore it. When all services in all environments are using
        // `maybe_max_l0_created_at` instead, this field can be removed.
        max_l0_created_at: params
            .max_l0_created_at
            .maybe_computed_timestamp()
            .unwrap_or(created_at)
            .get(),

        source: ParquetFileSource::to_proto(params.source),
    }
}

// See comment in the body for why the use of deprecated `max_l0_created_at` is fine.
#[expect(deprecated)]
pub(crate) fn deserialize_parquet_file_params(
    params: proto::ParquetFileParams,
) -> Result<ParquetFileParams, Error> {
    let compaction_level = params.compaction_level.convert().ctx("compaction_level")?;

    // The proto field `max_l0_created_at` is in the process of being deprecated in favor of
    // `maybe_max_l0_created_at`. If `maybe_max_l0_created_at` is set, use that value. If not, fall
    // back to `max_l0_created_at` as this message came from a service running older code. When all
    // services in all environments are using `maybe_max_l0_created_at` instead, this field can be
    // removed.
    let max_l0_created_at = match params.maybe_max_l0_created_at {
        Some(maybe_max) => match maybe_max {
            proto::parquet_file_params::MaybeMaxL0CreatedAt::Computed(max) => {
                MaxL0CreatedAt::Computed(Timestamp::new(max))
            }
            proto::parquet_file_params::MaybeMaxL0CreatedAt::NotCompacted(_) => {
                MaxL0CreatedAt::NotCompacted
            }
        },
        None => {
            if compaction_level == CompactionLevel::Initial
                && params.max_l0_created_at == params.created_at
            {
                MaxL0CreatedAt::NotCompacted
            } else {
                MaxL0CreatedAt::Computed(Timestamp::new(params.max_l0_created_at))
            }
        }
    };

    Ok(ParquetFileParams {
        namespace_id: NamespaceId::new(params.namespace_id),
        table_id: TableId::new(params.table_id),
        partition_id: PartitionId::new(params.partition_id),
        partition_hash_id: params
            .partition_hash_id
            .as_deref()
            .convert_opt()
            .ctx("partition_hash_id")?,
        object_store_id: deserialize_object_store_id(
            params.object_store_id.required().ctx("object_store_id")?,
        ),
        min_time: Timestamp::new(params.min_time),
        max_time: Timestamp::new(params.max_time),
        file_size_bytes: params.file_size_bytes,
        row_count: params.row_count,
        compaction_level,
        column_set: deserialize_column_set(params.column_set.required().ctx("column_set")?),
        max_l0_created_at,
        source: ParquetFileSource::from_proto(params.source),
    })
}

pub(crate) fn serialize_parquet_file(file: ParquetFile) -> proto::ParquetFile {
    let partition_hash_id = file
        .partition_hash_id
        .map(|x| x.as_bytes().to_vec())
        .unwrap_or_default();

    proto::ParquetFile {
        id: file.id.get(),
        namespace_id: file.namespace_id.get(),
        table_id: file.table_id.get(),
        partition_id: file.partition_id.get(),
        partition_hash_id,
        object_store_id: Some(serialize_object_store_id(file.object_store_id)),
        min_time: file.min_time.get(),
        max_time: file.max_time.get(),
        to_delete: file.to_delete.map(|ts| ts.get()),
        file_size_bytes: file.file_size_bytes,
        row_count: file.row_count,
        compaction_level: file.compaction_level as i32,
        created_at: file.created_at.get(),
        column_set: Some(serialize_column_set(&file.column_set)),
        max_l0_created_at: file.max_l0_created_at.get(),
        source: ParquetFileSource::to_proto(file.source),
    }
}

pub(crate) fn deserialize_parquet_file(file: proto::ParquetFile) -> Result<ParquetFile, Error> {
    let partition_hash_id = match file.partition_hash_id.as_slice() {
        b"" => None,
        s => Some(s.convert().ctx("partition_hash_id")?),
    };

    Ok(ParquetFile {
        id: ParquetFileId::new(file.id),
        namespace_id: NamespaceId::new(file.namespace_id),
        table_id: TableId::new(file.table_id),
        partition_id: PartitionId::new(file.partition_id),
        partition_hash_id,
        object_store_id: deserialize_object_store_id(
            file.object_store_id.required().ctx("object_store_id")?,
        ),
        min_time: Timestamp::new(file.min_time),
        max_time: Timestamp::new(file.max_time),
        to_delete: file.to_delete.map(Timestamp::new),
        file_size_bytes: file.file_size_bytes,
        row_count: file.row_count,
        compaction_level: file.compaction_level.convert().ctx("compaction_level")?,
        created_at: Timestamp::new(file.created_at),
        column_set: deserialize_column_set(file.column_set.required().ctx("column_set")?),
        max_l0_created_at: Timestamp::new(file.max_l0_created_at),
        source: ParquetFileSource::from_proto(file.source),
    })
}

#[cfg(test)]
mod tests {
    use crate::interface::SoftDeletedRows;
    use crate::util_serialization::{
        catalog_error_to_status, convert_status, deserialize_soft_deleted_rows,
        serialize_soft_deleted_rows,
    };
    use data_types::{
        CompactionLevel, PartitionHashId, PartitionKey,
        partition_template::TablePartitionTemplateOverride,
    };

    use super::*;

    #[test]
    fn test_column_type_roundtrip() {
        assert_column_type_roundtrip(ColumnType::Bool);
        assert_column_type_roundtrip(ColumnType::I64);
        assert_column_type_roundtrip(ColumnType::U64);
        assert_column_type_roundtrip(ColumnType::F64);
        assert_column_type_roundtrip(ColumnType::String);
        assert_column_type_roundtrip(ColumnType::Tag);
        assert_column_type_roundtrip(ColumnType::Time);
    }

    #[track_caller]
    fn assert_column_type_roundtrip(t: ColumnType) {
        let protobuf = serialize_column_type(t);
        let t2 = deserialize_column_type(protobuf).unwrap();
        assert_eq!(t, t2);
    }

    #[test]
    fn test_error_roundtrip() {
        use crate::interface::Error;

        assert_error_roundtrip(Error::AlreadyExists {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::LimitExceeded {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::NotFound {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::Malformed {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::NotImplemented {
            descr: "foo".to_owned(),
        });
    }

    #[track_caller]
    fn assert_error_roundtrip(e: crate::interface::Error) {
        let msg_orig = e.to_string();

        let status = catalog_error_to_status(e);
        let e = convert_status(status);
        let msg = e.to_string();
        assert_eq!(msg, msg_orig);
    }

    #[test]
    fn test_soft_deleted_rows_roundtrip() {
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::AllRows);
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::ExcludeDeleted);
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::OnlyDeleted);
    }

    #[track_caller]
    fn assert_soft_deleted_rows_roundtrip(sdr: SoftDeletedRows) {
        let protobuf = serialize_soft_deleted_rows(sdr);
        let sdr2 = deserialize_soft_deleted_rows(protobuf).unwrap();
        assert_eq!(sdr, sdr2);
    }

    #[test]
    fn test_namespace_roundtrip() {
        use generated_types::influxdata::iox::partition_template::v1 as proto;

        let ns = Namespace {
            id: NamespaceId::new(1),
            name: "ns".to_owned(),
            retention_period_ns: Some(2),
            max_tables: 3.try_into().unwrap(),
            max_columns_per_table: 4.try_into().unwrap(),
            deleted_at: Some(Timestamp::new(5)),
            partition_template: NamespacePartitionTemplateOverride::try_from(
                proto::PartitionTemplate {
                    parts: vec![proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    }],
                },
            )
            .unwrap(),
            router_version: NamespaceVersion::new(1),
        };
        let protobuf = serialize_namespace(ns.clone());
        let ns2 = deserialize_namespace(protobuf).unwrap();
        assert_eq!(ns, ns2);
    }

    #[test]
    fn test_table_roundtrip() {
        use generated_types::influxdata::iox::partition_template::v1 as proto;

        let table = Table {
            id: TableId::new(1),
            namespace_id: NamespaceId::new(2),
            name: "table".to_owned(),
            // Using `try_from_existing()` to ensure round-tripping uses the
            // more permissive validation than used by `try_new()`.
            partition_template: TablePartitionTemplateOverride::try_from_existing(
                Some(proto::PartitionTemplate {
                    parts: vec![proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("bananas".into())),
                    }],
                }),
                &NamespacePartitionTemplateOverride::const_default(),
            )
            .unwrap(),
            iceberg_enabled: false,
            deleted_at: Some(Timestamp::new(12345678)),
        };
        let protobuf = serialize_table(table.clone());
        let table2 = deserialize_table(protobuf).unwrap();
        assert_eq!(table, table2);
    }

    #[test]
    fn test_column_roundtrip() {
        let column = Column {
            id: ColumnId::new(1),
            table_id: TableId::new(2),
            name: "col".to_owned(),
            column_type: ColumnType::F64,
        };
        let protobuf = serialize_column(column.clone());
        let column2 = deserialize_column(protobuf).unwrap();
        assert_eq!(column, column2);
    }

    #[test]
    fn test_sort_key_ids_roundtrip() {
        assert_sort_key_ids_roundtrip(SortKeyIds::new(std::iter::empty()));
        assert_sort_key_ids_roundtrip(SortKeyIds::new([ColumnId::new(1)]));
        assert_sort_key_ids_roundtrip(SortKeyIds::new([
            ColumnId::new(1),
            ColumnId::new(5),
            ColumnId::new(20),
        ]));
    }

    #[track_caller]
    fn assert_sort_key_ids_roundtrip(sort_key_ids: SortKeyIds) {
        let protobuf = serialize_sort_key_ids(&sort_key_ids);
        let sort_key_ids2 = deserialize_sort_key_ids(protobuf);
        assert_eq!(sort_key_ids, sort_key_ids2);
    }

    #[test]
    fn test_partition_roundtrip() {
        let table_id = TableId::new(1);
        let partition_key = PartitionKey::from("key");
        let hash_id = PartitionHashId::new(table_id, &partition_key);

        assert_partition_roundtrip(Partition::new_catalog_only(
            PartitionId::new(2),
            Some(hash_id.clone()),
            table_id,
            partition_key.clone(),
            SortKeyIds::new([ColumnId::new(3), ColumnId::new(4)]),
            Some(Timestamp::new(5)),
            Default::default(),
            Some(Timestamp::new(4)),
        ));
        assert_partition_roundtrip(Partition::new_catalog_only(
            PartitionId::new(2),
            Some(hash_id),
            table_id,
            partition_key,
            SortKeyIds::new(std::iter::empty()),
            Some(Timestamp::new(5)),
            Default::default(),
            Some(Timestamp::new(4)),
        ));
    }

    #[track_caller]
    fn assert_partition_roundtrip(partition: Partition) {
        let protobuf = serialize_partition(partition.clone());
        let partition2 = deserialize_partition(protobuf).unwrap();
        assert_eq!(partition, partition2);
    }

    #[test]
    fn test_skipped_compaction_roundtrip() {
        let sc = SkippedCompaction {
            partition_id: PartitionId::new(1),
            reason: "foo".to_owned(),
            skipped_at: Timestamp::new(2),
            estimated_bytes: 3,
            limit_bytes: 4,
            num_files: 5,
            limit_num_files: 6,
            limit_num_files_first_in_partition: 7,
        };
        let protobuf = serialize_skipped_compaction(sc.clone());
        let sc2 = deserialize_skipped_compaction(protobuf);
        assert_eq!(sc, sc2);
    }

    #[test]
    fn test_object_store_id_roundtrip() {
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::nil()));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(0)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(u128::MAX)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(1)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(u128::MAX - 1)));
    }

    #[track_caller]
    fn assert_object_store_id_roundtrip(id: ObjectStoreId) {
        let protobuf = serialize_object_store_id(id);
        let id2 = deserialize_object_store_id(protobuf);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_column_set_roundtrip() {
        assert_column_set_roundtrip(ColumnSet::new([]));
        assert_column_set_roundtrip(ColumnSet::new([ColumnId::new(1)]));
        assert_column_set_roundtrip(ColumnSet::new([ColumnId::new(1), ColumnId::new(10)]));
        assert_column_set_roundtrip(ColumnSet::new([
            ColumnId::new(3),
            ColumnId::new(4),
            ColumnId::new(10),
        ]));
    }

    #[track_caller]
    fn assert_column_set_roundtrip(set: ColumnSet) {
        let protobuf = serialize_column_set(&set);
        let set2 = deserialize_column_set(protobuf);
        assert_eq!(set, set2);
    }

    #[test]
    fn test_parquet_file_params_roundtrip() {
        // This is only needed until `created_at` is fully deprecated
        let created_at = Timestamp::new(8);
        let params = ParquetFileParams {
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(2),
            partition_id: PartitionId::new(3),
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing()),
            object_store_id: ObjectStoreId::from_uuid(Uuid::from_u128(1337)),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(5),
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Final,
            column_set: ColumnSet::new([ColumnId::new(9), ColumnId::new(10)]),
            max_l0_created_at: MaxL0CreatedAt::Computed(Timestamp::new(11)),
            source: None,
        };
        let protobuf = serialize_parquet_file_params(&params, created_at);
        let params2 = deserialize_parquet_file_params(protobuf).unwrap();
        assert_eq!(params, params2);

        let params_without_max_l0_created_at = ParquetFileParams {
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(2),
            partition_id: PartitionId::new(3),
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing()),
            object_store_id: ObjectStoreId::from_uuid(Uuid::from_u128(1337)),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(5),
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Initial,
            column_set: ColumnSet::new([ColumnId::new(9), ColumnId::new(10)]),
            max_l0_created_at: MaxL0CreatedAt::NotCompacted,
            source: None,
        };
        let protobuf = serialize_parquet_file_params(&params_without_max_l0_created_at, created_at);
        let params_without_max_l0_created_at2 = deserialize_parquet_file_params(protobuf).unwrap();
        assert_eq!(
            params_without_max_l0_created_at,
            params_without_max_l0_created_at2
        );
    }

    // This test is exercising uses of the deprecated `max_l0_created_at` field.
    #[expect(deprecated)]
    #[test]
    fn max_l0_created_at_deprecated_but_still_supported() {
        let (high64, low64) = ObjectStoreId::from_uuid(Uuid::from_u128(1337))
            .get_uuid()
            .as_u64_pair();
        let object_store_id = proto::ObjectStoreId { high64, low64 };
        // This is only needed until `created_at` is fully deprecated
        let created_at = Timestamp::new(8);

        let old_proto_l2_max_l0_and_created_at_equal = proto::ParquetFileParams {
            namespace_id: 1,
            table_id: 2,
            partition_id: 3,
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing().as_bytes().to_vec()),
            object_store_id: Some(object_store_id),
            min_time: 4,
            max_time: 5,
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Final as i32,
            created_at: created_at.get(),
            column_set: Some(proto::ColumnSet {
                column_ids: vec![1],
            }),
            // This is simulating a message created by a service running older code than when this
            // field was introduced
            maybe_max_l0_created_at: None,
            max_l0_created_at: 8,
            source: 0,
        };
        let params2 =
            deserialize_parquet_file_params(old_proto_l2_max_l0_and_created_at_equal).unwrap();
        let protobuf = serialize_parquet_file_params(&params2, created_at);
        assert_eq!(
            protobuf,
            proto::ParquetFileParams {
                namespace_id: 1,
                table_id: 2,
                partition_id: 3,
                partition_hash_id: Some(
                    PartitionHashId::arbitrary_for_testing().as_bytes().to_vec()
                ),
                object_store_id: Some(object_store_id),
                min_time: 4,
                max_time: 5,
                file_size_bytes: 6,
                row_count: 7,
                compaction_level: CompactionLevel::Final as i32,
                created_at: 8,
                column_set: Some(proto::ColumnSet {
                    column_ids: vec![1],
                }),
                // Because we're now running newer code, this should now be set to Computed (even
                // though max_l0_created_at and created_at are 8; L2 files can only ever be
                // Computed)
                maybe_max_l0_created_at: Some(
                    proto::parquet_file_params::MaybeMaxL0CreatedAt::Computed(8)
                ),
                // `max_l0_created_at` should still be sent in case we're sending this to a service running older code
                max_l0_created_at: 8,
                source: 0,
            }
        );

        let old_proto_l0_max_l0_and_created_at_unequal = proto::ParquetFileParams {
            namespace_id: 1,
            table_id: 2,
            partition_id: 3,
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing().as_bytes().to_vec()),
            object_store_id: Some(object_store_id),
            min_time: 4,
            max_time: 5,
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Initial as i32,
            created_at: created_at.get(),
            column_set: Some(proto::ColumnSet {
                column_ids: vec![1],
            }),
            // This is simulating a message created by a service running older code than when this
            // field was introduced
            maybe_max_l0_created_at: None,
            max_l0_created_at: 2,
            source: 0,
        };
        let params2 =
            deserialize_parquet_file_params(old_proto_l0_max_l0_and_created_at_unequal).unwrap();
        let protobuf = serialize_parquet_file_params(&params2, created_at);
        assert_eq!(
            protobuf,
            proto::ParquetFileParams {
                namespace_id: 1,
                table_id: 2,
                partition_id: 3,
                partition_hash_id: Some(
                    PartitionHashId::arbitrary_for_testing().as_bytes().to_vec()
                ),
                object_store_id: Some(object_store_id),
                min_time: 4,
                max_time: 5,
                file_size_bytes: 6,
                row_count: 7,
                compaction_level: CompactionLevel::Initial as i32,
                created_at: 8,
                column_set: Some(proto::ColumnSet {
                    column_ids: vec![1],
                }),
                // Because we're now running newer code, this should now be set to Computed (even
                // though the compaction level is L0; the compactor can create L0s with computed
                // max_l0_created_ats but they'll be unequal because the compactor created file
                // will always be created later
                maybe_max_l0_created_at: Some(
                    proto::parquet_file_params::MaybeMaxL0CreatedAt::Computed(2)
                ),
                // `max_l0_created_at` should still be sent in case we're sending this to a service
                // running older code
                max_l0_created_at: 2,
                source: 0,
            }
        );
    }

    #[test]
    fn test_parquet_file_roundtrip() {
        let file = ParquetFile {
            id: ParquetFileId::new(12),
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(2),
            partition_id: PartitionId::new(3),
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing()),
            object_store_id: ObjectStoreId::from_uuid(Uuid::from_u128(1337)),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(5),
            to_delete: Some(Timestamp::new(13)),
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Final,
            created_at: Timestamp::new(8),
            column_set: ColumnSet::new([ColumnId::new(9), ColumnId::new(10)]),
            max_l0_created_at: Timestamp::new(11),
            source: None,
        };
        let protobuf = serialize_parquet_file(file.clone());
        let file2 = deserialize_parquet_file(protobuf).unwrap();
        assert_eq!(file, file2);
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let t = iox_time::Time::from_timestamp_nanos(0);
        let protobuf = serialize_timestamp(t);
        let t2 = deserialize_timestamp(protobuf).unwrap();
        assert_eq!(t, t2);
    }
}
