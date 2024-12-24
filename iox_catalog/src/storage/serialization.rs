use data_types::{
    partition_template::NamespacePartitionTemplateOverride, NamespaceId, NamespaceWithStorage,
    TableId, TableWithStorage,
};
use generated_types::influxdata::iox::catalog_storage::v1 as proto;

use super::sorting::{NamespaceSortField, SortDirection};
use crate::util_serialization::{ContextExt, ConvertExt, ConvertOptExt, Error};

pub(crate) fn serialize_namespace_with_storage(
    n: NamespaceWithStorage,
) -> proto::NamespaceWithStorage {
    proto::NamespaceWithStorage {
        id: n.id.get(),
        name: n.name,
        retention_period_ns: n.retention_period_ns,
        max_tables: n.max_tables.get_i32(),
        max_columns_per_table: n.max_columns_per_table.get_i32(),
        partition_template: n.partition_template.as_proto().cloned(),
        size_bytes: n.size_bytes,
        table_count: n.table_count as i32,
    }
}

#[allow(dead_code)]
pub(crate) fn deserialize_namespace_with_storage(
    n: proto::NamespaceWithStorage,
) -> Result<NamespaceWithStorage, Error> {
    Ok(NamespaceWithStorage {
        id: NamespaceId::new(n.id),
        name: n.name,
        retention_period_ns: n.retention_period_ns,
        max_tables: n.max_tables.convert().ctx("max_tables")?,
        max_columns_per_table: n
            .max_columns_per_table
            .convert()
            .ctx("max_columns_per_table")?,
        partition_template: n
            .partition_template
            .convert_opt()
            .ctx("partition_template")?
            .unwrap_or_else(NamespacePartitionTemplateOverride::const_default),
        size_bytes: n.size_bytes,
        table_count: n.table_count as i64,
    })
}

pub(crate) fn serialize_table_with_storage(t: TableWithStorage) -> proto::TableWithStorage {
    proto::TableWithStorage {
        id: t.id.get(),
        name: t.name,
        namespace_id: t.namespace_id.get(),
        partition_template: t.partition_template.as_proto().cloned(),
        size_bytes: t.size_bytes,
    }
}

#[allow(dead_code)]
pub(crate) fn deserialize_table_with_storage(
    t: proto::TableWithStorage,
) -> Result<TableWithStorage, Error> {
    Ok(TableWithStorage {
        id: TableId::new(t.id),
        name: t.name,
        namespace_id: NamespaceId::new(t.namespace_id),
        partition_template: t.partition_template.convert().ctx("partition_template")?,
        size_bytes: t.size_bytes,
    })
}

#[allow(dead_code)]
pub(crate) fn serialize_namespace_sort_field(sort_field: NamespaceSortField) -> i32 {
    let sort_field = match sort_field {
        NamespaceSortField::Unspecified => proto::NamespaceSortField::Unspecified, // Do not sort if unspecified
        NamespaceSortField::Id => proto::NamespaceSortField::Id,
        NamespaceSortField::Name => proto::NamespaceSortField::Name,
        NamespaceSortField::RetentionPeriod => proto::NamespaceSortField::RetentionPeriod,
        NamespaceSortField::Storage => proto::NamespaceSortField::Storage,
        NamespaceSortField::TableCount => proto::NamespaceSortField::TableCount,
    };
    sort_field.into()
}

pub(crate) fn deserialize_namespace_sort_field(
    sort_field: i32,
) -> Result<NamespaceSortField, Error> {
    let sort_field: proto::NamespaceSortField = sort_field.convert().ctx("namespace sort field")?;
    let sort_field = match sort_field {
        proto::NamespaceSortField::Unspecified => NamespaceSortField::Unspecified,
        proto::NamespaceSortField::Id => NamespaceSortField::Id,
        proto::NamespaceSortField::Name => NamespaceSortField::Name,
        proto::NamespaceSortField::RetentionPeriod => NamespaceSortField::RetentionPeriod,
        proto::NamespaceSortField::Storage => NamespaceSortField::Storage,
        proto::NamespaceSortField::TableCount => NamespaceSortField::TableCount,
    };
    Ok(sort_field)
}

#[allow(dead_code)]
pub(crate) fn serialize_sort_direction(sort_direction: SortDirection) -> i32 {
    let sort_direction = match sort_direction {
        SortDirection::Asc => proto::SortDirection::Asc,
        SortDirection::Desc => proto::SortDirection::Desc,
    };
    sort_direction.into()
}

pub(crate) fn deserialize_sort_direction(sort_direction: i32) -> Result<SortDirection, Error> {
    let sort_direction: proto::SortDirection = sort_direction.convert().ctx("sort direction")?;
    let sort_direction = match sort_direction {
        proto::SortDirection::Unspecified => SortDirection::Asc, // Default to ascending
        proto::SortDirection::Asc => SortDirection::Asc,
        proto::SortDirection::Desc => SortDirection::Desc,
    };
    Ok(sort_direction)
}

#[cfg(test)]
mod tests {
    use data_types::{
        partition_template::NamespacePartitionTemplateOverride, NamespaceId, NamespaceWithStorage,
    };
    use generated_types::influxdata::iox::partition_template::v1::{
        template_part::Part, PartitionTemplate, TemplatePart,
    };

    use super::{
        deserialize_namespace_sort_field, deserialize_sort_direction,
        serialize_namespace_sort_field, serialize_sort_direction, NamespaceSortField,
        SortDirection,
    };

    #[test]
    fn test_namespace_with_storage_roundtrip() {
        use super::{deserialize_namespace_with_storage, serialize_namespace_with_storage};

        let namespace_with_storage = NamespaceWithStorage {
            id: NamespaceId::new(1),
            name: "namespace1".to_owned(),
            retention_period_ns: Some(2),
            max_tables: 3.try_into().unwrap(),
            max_columns_per_table: 4.try_into().unwrap(),
            partition_template: NamespacePartitionTemplateOverride::try_from(PartitionTemplate {
                parts: vec![TemplatePart {
                    part: Some(Part::TimeFormat("year-%Y".into())),
                }],
            })
            .unwrap(),
            size_bytes: 6,
            table_count: 7,
        };
        let protobuf = serialize_namespace_with_storage(namespace_with_storage.clone());
        let namespace_with_storage2 = deserialize_namespace_with_storage(protobuf).unwrap();
        assert_eq!(namespace_with_storage, namespace_with_storage2);
    }

    #[test]
    fn test_table_with_stroage_roundtrip() {
        use data_types::{
            partition_template::TablePartitionTemplateOverride, TableId, TableWithStorage,
        };

        use super::{deserialize_table_with_storage, serialize_table_with_storage};

        let table_with_storage = TableWithStorage {
            id: TableId::new(1),
            name: "table1".to_owned(),
            namespace_id: NamespaceId::new(2),
            partition_template: TablePartitionTemplateOverride::try_from_existing(
                Some(PartitionTemplate {
                    parts: vec![TemplatePart {
                        part: Some(Part::TagValue("bananas".into())),
                    }],
                }),
                &NamespacePartitionTemplateOverride::const_default(),
            )
            .unwrap(),
            size_bytes: 10,
        };
        let protobuf = serialize_table_with_storage(table_with_storage.clone());
        let table_with_storage2 = deserialize_table_with_storage(protobuf).unwrap();
        assert_eq!(table_with_storage, table_with_storage2);
    }

    #[test]
    fn test_namespace_sort_field_roundtrip() {
        assert_namespace_sort_field_roundtrip(NamespaceSortField::Id);
        assert_namespace_sort_field_roundtrip(NamespaceSortField::Name);
        assert_namespace_sort_field_roundtrip(NamespaceSortField::RetentionPeriod);
        assert_namespace_sort_field_roundtrip(NamespaceSortField::Storage);
        assert_namespace_sort_field_roundtrip(NamespaceSortField::TableCount);
    }

    #[track_caller]
    fn assert_namespace_sort_field_roundtrip(sort_field: NamespaceSortField) {
        let protobuf = serialize_namespace_sort_field(sort_field);
        let sort_field_2 = deserialize_namespace_sort_field(protobuf).unwrap();
        assert_eq!(sort_field, sort_field_2);
    }

    #[test]
    fn test_sort_direction_roundtrip() {
        assert_sort_direction_roundtrip(SortDirection::Asc);
        assert_sort_direction_roundtrip(SortDirection::Desc);
    }

    #[track_caller]
    fn assert_sort_direction_roundtrip(sort_direction: SortDirection) {
        let protobuf = serialize_sort_direction(sort_direction);
        let sort_direction_2 = deserialize_sort_direction(protobuf).unwrap();
        assert_eq!(sort_direction, sort_direction_2);
    }
}
