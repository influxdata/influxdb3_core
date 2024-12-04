use data_types::{NamespaceId, TableId, TableWithStorage};
use generated_types::influxdata::iox::catalog_storage::v1 as proto;

use crate::util_serialization::{ContextExt, ConvertExt, Error};

#[allow(dead_code)]
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

mod tests {

    #[test]
    fn test_table_with_stroage_roundtrip() {
        use data_types::{
            partition_template::{
                NamespacePartitionTemplateOverride, TablePartitionTemplateOverride,
            },
            NamespaceId, TableId, TableWithStorage,
        };
        use generated_types::influxdata::iox::partition_template::v1::{
            template_part::Part, PartitionTemplate, TemplatePart,
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
}
