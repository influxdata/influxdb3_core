use data_types::NamespaceWithStorage;

use crate::interface::{
    NamespaceSortField, NamespaceSorting, SortDirection, TableSortField, TableSorting,
};

/// Utility function to sort namespaces in place based on the provided sorting configuration.
pub(crate) fn sort_namespaces(
    namespaces: &mut [NamespaceWithStorage],
    sorting: Option<NamespaceSorting>,
) {
    if let Some(sorting) = sorting {
        match sorting.field {
            NamespaceSortField::Id => namespaces.sort_by(|a, b| {
                if sorting.direction == SortDirection::Ascending {
                    a.id.cmp(&b.id)
                } else {
                    b.id.cmp(&a.id)
                }
            }),
            NamespaceSortField::Name => namespaces.sort_by(|a, b| {
                if sorting.direction == SortDirection::Ascending {
                    a.name.cmp(&b.name)
                } else {
                    b.name.cmp(&a.name)
                }
            }),
            NamespaceSortField::RetentionPeriod => namespaces.sort_by(|a, b| {
                if sorting.direction == SortDirection::Ascending {
                    a.retention_period_ns.cmp(&b.retention_period_ns)
                } else {
                    b.retention_period_ns.cmp(&a.retention_period_ns)
                }
            }),
            NamespaceSortField::TableCount => namespaces.sort_by(|a, b| {
                if sorting.direction == SortDirection::Ascending {
                    a.table_count.cmp(&b.table_count)
                } else {
                    b.table_count.cmp(&a.table_count)
                }
            }),
        }
    }
}

/// Utility function to sort tables in place based on the provided sorting configuration.
pub(crate) fn sort_tables(tables: &mut [data_types::Table], sorting: Option<TableSorting>) {
    if let Some(sorting) = sorting {
        match sorting.field {
            TableSortField::Name => tables.sort_by(|a, b| {
                if sorting.direction == SortDirection::Ascending {
                    a.name.cmp(&b.name)
                } else {
                    b.name.cmp(&a.name)
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use data_types::{NamespaceId, Table, TableId};

    use super::*;

    #[test]
    fn test_sort_namespaces() {
        let namespaces = vec![
            NamespaceWithStorage {
                id: NamespaceId::new(3),
                name: "c".into(),
                retention_period_ns: Some(3),
                size_bytes: 3,
                table_count: 3,
                max_tables: Default::default(),
                max_columns_per_table: Default::default(),
                partition_template: Default::default(),
                deleted_at: None,
            },
            NamespaceWithStorage {
                id: NamespaceId::new(1),
                name: "a".into(),
                retention_period_ns: Some(1),
                size_bytes: 1,
                table_count: 1,
                max_tables: Default::default(),
                max_columns_per_table: Default::default(),
                partition_template: Default::default(),
                deleted_at: None,
            },
            NamespaceWithStorage {
                id: NamespaceId::new(2),
                name: "b".into(),
                retention_period_ns: Some(2),
                size_bytes: 2,
                table_count: 2,
                max_tables: Default::default(),
                max_columns_per_table: Default::default(),
                partition_template: Default::default(),
                deleted_at: None,
            },
        ];

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, None);
        // unspecified sort field should not change the order
        assert_eq!(n, namespaces);

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::Id,
                direction: SortDirection::Ascending,
            }),
        );
        assert_eq!(n[0].id.get(), 1);
        assert_eq!(n[1].id.get(), 2);
        assert_eq!(n[2].id.get(), 3);

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::Id,
                direction: SortDirection::Descending,
            }),
        );
        assert_eq!(n[0].id.get(), 3);
        assert_eq!(n[1].id.get(), 2);
        assert_eq!(n[2].id.get(), 1);

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::Name,
                direction: SortDirection::Ascending,
            }),
        );
        assert_eq!(n[0].name, "a");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "c");

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::Name,
                direction: SortDirection::Descending,
            }),
        );
        assert_eq!(n[0].name, "c");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "a");

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::RetentionPeriod,
                direction: SortDirection::Ascending,
            }),
        );
        assert_eq!(n[0].retention_period_ns, Some(1));
        assert_eq!(n[1].retention_period_ns, Some(2));
        assert_eq!(n[2].retention_period_ns, Some(3));

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::RetentionPeriod,
                direction: SortDirection::Descending,
            }),
        );
        assert_eq!(n[0].retention_period_ns, Some(3));
        assert_eq!(n[1].retention_period_ns, Some(2));
        assert_eq!(n[2].retention_period_ns, Some(1));

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::TableCount,
                direction: SortDirection::Ascending,
            }),
        );
        assert_eq!(n[0].table_count, 1);
        assert_eq!(n[1].table_count, 2);
        assert_eq!(n[2].table_count, 3);

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            Some(NamespaceSorting {
                field: NamespaceSortField::TableCount,
                direction: SortDirection::Descending,
            }),
        );
        assert_eq!(n[0].table_count, 3);
        assert_eq!(n[1].table_count, 2);
        assert_eq!(n[2].table_count, 1);
    }

    #[test]
    fn test_sort_tables() {
        let tables = vec![
            Table {
                id: TableId::new(3),
                namespace_id: NamespaceId::new(0),
                name: "c".into(),
                partition_template: Default::default(),
                deleted_at: None,
                iceberg_enabled: false,
            },
            Table {
                id: TableId::new(2),
                namespace_id: NamespaceId::new(0),
                name: "b".into(),
                partition_template: Default::default(),
                deleted_at: None,
                iceberg_enabled: false,
            },
            Table {
                id: TableId::new(1),
                namespace_id: NamespaceId::new(0),
                name: "a".into(),
                partition_template: Default::default(),
                deleted_at: None,
                iceberg_enabled: false,
            },
        ];

        let mut tables = tables.clone();
        sort_tables(&mut tables, None);
        // unspecified sort field should not change the order
        assert_eq!(tables, tables);

        let mut n = tables.clone();
        sort_tables(
            &mut n,
            Some(TableSorting {
                field: TableSortField::Name,
                direction: SortDirection::Ascending,
            }),
        );
        assert_eq!(n[0].name, "a");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "c");

        let mut n = tables.clone();
        sort_tables(
            &mut n,
            Some(TableSorting {
                field: TableSortField::Name,
                direction: SortDirection::Descending,
            }),
        );
        assert_eq!(n[0].name, "c");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "a");
    }
}
