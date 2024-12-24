use data_types::NamespaceWithStorage;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NamespaceSortField {
    Unspecified = 0,
    Id = 1,
    Name = 2,
    RetentionPeriod = 3,
    Storage = 4,
    TableCount = 5,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SortDirection {
    Asc = 1,
    Desc = 2,
}

pub(crate) fn sort_namespaces(
    namespaces: &mut [NamespaceWithStorage],
    sort_field: NamespaceSortField,
    sort_direction: SortDirection,
) {
    match sort_field {
        NamespaceSortField::Unspecified => {
            // Do not sort
        }
        NamespaceSortField::Id => namespaces.sort_by(|a, b| {
            if sort_direction == SortDirection::Asc {
                a.id.cmp(&b.id)
            } else {
                b.id.cmp(&a.id)
            }
        }),
        NamespaceSortField::Name => namespaces.sort_by(|a, b| {
            if sort_direction == SortDirection::Asc {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        NamespaceSortField::RetentionPeriod => namespaces.sort_by(|a, b| {
            if sort_direction == SortDirection::Asc {
                a.retention_period_ns.cmp(&b.retention_period_ns)
            } else {
                b.retention_period_ns.cmp(&a.retention_period_ns)
            }
        }),
        NamespaceSortField::Storage => namespaces.sort_by(|a, b| {
            if sort_direction == SortDirection::Asc {
                a.size_bytes.cmp(&b.size_bytes)
            } else {
                b.size_bytes.cmp(&a.size_bytes)
            }
        }),
        NamespaceSortField::TableCount => namespaces.sort_by(|a, b| {
            if sort_direction == SortDirection::Asc {
                a.table_count.cmp(&b.table_count)
            } else {
                b.table_count.cmp(&a.table_count)
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use data_types::NamespaceId;

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
            },
        ];

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Unspecified, SortDirection::Asc);
        // unspecified sort field should not change the order
        assert_eq!(n, namespaces);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Id, SortDirection::Asc);
        assert_eq!(n[0].id.get(), 1);
        assert_eq!(n[1].id.get(), 2);
        assert_eq!(n[2].id.get(), 3);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Id, SortDirection::Desc);
        assert_eq!(n[0].id.get(), 3);
        assert_eq!(n[1].id.get(), 2);
        assert_eq!(n[2].id.get(), 1);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Name, SortDirection::Asc);
        assert_eq!(n[0].name, "a");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "c");

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Name, SortDirection::Desc);
        assert_eq!(n[0].name, "c");
        assert_eq!(n[1].name, "b");
        assert_eq!(n[2].name, "a");

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            NamespaceSortField::RetentionPeriod,
            SortDirection::Asc,
        );
        assert_eq!(n[0].retention_period_ns, Some(1));
        assert_eq!(n[1].retention_period_ns, Some(2));
        assert_eq!(n[2].retention_period_ns, Some(3));

        let mut n = namespaces.clone();
        sort_namespaces(
            &mut n,
            NamespaceSortField::RetentionPeriod,
            SortDirection::Desc,
        );
        assert_eq!(n[0].retention_period_ns, Some(3));
        assert_eq!(n[1].retention_period_ns, Some(2));
        assert_eq!(n[2].retention_period_ns, Some(1));

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Storage, SortDirection::Asc);
        assert_eq!(n[0].size_bytes, 1);
        assert_eq!(n[1].size_bytes, 2);
        assert_eq!(n[2].size_bytes, 3);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::Storage, SortDirection::Desc);
        assert_eq!(n[0].size_bytes, 3);
        assert_eq!(n[1].size_bytes, 2);
        assert_eq!(n[2].size_bytes, 1);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::TableCount, SortDirection::Asc);
        assert_eq!(n[0].table_count, 1);
        assert_eq!(n[1].table_count, 2);
        assert_eq!(n[2].table_count, 3);

        let mut n = namespaces.clone();
        sort_namespaces(&mut n, NamespaceSortField::TableCount, SortDirection::Desc);
        assert_eq!(n[0].table_count, 3);
        assert_eq!(n[1].table_count, 2);
        assert_eq!(n[2].table_count, 1);
    }
}
