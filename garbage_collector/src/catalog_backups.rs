#![allow(dead_code)] // This code is not called anywhere yet but will be 🔜

use data_types::{NamespaceId, NamespaceName, TableId};
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use std::{collections::HashMap, sync::Arc};

async fn get_namespace_name_map(
    catalog: &Arc<dyn Catalog>,
) -> Result<HashMap<NamespaceId, NamespaceName<'_>>, iox_catalog::interface::Error> {
    let namespaces = catalog
        .repositories()
        .namespaces()
        .list(SoftDeletedRows::AllRows)
        .await?;

    Ok(namespaces
        .into_iter()
        .map(|namespace| {
            (
                namespace.id,
                namespace
                    .name
                    .try_into()
                    .expect("namespace names from the catalog should be valid"),
            )
        })
        .collect())
}

async fn get_table_name_map(
    catalog: &Arc<dyn Catalog>,
) -> Result<HashMap<TableId, String>, iox_catalog::interface::Error> {
    let tables = catalog.repositories().tables().list().await?;

    Ok(tables
        .into_iter()
        .map(|table| (table.id, table.name))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod name_map_lookups {
        use super::*;
        use iox_catalog::{mem::MemCatalog, test_helpers::*};
        use iox_time::SystemProvider;

        #[tokio::test]
        async fn namespace_id_name_maps() {
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
                Default::default(),
                Arc::new(SystemProvider::new()),
            ));
            let mut repos = catalog.repositories();

            // Add a namespace to the catalog and mark it as deleted, to ensure we can get names
            // for soft-deleted namespaces
            arbitrary_namespace(&mut *repos, "test_namespace").await;
            let soft_deleted_id = repos
                .namespaces()
                .soft_delete("test_namespace")
                .await
                .unwrap();
            // Add an active namespace
            let another = arbitrary_namespace(&mut *repos, "not_deleted_namespace").await;

            let namespace_name_map = get_namespace_name_map(&catalog).await.unwrap();
            assert_eq!(namespace_name_map.len(), 2);

            assert_eq!(
                namespace_name_map.get(&soft_deleted_id).unwrap().as_str(),
                "test_namespace"
            );
            assert_eq!(
                namespace_name_map.get(&another.id).unwrap().as_str(),
                "not_deleted_namespace"
            );
        }

        #[tokio::test]
        async fn table_id_name_maps() {
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(
                Default::default(),
                Arc::new(SystemProvider::new()),
            ));
            let mut repos = catalog.repositories();

            let namespace1 = arbitrary_namespace(&mut *repos, "namespace1").await;
            let namespace2 = arbitrary_namespace(&mut *repos, "namespace2").await;

            // Create 2 tables in namespace 1 with diferent names
            let table1_1 = arbitrary_table(&mut *repos, "test_table1", &namespace1).await;
            let table1_2 = arbitrary_table(&mut *repos, "test_table2", &namespace1).await;

            // Create 1 table in namespace 2 with the same name as one of namespace 1's tables
            let table2_1 = arbitrary_table(&mut *repos, "test_table1", &namespace2).await;

            let table_name_map = get_table_name_map(&catalog).await.unwrap();
            assert_eq!(table_name_map.len(), 3);

            assert_eq!(
                table_name_map.get(&table1_1.id).unwrap().as_str(),
                "test_table1"
            );
            assert_eq!(
                table_name_map.get(&table1_2.id).unwrap().as_str(),
                "test_table2"
            );
            assert_eq!(
                table_name_map.get(&table2_1.id).unwrap().as_str(),
                "test_table1"
            );
        }
    }
}
