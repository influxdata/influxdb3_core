use std::sync::{Arc, LazyLock};

use chrono::Utc;
use data_types::{
    ColumnId, ColumnSet, CompactionLevel, ObjectStoreId, ParquetFile, ParquetFileParams, Timestamp,
};
use iox_catalog::{
    interface::{Catalog, ParquetFileRepoExt},
    mem::MemCatalog,
    test_helpers::{arbitrary_namespace_with_retention_policy, arbitrary_table},
};
use iox_time::SystemProvider;
use object_store::{path::Path, ObjectMeta};

pub(crate) static ARBITRARY_BAD_OBJECT_META: LazyLock<ObjectMeta> = LazyLock::new(|| ObjectMeta {
    location: Path::from(""),
    last_modified: Utc::now(),
    size: 0,
    e_tag: None,
    version: None,
});

pub(crate) async fn create_catalog_and_file_with_max_time(
    ts: Timestamp,
) -> (Arc<dyn Catalog>, ParquetFile) {
    let metric_registry = Arc::new(metric::Registry::new());
    let time_provider = Arc::new(SystemProvider::new());
    let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry), time_provider));
    create_schema_and_file_with_max_time(catalog, ts).await
}

pub(crate) async fn create_schema_and_file_with_max_time(
    catalog: Arc<dyn Catalog>,
    ts: Timestamp,
) -> (Arc<dyn Catalog>, ParquetFile) {
    let mut repos = catalog.repositories();
    let namespace =
        arbitrary_namespace_with_retention_policy(&mut *repos, "namespace_parquet_file_test", 60)
            .await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
    let partition = repos
        .partitions()
        .create_or_get("one".into(), table.id)
        .await
        .unwrap();

    let parquet_file_params = ParquetFileParams {
        namespace_id: namespace.id,
        table_id: partition.table_id,
        partition_id: partition.id,
        partition_hash_id: partition.hash_id().cloned(),
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(1),
        max_time: ts,
        file_size_bytes: 1337,
        row_count: 0,
        compaction_level: CompactionLevel::Initial,
        created_at: Timestamp::new(1),
        column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        max_l0_created_at: Timestamp::new(1),
        source: None,
    };

    let parquet_file = repos
        .parquet_files()
        .create(parquet_file_params)
        .await
        .unwrap();

    (catalog, parquet_file)
}
