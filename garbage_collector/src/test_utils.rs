use std::sync::{Arc, LazyLock};

use data_types::{ParquetFile, ParquetFileParams, Timestamp};
use iox_catalog::{
    interface::{Catalog, ParquetFileRepoExt},
    mem::MemCatalog,
    test_helpers::{
        arbitrary_namespace_with_retention_policy, arbitrary_parquet_file_params, arbitrary_table,
    },
};
use iox_time::{SystemProvider, TimeProvider};
use object_store::{path::Path, ObjectMeta};

pub(crate) static ARBITRARY_BAD_OBJECT_META: LazyLock<ObjectMeta> = LazyLock::new(|| ObjectMeta {
    location: Path::from(""),
    last_modified: SystemProvider::new().now().date_time(),
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
    let parquet_file = create_schema_and_file_with_max_time(Arc::clone(&catalog) as _, ts).await;

    (catalog, parquet_file)
}

pub(crate) async fn create_schema_and_file_with_max_time(
    catalog: Arc<dyn Catalog>,
    max_time: Timestamp,
) -> ParquetFile {
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
        max_time,
        ..arbitrary_parquet_file_params(&namespace, &table, &partition)
    };
    repos
        .parquet_files()
        .create(parquet_file_params)
        .await
        .unwrap()
}
