//! gRPC catalog tunnel.
//!
//! This tunnels catalog requests over gRPC.

pub mod client;
mod serialization;
pub mod server;
pub mod test_server;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{MaxColumnsPerTable, MaxTables, Namespace, NamespaceName, NamespaceVersion};
    use generated_types::influxdata::iox::catalog::v2 as proto;
    use iox_time::{SystemProvider, TimeProvider};
    use metric::{Attributes, Metric, U64Counter};
    use serialization::{deserialize_namespace, RequiredExt};
    use test_helpers::{maybe_start_logging, timeout::FutureTimeout};

    use crate::{
        grpc::test_server::TestGrpcServer, interface::Catalog, mem::MemCatalog,
        test_helpers::TestCatalog,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_catalog() {
        maybe_start_logging();

        crate::interface_tests::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let time_provider = Arc::new(SystemProvider::new()) as _;
            let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
            let test_server = TestGrpcServer::new(backing_catalog).await;
            let uri = test_server.uri();

            // create new metrics for client so that they don't overlap w/ server
            let metrics = Arc::new(metric::Registry::default());
            let client =
                client::GrpcCatalogClient::builder(vec![uri], metrics, Arc::clone(&time_provider))
                    .build();

            let test_catalog = TestCatalog::new(client);
            test_catalog.hold_onto(test_server);

            Arc::new(test_catalog) as _
        })
        .await;
    }

    #[tokio::test]
    async fn test_catalog_metrics() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        // create new metrics for client so that they don't overlap w/ server
        let metrics = Arc::new(metric::Registry::default());
        let client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );

        let ns = client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("testns").unwrap(), None, None, None)
            .await
            .expect("namespace failed to create");

        let _ = client
            .repositories()
            .tables()
            .list_by_namespace_id(ns.id)
            .await
            .expect("failed to list namespaces");

        let metric = metrics
            .get_instrument::<Metric<U64Counter>>("grpc_client_requests")
            .expect("failed to get metric");

        let count = metric
            .get_observer(&Attributes::from(&[
                (
                    "path",
                    "/influxdata.iox.catalog.v2.CatalogService/NamespaceCreate",
                ),
                ("status", "ok"),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(count, 1);

        let count = metric
            .get_observer(&Attributes::from(&[
                (
                    "path",
                    "/influxdata.iox.catalog.v2.CatalogService/TableListByNamespaceId",
                ),
                ("status", "ok"),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_catalog_get_time() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        // create new metrics for client so that they don't overlap w/ server
        let metrics = Arc::new(metric::Registry::default());
        let client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );

        let system_now = client.time_provider().now();
        let server_now = client.get_time().await;

        assert_matches!(server_now, Ok(t) => {
            assert!(t.second().abs_diff(system_now.second()) <= 1);
        });
    }

    #[tokio::test]
    async fn test_catalog_soft_delete_accepts_id() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        let metrics = Arc::new(metric::Registry::default());
        // Create a namespace, then send a delete request containing its ID.
        //
        // The delete must be serviced and resolve the name.

        let catalog_client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );
        let created_ns = catalog_client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("bananas").unwrap(), None, None, None)
            .await
            .expect("namespace must be created");

        // Create a raw client, then send a delete request which contains a
        // namespace ID and not a name.
        let mut grpc_client =
            proto::catalog_service_client::CatalogServiceClient::connect(test_server.uri())
                .with_timeout_panic(Duration::from_millis(500))
                .await
                .expect("failed to connect client");

        let deleted_id = grpc_client
            .namespace_soft_delete(proto::NamespaceSoftDeleteRequest {
                target: Some(proto::namespace_soft_delete_request::Target::Id(
                    created_ns.id.get(),
                )),
            })
            .await
            .expect("failed to delete")
            .into_inner()
            .namespace_id;

        assert_eq!(deleted_id, created_ns.id.get());

        // The only deleted namespace must be the one just created.
        let deleted_ns = catalog_client
            .repositories()
            .namespaces()
            .list(crate::interface::SoftDeletedRows::OnlyDeleted)
            .await
            .expect("list failed");
        assert_matches!(deleted_ns.as_slice(), [ns] if ns.id == created_ns.id);
    }

    #[tokio::test]
    async fn test_catalog_update_namespace_retention_accepts_id() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        let metrics = Arc::new(metric::Registry::default());

        // Create a namespace, then send an update retention period containing its ID.
        //
        // The update must be serviced and resolve the name.
        let catalog_client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );
        let created_ns = catalog_client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("bananas").unwrap(), None, None, None)
            .await
            .expect("namespace must be created");

        // Create a raw client, then send an update request which contains a
        // namespace ID and not a name.
        let mut grpc_client =
            proto::catalog_service_client::CatalogServiceClient::connect(test_server.uri())
                .with_timeout_panic(Duration::from_millis(500))
                .await
                .unwrap();

        let updated_ns = grpc_client
            .namespace_update_retention_period(proto::NamespaceUpdateRetentionPeriodRequest {
                target: Some(
                    proto::namespace_update_retention_period_request::Target::Id(
                        created_ns.id.get(),
                    ),
                ),
                retention_period_ns: Some(42),
            })
            .await
            .expect("failed to update namespace retention")
            .into_inner()
            .namespace
            .required()
            .and_then(deserialize_namespace)
            .unwrap();

        // The returned namespace must be the same, bar the retention period and
        // incremented version.
        assert_eq!(
            updated_ns,
            Namespace {
                retention_period_ns: Some(42),
                router_version: NamespaceVersion::new(created_ns.router_version.get() + 1),
                ..created_ns
            }
        );
    }

    #[tokio::test]
    async fn test_catalog_update_namespace_table_limit_accepts_id() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        let metrics = Arc::new(metric::Registry::default());

        // Create a namespace, then send a table limit update request containing
        // its ID.
        // The update must be serviced and resolve the name.
        let catalog_client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );
        let created_ns = catalog_client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("bananas").unwrap(), None, None, None)
            .await
            .expect("namespace must be created");

        // Create a raw client, then send an update request which contains a
        // namespace ID and not a name.
        let mut grpc_client =
            proto::catalog_service_client::CatalogServiceClient::connect(test_server.uri())
                .with_timeout_panic(Duration::from_millis(500))
                .await
                .unwrap();

        let updated_ns = grpc_client
            .namespace_update_table_limit(proto::NamespaceUpdateTableLimitRequest {
                target: Some(proto::namespace_update_table_limit_request::Target::Id(
                    created_ns.id.get(),
                )),
                new_max: 42,
            })
            .await
            .expect("failed to update namespace retention")
            .into_inner()
            .namespace
            .required()
            .and_then(deserialize_namespace)
            .unwrap();

        // The returned namespace must be the same, bar the max tables and
        // incremented version.
        assert_eq!(
            updated_ns,
            Namespace {
                max_tables: MaxTables::try_from(42).expect("valid"),
                router_version: NamespaceVersion::new(created_ns.router_version.get() + 1),
                ..created_ns
            }
        );
    }

    #[tokio::test]
    async fn test_catalog_update_namespace_column_limit_accepts_id() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestGrpcServer::new(backing_catalog).await;
        let uri = test_server.uri();

        let metrics = Arc::new(metric::Registry::default());

        // Create a namespace, then send an column limit update containing its ID.
        //
        // The update must be serviced and resolve the name.
        let catalog_client = Arc::new(
            client::GrpcCatalogClient::builder(
                vec![uri],
                Arc::clone(&metrics),
                Arc::clone(&time_provider),
            )
            .build(),
        );
        let created_ns = catalog_client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("bananas").unwrap(), None, None, None)
            .await
            .expect("namespace must be created");

        // Create a raw client, then send an update request which contains a
        // namespace ID and not a name.
        let mut grpc_client =
            proto::catalog_service_client::CatalogServiceClient::connect(test_server.uri())
                .with_timeout_panic(Duration::from_millis(500))
                .await
                .unwrap();

        let updated_ns = grpc_client
            .namespace_update_column_limit(proto::NamespaceUpdateColumnLimitRequest {
                target: Some(proto::namespace_update_column_limit_request::Target::Id(
                    created_ns.id.get(),
                )),
                new_max: 42,
            })
            .await
            .expect("failed to update namespace retention")
            .into_inner()
            .namespace
            .required()
            .and_then(deserialize_namespace)
            .unwrap();

        // The returned namespace must be the same, bar the max tables and
        // incremented version.
        assert_eq!(
            updated_ns,
            Namespace {
                max_columns_per_table: MaxColumnsPerTable::try_from(42).expect("valid"),
                router_version: NamespaceVersion::new(created_ns.router_version.get() + 1),
                ..created_ns
            }
        );
    }
}
