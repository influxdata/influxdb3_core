//! The controller module contains the API and functionality
//! used to implement the controller for a DataCacheSet.

use kube::Client;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

mod error;
pub use error::{Error, Result};

mod kube_util;
mod parquet_cache;
pub use parquet_cache::{
    ParquetCache, ParquetCacheInstanceSet, ParquetCacheSpec, ParquetCacheStatus,
};

mod parquet_cache_controller;

mod parquet_cache_set;
pub use parquet_cache_set::{ParquetCacheSet, ParquetCacheSetSpec, ParquetCacheSetStatus};

mod parquet_cache_set_controller;

mod state_service;

/// The name of the controller.
const CONTROLLER_NAME: &str = "parquet-cache-set-controller";

/// Label used to annotate the objects with the hash of the pod template.
const POD_TEMPLATE_HASH_LABEL: &str = "pod-template-hash";

/// Label used to annotate objects with the count of parquet cache replicas.
const PARQUET_CACHE_REPLICAS_LABEL: &str = "parquet-cache-replicas";

/// The time to wait before re-executing when waiting for cache instances to warm, or cool.
const SHORT_WAIT: Duration = Duration::from_secs(60);

/// The time to wait before re-executing when there is no longer any active work to do, or
/// the controller will be awoken by changes to owned objects.
const LONG_WAIT: Duration = Duration::from_secs(3600);

/// Run the controller for reconciling ParquetCache objects.
pub async fn run_parquet_cache_controller(
    shutdown: CancellationToken,
    client: Client,
    ns: Option<String>,
) -> Result<(), kube::Error> {
    parquet_cache_controller::run_controller(async move { shutdown.cancelled().await }, client, ns)
        .await
}

/// Run the controller for reconciling ParquetCacheSet objects.
pub async fn run_parquet_cache_set_controller(
    shutdown: CancellationToken,
    client: Client,
    ns: Option<String>,
) -> Result<(), kube::Error> {
    parquet_cache_set_controller::run_controller(
        async move { shutdown.cancelled().await },
        client,
        ns,
    )
    .await
}
