use std::{
    ops::{ControlFlow, Sub},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};

use backoff::{Backoff, BackoffConfig};
use chrono::{DateTime, Utc};
use data_types::{
    partition_template::{build_column_values, ColumnValue, TablePartitionTemplateOverride},
    snapshot::table::TableSnapshotPartition,
    ParquetFile, ParquetFileParams, PartitionId, Table, TableId,
};
use futures::{future::join_all, ready, stream, Future, FutureExt, StreamExt, TryStreamExt};
use http::{Response, StatusCode};
use hyper::Body;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::{error, info};
use parquet_file::ParquetFilePath;
use schema::TIME_COLUMN_NAME;
use tower::{Layer, Service};

use crate::{
    data_types::{PolicyConfig, Request, WriteHintAck, WriteHintRequestBody},
    ParquetCacheServerConfig,
};

use super::{error::Error, response::PinnedFuture};

pub type FinalResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<Body>, super::error::Error>> + Send>>;

/// WarmingError is a local error type.
/// Tower service layers are expected to convert to [`Error`].
#[derive(Debug, thiserror::Error)]
pub enum WarmingError {
    #[error("Service unavailable")]
    ServiceNotReady,
    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),
    #[error("Catalog metadata error: {0}")]
    CatalogMetadata(String),
    #[error("Write-back error: {0}")]
    WriteBack(#[from] Box<Error>),
    #[error("Failed to finalize to running state")]
    Finalize,
}

/// Cache Service
#[derive(Debug, Clone)]
pub struct CacheService<S: Clone> {
    catalog: Arc<dyn Catalog>,
    cache_policy: PolicyConfig,
    inner: S,
}

impl<S> CacheService<S>
where
    S: Service<Request, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    pub fn new(inner: S, catalog: Arc<dyn Catalog>, config: ParquetCacheServerConfig) -> Self {
        let ParquetCacheServerConfig {
            policy_config: cache_policy,
            prewarming_table_concurrency,
            ..
        } = config;

        let cache_service = Self {
            catalog,
            cache_policy,
            inner,
        };

        let mut cache_service_captured = cache_service.clone();
        tokio::spawn(async move {
            if let Err(error) =
                match futures::future::poll_fn(|cx| cache_service_captured.inner.poll_ready(cx))
                    .await
                    .map_err(|_| WarmingError::ServiceNotReady)
                {
                    Ok(_) => {
                        cache_service_captured
                            .prewarm(prewarming_table_concurrency)
                            .await
                    }
                    Err(e) => Err(e),
                }
            {
                error!(%error, "prewarming failure");
            }
        });

        cache_service
    }

    async fn get_partitions_per_table(
        &self,
        table_id: TableId,
        now: DateTime<Utc>,
    ) -> Result<Vec<PartitionId>, WarmingError> {
        // query for each table's partition template from the snapshot, not on the table itself
        let catalog = Arc::clone(&self.catalog);
        let mut fn_mut = || async {
            catalog
                .repositories()
                .tables()
                .snapshot(table_id)
                .await
                .map_err(WarmingError::Catalog)
        };
        let snapshot = perform_catalog_query_with_retries(&mut fn_mut).await?;

        // prune partitions based on cache policy
        let partitions = snapshot.partitions().filter_map(Result::ok);
        let template = snapshot
            .table()
            .map(|t| t.partition_template)
            .map_err(|e| WarmingError::CatalogMetadata(e.to_string()))?;

        Ok(partitions
            .filter_map(
                |p| match within_policy(&self.cache_policy, now, &template, p) {
                    Err(error) => {
                        error!(%error, "failured to check parquet cache policy against partition");
                        None
                    }
                    Ok(r) => r,
                },
            )
            .collect::<Vec<_>>())
    }

    async fn get_parquet_files(
        &mut self,
        prewarming_table_concurrency: usize,
    ) -> Result<Vec<ParquetFile>, WarmingError> {
        let now = Utc::now();

        // query for list of table_ids
        let catalog = Arc::clone(&self.catalog);
        let mut fn_mut = || async {
            catalog
                .repositories()
                .tables()
                .list()
                .await
                .map(|tables| {
                    tables
                        .iter()
                        .map(|Table { id, .. }| *id)
                        .collect::<Vec<_>>()
                })
                .map_err(WarmingError::Catalog)
        };
        let tables = perform_catalog_query_with_retries(&mut fn_mut).await?;

        let partitions_to_fetch = stream::iter(tables).map(|table_id| {
            let this = self.clone();
            async move {
                let partitions = match this.get_partitions_per_table(table_id, now).await {
                    Ok(p) => p,
                    Err(error) => {
                        error!(%error, %table_id, "prewarming failed to get partitions for table");
                        vec![]
                    },
                };
                futures::stream::iter(partitions.into_iter().map(Ok::<PartitionId, WarmingError>))
            }
        })
        .buffer_unordered(prewarming_table_concurrency)
        .flatten()
        .try_collect::<Vec<PartitionId>>().await?;

        // query (fetch) the parquet files
        let catalog = Arc::clone(&self.catalog);
        let mut fn_mut = || async {
            catalog
                .repositories()
                .parquet_files()
                .list_by_partition_not_to_delete_batch(partitions_to_fetch.clone())
                .await
                .map_err(WarmingError::Catalog)
        };
        perform_catalog_query_with_retries(&mut fn_mut).await
    }

    pub async fn prewarm(
        &mut self,
        prewarming_table_concurrency: usize,
    ) -> Result<(), WarmingError> {
        // 0. (already done): LruCacheManager::new() => should have cache policy.
        // 1. (already done): Keyspace::poll_ready() => should have the keyspace.
        // 2. TODO(optional): may have persisted state from previous LruCacheManager, to reduce catalog load

        // 3. GET list of obj_keys from catalog.
        //      * Query limits based on cache policy.
        //      * Use slower prewarming, paginated catalog queries, prioritized cache insertion.
        let parquet_files = self.get_parquet_files(prewarming_table_concurrency).await?;
        let to_write = parquet_files.len() as u64;
        let written = AtomicU64::new(0);
        let logging_incr = to_write.checked_div(10).unwrap_or(100);
        info!("starting {} write-back attempts", to_write);

        // 4. for key in list => self.call(<`/write-hint` request for key>)
        //      * inner KeyspaceService will filter by key hash
        //      * inner DataService will filter by cache eviction policy
        //      * inner WriteHandler will handle write-back
        let write_backs = parquet_files
            .into_iter()
            .map(|parquet_file| {
                let params = ParquetFileParams::from(parquet_file);
                let location = ParquetFilePath::from(&params).to_string();

                let req = Request::WriteHint(WriteHintRequestBody {
                    location,
                    hint: (&params).into(),
                    ack_setting: WriteHintAck::Completed,
                });

                self.inner.call(req).map(|resp| {
                    let curr_cnt = written.load(Ordering::Relaxed);
                    if curr_cnt.checked_rem(logging_incr).is_none() {
                        info!(
                            "completed {} out of {} write-back attempts",
                            curr_cnt, to_write
                        );
                    }
                    written.fetch_add(1, Ordering::Relaxed);
                    resp
                })
            })
            .collect::<Vec<_>>();
        for resp in join_all(write_backs).await.into_iter() {
            match resp {
                Err(Error::Keyspace(_)) => continue, // Object filtered by keyspace. Is ok.
                Ok(_) => continue,
                Err(e) => return Err(WarmingError::WriteBack(Box::new(e))),
            }
        }

        // 5. message to inner that prewarming is done.
        self.inner
            .call(Request::Warmed)
            .await
            .map_err(|_| WarmingError::Finalize)?;

        Ok(())
    }
}

impl<S> Service<http::Request<Body>> for CacheService<S>
where
    S: Service<Request, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = Error;
    type Future = FinalResponseFuture;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // wait for inner service to receive requests
        let _ = ready!(self.inner.poll_ready(cx));

        // poll_ready is about ability to respond to `GET /state` requests.
        // return ok, as can provide state "warming"
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let req = Request::parse(req).await?;
            let resp = inner.call(req).await?;
            Response::builder()
                .status(resp.code())
                .body(resp.into())
                .or_else(|e| {
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(e.to_string().into())
                        .expect("should build error response"))
                })
        })
    }
}

fn within_policy(
    policy: &PolicyConfig,
    now: DateTime<Utc>,
    template: &TablePartitionTemplateOverride,
    partition: TableSnapshotPartition,
) -> Result<Option<PartitionId>, WarmingError> {
    let cutoff = now.sub(Duration::from_nanos(
        policy.event_recency_max_duration_nanoseconds,
    ));

    for (col, val) in build_column_values(
        template,
        std::str::from_utf8(partition.key()).expect("should be valid utf-8 partition key"),
    ) {
        match val {
            ColumnValue::Datetime { end, .. } if col.eq(TIME_COLUMN_NAME) => {
                if cutoff <= end {
                    return Ok(Some(partition.id()));
                }
                return Ok(None);
            }
            _ => {}
        }
    }

    Err(WarmingError::CatalogMetadata(format!(
        "cannot locate event time column for partition {:?}",
        partition
    )))
}

async fn perform_catalog_query_with_retries<F, F1, T>(func: &mut F) -> Result<T, WarmingError>
where
    F: (FnMut() -> F1) + Send,
    F1: std::future::Future<Output = Result<T, WarmingError>> + Send,
{
    Backoff::new(&BackoffConfig::default())
        .retry_with_backoff("get prewarming files from catalog", || {
            let fut = func();
            async move {
                let resp = fut.await;
                match resp {
                    // retry for only catalog errors
                    Err(WarmingError::Catalog(e)) => {
                        ControlFlow::<_, Error>::Continue(WarmingError::Catalog(e).into())
                    }
                    _ => ControlFlow::Break(resp),
                }
            }
        })
        .await
        .expect("retry forever")
}

pub struct BuildCacheService {
    catalog: Arc<dyn Catalog>,
    config: ParquetCacheServerConfig,
}

impl BuildCacheService {
    pub fn new(catalog: Arc<dyn Catalog>, config: ParquetCacheServerConfig) -> Self {
        Self { catalog, config }
    }
}

impl<S> Layer<S> for BuildCacheService
where
    S: Service<Request, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    type Service = CacheService<S>;

    fn layer(&self, service: S) -> Self::Service {
        CacheService::new(service, Arc::clone(&self.catalog), self.config.clone())
    }
}
