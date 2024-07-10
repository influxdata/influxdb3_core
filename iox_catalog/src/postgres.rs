//! A Postgres backed implementation of the Catalog

use crate::constants::MAX_PARTITION_SELECTED_ONCE_FOR_DELETE;
use crate::interface::{namespace_snapshot_by_name, PartitionRepoExt, RootRepo};
use crate::metrics::CatalogMetrics;
use crate::util::should_delete_partition;
use crate::{
    constants::{
        MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE, MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION,
        MAX_PARQUET_L0_FILES_PER_PARTITION,
    },
    interface::{
        AlreadyExistsSnafu, CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo,
        PartitionRepo, RepoCollection, Result, SoftDeletedRows, TableRepo,
    },
    migrate::IOxMigrator,
};
use async_trait::async_trait;
use data_types::snapshot::root::RootSnapshot;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, TemplatePart,
    },
    snapshot::{namespace::NamespaceSnapshot, partition::PartitionSnapshot, table::TableSnapshot},
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, NamespaceVersion, ObjectStoreId,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionHashId, PartitionId,
    PartitionKey, SkippedCompaction, SortKeyIds, Table, TableId, Timestamp,
};
use futures::StreamExt;
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, Instrument, MetricKind};
use observability_deps::tracing::{debug, info, warn};
use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockWriteGuard};
use snafu::prelude::*;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Acquire, ConnectOptions, Executor, Postgres, Row,
};
use sqlx_hotswap_pool::HotSwapPool;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    env,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinSet;
use trace::ctx::SpanContext;

static MIGRATOR: Lazy<IOxMigrator> =
    Lazy::new(|| IOxMigrator::try_from(&sqlx::migrate!()).expect("valid migration"));

/// Postgres connection options.
#[derive(Debug, Clone)]
pub struct PostgresConnectionOptions {
    /// Application name.
    ///
    /// This will be reported to postgres.
    pub app_name: String,

    /// Schema name.
    pub schema_name: String,

    /// DSN.
    pub dsn: String,

    /// Maximum number of concurrent connections.
    pub max_conns: u32,

    /// Set the amount of time to attempt connecting to the database.
    pub connect_timeout: Duration,

    /// Set a maximum idle duration for individual connections.
    pub idle_timeout: Duration,

    /// Set a maximum duration for individual statements
    pub statement_timeout: Duration,

    /// If the DSN points to a file (i.e. starts with `dsn-file://`), this sets the interval how often the the file
    /// should be polled for updates.
    ///
    /// If an update is encountered, the underlying connection pool will be hot-swapped.
    pub hotswap_poll_interval: Duration,
}

impl PostgresConnectionOptions {
    /// Default value for [`schema_name`](Self::schema_name).
    pub const DEFAULT_SCHEMA_NAME: &'static str = "iox_catalog";

    /// Default value for [`max_conns`](Self::max_conns).
    pub const DEFAULT_MAX_CONNS: u32 = 10;

    /// Default value for [`connect_timeout`](Self::connect_timeout).
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

    /// Default value for [`idle_timeout`](Self::idle_timeout).
    pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default value for [`statement_timeout`](Self::statement_timeout).
    pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

    /// Default value for [`hotswap_poll_interval`](Self::hotswap_poll_interval).
    pub const DEFAULT_HOTSWAP_POLL_INTERVAL: Duration = Duration::from_secs(5);
}

impl Default for PostgresConnectionOptions {
    fn default() -> Self {
        Self {
            app_name: String::from("iox"),
            schema_name: String::from(Self::DEFAULT_SCHEMA_NAME),
            dsn: String::new(),
            max_conns: Self::DEFAULT_MAX_CONNS,
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: Self::DEFAULT_IDLE_TIMEOUT,
            statement_timeout: Self::DEFAULT_STATEMENT_TIMEOUT,
            hotswap_poll_interval: Self::DEFAULT_HOTSWAP_POLL_INTERVAL,
        }
    }
}

/// PostgreSQL catalog.
#[derive(Debug)]
pub struct PostgresCatalog {
    metrics: CatalogMetrics,
    pool: HotSwapPool<Postgres>,

    // keep around in the background
    refresh_task: Arc<JoinSet<()>>,

    time_provider: Arc<dyn TimeProvider>,
    // Connection options for display
    options: PostgresConnectionOptions,
}

impl PostgresCatalog {
    const NAME: &'static str = "postgres";

    /// Connect to the catalog store.
    pub async fn connect(
        options: PostgresConnectionOptions,
        metrics: Arc<metric::Registry>,
    ) -> Result<Self> {
        let (pool, refresh_task) = new_pool(&options, Arc::clone(&metrics)).await?;
        let time_provider = Arc::new(SystemProvider::new()) as _;

        Ok(Self {
            pool,
            refresh_task: Arc::new(refresh_task),
            metrics: CatalogMetrics::new(metrics, Arc::clone(&time_provider), Self::NAME),
            time_provider,
            options,
        })
    }

    fn schema_name(&self) -> &str {
        &self.options.schema_name
    }

    #[cfg(test)]
    pub(crate) fn into_pool(self) -> HotSwapPool<Postgres> {
        self.pool
    }
}

/// transaction for [`PostgresCatalog`].
#[derive(Debug)]
pub struct PostgresTxn {
    inner: PostgresTxnInner,

    // keep around in the background
    #[allow(dead_code)]
    refresh_task: Arc<JoinSet<()>>,

    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
struct PostgresTxnInner {
    pool: HotSwapPool<Postgres>,
}

impl<'c> Executor<'c> for &'c mut PostgresTxnInner {
    type Database = Postgres;

    #[allow(clippy::type_complexity)]
    fn fetch_many<'e, 'q, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Self::Database as sqlx::Database>::QueryResult,
                <Self::Database as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        'q: 'e,
        E: sqlx::Execute<'q, Self::Database> + 'q,
    {
        self.pool.fetch_many(query)
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        'q: 'e,
        E: sqlx::Execute<'q, Self::Database> + 'q,
    {
        self.pool.fetch_optional(query)
    }

    fn prepare_with<'e, 'q>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::database::HasStatement<'q>>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
        'q: 'e,
    {
        self.pool.prepare_with(sql, parameters)
    }

    fn describe<'e, 'q>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
        'q: 'e,
    {
        self.pool.describe(sql)
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // We need to create the schema if we're going to set it as the first item of the
        // search_path otherwise when we run the sqlx migration scripts for the first time, sqlx
        // will create the `_sqlx_migrations` table in the public namespace (the only namespace
        // that exists), but the second time it will create it in the `<schema_name>` namespace and
        // re-run all the migrations without skipping the ones already applied (see #3893).
        //
        // This makes the migrations/20210217134322_create_schema.sql step unnecessary; we need to
        // keep that file because migration files are immutable.
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {};", self.schema_name());
        self.pool.execute(sqlx::query(&create_schema_query)).await?;

        MIGRATOR.run(&self.pool).await?;

        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(self.metrics.repos(Box::new(PostgresTxn {
            inner: PostgresTxnInner {
                pool: self.pool.clone(),
            },
            refresh_task: Arc::clone(&self.refresh_task),
            time_provider: Arc::clone(&self.time_provider),
        })))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        self.metrics.registry()
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    async fn active_applications(&self) -> Result<HashSet<String>, Error> {
        let pool = self.pool.clone();

        let rows = sqlx::query(
            r#"
            SELECT DISTINCT application_name
            FROM pg_stat_activity
            WHERE datname = current_database();
            "#,
        )
        .fetch_all(&pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| row.get("application_name"))
            .collect())
    }

    fn name(&self) -> &'static str {
        Self::NAME
    }
}

/// Adapter to connect sqlx pools with our metrics system.
#[derive(Debug, Clone, Default)]
struct PoolMetrics {
    /// Actual shared state.
    state: Arc<PoolMetricsInner>,
}

/// Inner state of [`PoolMetrics`] that is wrapped into an [`Arc`].
#[derive(Debug, Default)]
struct PoolMetricsInner {
    /// Next pool ID.
    pool_id_gen: AtomicU64,

    /// Set of known pools and their ID labels.
    ///
    /// Note: The pool is internally ref-counted via an [`Arc`]. Holding a reference does NOT prevent it from being closed.
    pools: RwLock<Vec<(Arc<str>, sqlx::Pool<Postgres>)>>,
}

impl PoolMetrics {
    /// Create new pool metrics.
    fn new(metrics: Arc<metric::Registry>) -> Self {
        metrics.register_instrument("iox_catalog_postgres", Self::default)
    }

    /// Register a new pool.
    fn register_pool(&self, pool: sqlx::Pool<Postgres>) {
        let id = self
            .state
            .pool_id_gen
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .into();
        let mut pools = self.state.pools.write();
        pools.push((id, pool));
    }

    /// Remove closed pools from given list.
    fn clean_pools(pools: &mut Vec<(Arc<str>, sqlx::Pool<Postgres>)>) {
        pools.retain(|(_id, p)| !p.is_closed());
    }
}

impl Instrument for PoolMetrics {
    fn report(&self, reporter: &mut dyn metric::Reporter) {
        let mut pools = self.state.pools.write();
        Self::clean_pools(&mut pools);
        let pools = RwLockWriteGuard::downgrade(pools);

        reporter.start_metric(
            "sqlx_postgres_pools",
            "Number of pools that sqlx uses",
            MetricKind::U64Gauge,
        );
        reporter.report_observation(
            &Attributes::from([]),
            metric::Observation::U64Gauge(pools.len() as u64),
        );
        reporter.finish_metric();

        reporter.start_metric(
            "sqlx_postgres_connections",
            "Number of connections within the postgres connection pool that sqlx uses",
            MetricKind::U64Gauge,
        );
        for (id, p) in pools.iter() {
            let active = p.size() as u64;
            let idle = p.num_idle() as u64;

            // We get both values independently (from underlying atomic counters) so they might be out of sync (with a
            // low likelyhood). Calculating this value and emitting it is useful though since it allows easier use in
            // dashboards since you can `max_over_time` w/o any recording rules.
            let used = active.saturating_sub(idle);

            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("active")),
                ]),
                metric::Observation::U64Gauge(active),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("idle")),
                ]),
                metric::Observation::U64Gauge(idle),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("used")),
                ]),
                metric::Observation::U64Gauge(used),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("max")),
                ]),
                metric::Observation::U64Gauge(p.options().get_max_connections() as u64),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("min")),
                ]),
                metric::Observation::U64Gauge(p.options().get_min_connections() as u64),
            );
        }

        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Creates a new [`sqlx::Pool`] from a database config and an explicit DSN.
///
/// This function doesn't support the IDPE specific `dsn-file://` uri scheme.
async fn new_raw_pool(
    options: &PostgresConnectionOptions,
    parsed_dsn: &str,
    metrics: PoolMetrics,
) -> Result<sqlx::Pool<Postgres>, sqlx::Error> {
    // sqlx exposes some options as pool options, while other options are available as connection options.
    let mut connect_options = PgConnectOptions::from_str(parsed_dsn)?
        // the default is INFO, which is frankly surprising.
        .log_statements(log::LevelFilter::Trace);

    // Workaround sqlx ignoring the SSL_CERT_FILE environment variable.
    // Remove workaround when upstream sqlx handles SSL_CERT_FILE properly (#8994).
    let cert_file = env::var("SSL_CERT_FILE").unwrap_or_default();
    if !cert_file.is_empty() {
        connect_options = connect_options.ssl_root_cert(cert_file);
    }

    let app_name = options.app_name.clone();
    let app_name2 = options.app_name.clone(); // just to log below
    let schema_name = options.schema_name.clone();
    let statement_timeout = options.statement_timeout.as_millis();
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(options.max_conns)
        .acquire_timeout(options.connect_timeout)
        .idle_timeout(options.idle_timeout)
        .test_before_acquire(true)
        .after_connect(move |c, _meta| {
            let app_name = app_name.to_owned();
            let schema_name = schema_name.to_owned();
            Box::pin(async move {
                // Tag the connection with the provided application name, while allowing it to
                // be override from the connection string (aka DSN).
                // If current_application_name is empty here it means the application name wasn't
                // set as part of the DSN, and we can set it explicitly.
                // Recall that this block is running on connection, not when creating the pool!
                let current_application_name: String =
                    sqlx::query_scalar("SELECT current_setting('application_name');")
                        .fetch_one(&mut *c)
                        .await?;
                if current_application_name.is_empty() {
                    sqlx::query("SELECT set_config('application_name', $1, false);")
                        .bind(&*app_name)
                        .execute(&mut *c)
                        .await?;
                }
                let search_path_query = format!("SET search_path TO {schema_name},public;");
                c.execute(sqlx::query(&search_path_query)).await?;
                let timeout = format!("SET statement_timeout to {statement_timeout}");
                c.execute(sqlx::query(&timeout)).await?;

                // Ensure explicit timezone selection, instead of deferring to
                // the server value.
                c.execute("SET timezone = 'UTC';").await?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await?;

    // Log a connection was successfully established and include the application
    // name for cross-correlation between Conductor logs & database connections.
    info!(application_name=%app_name2, "connected to config store");

    metrics.register_pool(pool.clone());
    Ok(pool)
}

/// Parse a postgres catalog dsn, handling the special `dsn-file://`
/// syntax (see [`new_pool`] for more details).
///
/// Returns an error if the dsn-file could not be read correctly.
pub fn parse_dsn(dsn: &str) -> Result<String, sqlx::Error> {
    let dsn = match get_dsn_file_path(dsn) {
        Some(filename) => std::fs::read_to_string(filename)?,
        None => dsn.to_string(),
    };
    Ok(dsn)
}

/// Creates a new HotSwapPool
///
/// This function understands the IDPE specific `dsn-file://` dsn uri scheme
/// and hot swaps the pool with a new sqlx::Pool when the file changes.
/// This is useful because the credentials can be rotated by infrastructure
/// agents while the service is running.
///
/// The file is polled for changes every `polling_interval`.
///
/// The pool is replaced only once the new pool is successfully created.
/// The [`new_raw_pool`] function will return a new pool only if the connection
/// is successfull (see [`sqlx::pool::PoolOptions::test_before_acquire`]).
async fn new_pool(
    options: &PostgresConnectionOptions,
    metrics: Arc<metric::Registry>,
) -> Result<(HotSwapPool<Postgres>, JoinSet<()>), sqlx::Error> {
    let parsed_dsn = parse_dsn(&options.dsn)?;
    let metrics = PoolMetrics::new(metrics);
    let pool = HotSwapPool::new(new_raw_pool(options, &parsed_dsn, metrics.clone()).await?);
    let polling_interval = options.hotswap_poll_interval;
    let mut refresh_task = JoinSet::new();

    if let Some(dsn_file) = get_dsn_file_path(&options.dsn) {
        let pool = pool.clone();
        let options = options.clone();

        // TODO(mkm): return a guard that stops this background worker.
        // We create only one pool per process, but it would be cleaner to be
        // able to properly destroy the pool. If we don't kill this worker we
        // effectively keep the pool alive (since it holds a reference to the
        // Pool) and we also potentially pollute the logs with spurious warnings
        // if the dsn file disappears (this may be annoying if they show up in the test
        // logs).
        refresh_task.spawn(async move {
            let mut current_dsn = parsed_dsn.clone();
            let mut close_pool_tasks = JoinSet::new();

            loop {
                tokio::time::sleep(polling_interval).await;

                match try_update(
                    &options,
                    &current_dsn,
                    &dsn_file,
                    &pool,
                    metrics.clone(),
                    &mut close_pool_tasks,
                )
                .await
                {
                    Ok(None) => {}
                    Ok(Some(new_dsn)) => {
                        current_dsn = new_dsn;
                    }
                    Err(e) => {
                        warn!(
                            error=%e,
                            filename=%dsn_file,
                            "not replacing hotswap pool because of an error \
                            connecting to the new DSN"
                        );
                    }
                }
            }
        });
    }

    Ok((pool, refresh_task))
}

async fn try_update(
    options: &PostgresConnectionOptions,
    current_dsn: &str,
    dsn_file: &str,
    pool: &HotSwapPool<Postgres>,
    metrics: PoolMetrics,
    close_pool_tasks: &mut JoinSet<()>,
) -> Result<Option<String>, sqlx::Error> {
    let new_dsn = tokio::fs::read_to_string(dsn_file).await?;

    if new_dsn == current_dsn {
        Ok(None)
    } else {
        let new_pool = new_raw_pool(options, &new_dsn, metrics).await?;
        let old_pool = pool.replace(new_pool);
        info!("replaced hotswap pool");

        // check for old tasks
        while let Some(res) = close_pool_tasks.try_join_next() {
            if let Err(e) = res {
                warn!(%e, "closing of old pool of previous DSN change failed");
            }
        }
        let n_tasks = close_pool_tasks.len();
        if n_tasks > 0 {
            warn!(
                n_tasks,
                "still trying to close old pools from previous DSN changes"
            );
        }

        // Spawn new background task to close the pool.
        // This may take a while and we don't want this to block the overall check&refresh loop.
        // See https://github.com/influxdata/influxdb_iox/issues/10353 .
        close_pool_tasks.spawn(async move {
            info!(?old_pool, "closing old DB connection pool");
            // The pool is not closed on drop. We need to call `close`.
            // It will close all idle connections, and wait until acquired connections
            // are returned to the pool or closed.
            old_pool.close().await;
            info!(?old_pool, "closed old DB connection pool");
        });

        Ok(Some(new_dsn))
    }
}

// Parses a `dsn-file://` scheme, according to the rules of the IDPE kit/sql package.
//
// If the dsn matches the `dsn-file://` prefix, the prefix is removed and the rest is interpreted
// as a file name, in which case this function will return `Some(filename)`.
// Otherwise it will return None. No URI decoding is performed on the filename.
fn get_dsn_file_path(dsn: &str) -> Option<String> {
    const DSN_SCHEME: &str = "dsn-file://";
    dsn.starts_with(DSN_SCHEME)
        .then(|| dsn[DSN_SCHEME.len()..].to_owned())
}

impl RepoCollection for PostgresTxn {
    fn root(&mut self) -> &mut dyn RootRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }

    fn set_span_context(&mut self, _span_ctx: Option<SpanContext>) {}
}

async fn insert_column_with_connection<'q, E>(
    executor: E,
    name: &str,
    table_id: TableId,
    column_type: ColumnType,
) -> Result<Column>
where
    E: Executor<'q, Database = Postgres>,
{
    let rec = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT $1, table_id, $3 FROM (
    SELECT max_columns_per_table, namespace.id, table_name.id as table_id, COUNT(column_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
                   LEFT JOIN column_name ON table_name.id = column_name.table_id
    WHERE table_name.id = $2
    GROUP BY namespace.max_columns_per_table, namespace.id, table_name.id
) AS get_count WHERE count < max_columns_per_table
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(table_id) // $2
        .bind(column_type) // $3
        .fetch_one(executor)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::LimitExceeded {
                descr: format!("couldn't create column {} in table {}; limit reached on namespace", name, table_id)
            },
            _ => {
            if is_fk_violation(&e) {
                Error::NotFound { descr: e.to_string() }
            } else {
                Error::External { source: Arc::new(e) }
            }
        }})?;

    ensure!(
        rec.column_type == column_type,
        AlreadyExistsSnafu {
            descr: format!(
                "column {} is type {} but schema update has type {}",
                name, rec.column_type, column_type
            ),
        }
    );

    Ok(rec)
}

#[async_trait]
impl RootRepo for PostgresTxn {
    async fn snapshot(&mut self) -> Result<RootSnapshot> {
        let mut tx = self.inner.pool.begin().await?;

        sqlx::query("SELECT * from root FOR UPDATE;")
            .fetch_one(&mut *tx)
            .await?;

        let namespaces = sqlx::query_as::<_, Namespace>("SELECT * from namespace;")
            .fetch_all(&mut *tx)
            .await?;

        let (generation,): (i64,) =
            sqlx::query_as("UPDATE root SET generation = generation + 1 RETURNING generation;")
                .fetch_one(&mut *tx)
                .await?;

        tx.commit().await?;

        Ok(RootSnapshot::encode(namespaces, generation as _)?)
    }
}

#[async_trait]
impl NamespaceRepo for PostgresTxn {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let max_tables = service_protection_limits
            .and_then(|l| l.max_tables)
            .unwrap_or_default();
        let max_columns_per_table = service_protection_limits
            .and_then(|l| l.max_columns_per_table)
            .unwrap_or_default();

        let rec = sqlx::query_as::<_, Namespace>(
            r#"
INSERT INTO namespace (
    name, retention_period_ns, max_tables, max_columns_per_table, partition_template, router_version
)
VALUES ( $1, $2, $3, $4, $5, $6)
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template, router_version;
            "#,
        )
        .bind(name.as_str()) // $1
        .bind(retention_period_ns) // $2
        .bind(max_tables) // $3
        .bind(max_columns_per_table) // $4
        .bind(partition_template) // $5
        .bind(NamespaceVersion::default()); // $6 - T

        let rec = rec.fetch_one(&mut self.inner).await.map_err(|e| {
            if is_unique_violation(&e) {
                Error::AlreadyExists {
                    descr: name.to_string(),
                }
            } else if is_fk_violation(&e) {
                Error::NotFound {
                    descr: e.to_string(),
                }
            } else {
                Error::External {
                    source: Arc::new(e),
                }
            }
        })?;

        Ok(rec)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template, router_version
FROM namespace
WHERE {v};
                "#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .fetch_all(&mut self.inner)
        .await?;

        Ok(rec)
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template, router_version
FROM namespace
WHERE id=$1 AND {v};
                "#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec?;

        Ok(Some(namespace))
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template, router_version
FROM namespace
WHERE name=$1 AND {v};
                "#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(name) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec?;

        Ok(Some(namespace))
    }

    async fn soft_delete(&mut self, name: &str) -> Result<NamespaceId> {
        let flagged_at = Timestamp::from(self.time_provider.now());

        // note that there is a uniqueness constraint on the name column in the DB
        let rec = sqlx::query_as::<_, NamespaceId>(
            r#"UPDATE namespace SET deleted_at=$1 WHERE name = $2 RETURNING id;"#,
        )
        .bind(flagged_at) // $1
        .bind(name) // $2
        .fetch_one(&mut self.inner)
        .await;

        let id = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NotFound {
                descr: name.to_string(),
            },
            _ => Error::External {
                source: Arc::new(e),
            },
        })?;

        Ok(id)
    }

    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_tables = $1, router_version = router_version + 1
WHERE name = $2
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template, router_version;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NotFound {
                descr: name.to_string(),
            },
            _ => Error::External {
                source: Arc::new(e),
            },
        })?;

        Ok(namespace)
    }

    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_columns_per_table = $1, router_version = router_version + 1
WHERE name = $2
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template, router_version;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NotFound {
                descr: name.to_string(),
            },
            _ => Error::External {
                source: Arc::new(e),
            },
        })?;

        Ok(namespace)
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET retention_period_ns = $1, router_version = router_version + 1
WHERE name = $2
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template, router_version;
        "#,
        )
        .bind(retention_period_ns) // $1
        .bind(name) // $2
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NotFound {
                descr: name.to_string(),
            },
            _ => Error::External {
                source: Arc::new(e),
            },
        })?;

        Ok(namespace)
    }

    async fn snapshot(&mut self, namespace_id: NamespaceId) -> Result<NamespaceSnapshot> {
        let mut tx = self.inner.pool.begin().await?;
        let rec =
            sqlx::query_as::<_, Namespace>("SELECT * from namespace WHERE id = $1 FOR UPDATE;")
                .bind(namespace_id) // $1
                .fetch_one(&mut *tx)
                .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Err(Error::NotFound {
                descr: format!("namespace: {namespace_id}"),
            });
        }
        let ns = rec?;

        let tables =
            sqlx::query_as::<_, Table>("SELECT * from table_name where namespace_id = $1;")
                .bind(namespace_id) // $1
                .fetch_all(&mut *tx)
                .await?;

        let (generation,): (i64,) = sqlx::query_as(
            "UPDATE namespace SET generation = generation + 1 where id = $1 RETURNING generation;",
        )
        .bind(namespace_id) // $1
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(NamespaceSnapshot::encode(ns, tables, generation as _)?)
    }

    async fn snapshot_by_name(&mut self, name: &str) -> Result<NamespaceSnapshot> {
        namespace_snapshot_by_name(self, name).await
    }
}

#[async_trait]
impl TableRepo for PostgresTxn {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let mut tx = self.inner.pool.begin().await?;

        // A simple insert statement becomes quite complicated in order to avoid checking the table
        // limits in a select and then conditionally inserting (which would be racey).
        //
        // from https://www.postgresql.org/docs/current/sql-insert.html
        //   "INSERT inserts new rows into a table. One can insert one or more rows specified by
        //   value expressions, or zero or more rows resulting from a query."
        // By using SELECT rather than VALUES it will insert zero rows if it finds a null in the
        // subquery, i.e. if count >= max_tables. fetch_one() will return a RowNotFound error if
        // nothing was inserted. Not pretty!
        let table = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
SELECT $1, id, $2 FROM (
    SELECT namespace.id AS id, max_tables, COUNT(table_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    WHERE namespace.id = $3
    GROUP BY namespace.max_tables, table_name.namespace_id, namespace.id
) AS get_count WHERE count < max_tables
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(partition_template) // $2
        .bind(namespace_id) // $3
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::LimitExceeded {
                descr: format!(
                    "couldn't create table {}; limit reached on namespace {}",
                    name, namespace_id
                ),
            },
            _ => {
                if is_unique_violation(&e) {
                    Error::AlreadyExists {
                        descr: format!("table '{name}' in namespace {namespace_id}"),
                    }
                } else if is_fk_violation(&e) {
                    Error::NotFound {
                        descr: e.to_string(),
                    }
                } else {
                    Error::External {
                        source: Arc::new(e),
                    }
                }
            }
        })?;

        // Partitioning is only supported for tags, so create tag columns for all `TagValue`
        // partition template parts. It's important this happens within the table creation
        // transaction so that there isn't a possibility of a concurrent write creating these
        // columns with an unsupported type.
        for template_part in table.partition_template.parts() {
            if let TemplatePart::TagValue(tag_name) = template_part {
                insert_column_with_connection(&mut *tx, tag_name, table.id, ColumnType::Tag)
                    .await?;
            }
        }

        tx.commit().await?;

        Ok(table)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec?;

        Ok(Some(table))
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1 AND name = $2;
            "#,
        )
        .bind(namespace_id) // $1
        .bind(name) // $2
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec?;

        Ok(Some(table))
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(&mut self.inner)
        .await?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>("SELECT * FROM table_name;")
            .fetch_all(&mut self.inner)
            .await?;

        Ok(rec)
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        let mut tx = self.inner.pool.begin().await?;
        let rec = sqlx::query_as::<_, Table>("SELECT * from table_name WHERE id = $1 FOR UPDATE;")
            .bind(table_id) // $1
            .fetch_one(&mut *tx)
            .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Err(Error::NotFound {
                descr: format!("table: {table_id}"),
            });
        }
        let table = rec?;

        let columns = sqlx::query_as::<_, Column>("SELECT * from column_name where table_id = $1;")
            .bind(table_id) // $1
            .fetch_all(&mut *tx)
            .await?;

        let partitions =
            sqlx::query_as::<_, Partition>(r#"SELECT * FROM partition WHERE table_id = $1;"#)
                .bind(table_id) // $1
                .fetch_all(&mut *tx)
                .await?;

        let (generation,): (i64,) = sqlx::query_as(
            "UPDATE table_name SET generation = generation + 1 where id = $1 RETURNING generation;",
        )
        .bind(table_id) // $1
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(TableSnapshot::encode(
            table,
            partitions,
            columns,
            generation as _,
        )?)
    }
}

#[async_trait]
impl ColumnRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        insert_column_with_connection(&mut self.inner, name, table_id, column_type).await
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(&mut self.inner)
        .await?;

        Ok(rec)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT * FROM column_name
WHERE table_id = $1;
            "#,
        )
        .bind(table_id)
        .fetch_all(&mut self.inner)
        .await?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>("SELECT * FROM column_name;")
            .fetch_all(&mut self.inner)
            .await?;

        Ok(rec)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let num_columns = columns.len();
        let (v_name, v_column_type): (Vec<&str>, Vec<i16>) = columns
            .iter()
            .map(|(&name, &column_type)| (name, column_type as i16))
            .unzip();

        // The `ORDER BY` in this statement is important to avoid deadlocks during concurrent
        // writes to the same IOx table that each add many new columns. See:
        //
        // - <https://rcoh.svbtle.com/postgres-unique-constraints-can-cause-deadlock>
        // - <https://dba.stackexchange.com/a/195220/27897>
        // - <https://github.com/influxdata/idpe/issues/16298>
        let out = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT name, $1, column_type
FROM UNNEST($2, $3) as a(name, column_type)
ORDER BY name
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
            "#,
        )
        .bind(table_id) // $1
        .bind(&v_name) // $2
        .bind(&v_column_type) // $3
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::NotFound {
                    descr: e.to_string(),
                }
            } else {
                Error::External {
                    source: Arc::new(e),
                }
            }
        })?;

        assert_eq!(num_columns, out.len());

        for existing in &out {
            let want = columns.get(existing.name.as_str()).unwrap();
            ensure!(
                existing.column_type == *want,
                AlreadyExistsSnafu {
                    descr: format!(
                        "column {} is type {} but schema update has type {}",
                        existing.name, existing.column_type, want
                    ),
                }
            );
        }

        Ok(out)
    }
}

#[async_trait]
impl PartitionRepo for PostgresTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let hash_id = PartitionHashId::new(table_id, &key);

        let v = sqlx::query_as::<_, Partition>(
            r#"
INSERT INTO partition
    (partition_key, table_id, hash_id, sort_key_ids)
VALUES
    ( $1, $2, $3, '{}')
ON CONFLICT ON CONSTRAINT partition_key_unique
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at;
        "#,
        )
        .bind(&key) // $1
        .bind(table_id) // $2
        .bind(&hash_id) // $3
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::NotFound {
                    descr: e.to_string(),
                }
            } else if is_unique_violation(&e) {
                // Logging more information to diagnose a production issue maybe
                warn!(
                    error=?e,
                    %table_id,
                    %key,
                    %hash_id,
                    "possible duplicate partition_hash_id?"
                );
                Error::External {
                    source: Arc::new(e),
                }
            } else {
                Error::External {
                    source: Arc::new(e),
                }
            }
        })?;

        Ok(v)
    }

    async fn set_new_file_at(
        &mut self,
        _partition_id: PartitionId,
        _new_file_at: Timestamp,
    ) -> Result<()> {
        Err(Error::NotImplemented {
            descr: "set_new_file_at is for test use only, not implemented for postgres".to_string(),
        })
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        let ids: Vec<_> = partition_ids.iter().map(|p| p.get()).collect();

        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at
FROM partition
WHERE id = ANY($1);
        "#,
        )
        .bind(&ids[..]) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at
FROM partition
WHERE table_id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    /// Update the sort key for `partition_id` if and only if `old_sort_key`
    /// matches the current value in the database.
    ///
    /// This compare-and-swap operation is allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons (avoiding multiple
    /// round trips to service a transaction in the happy path).
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        let old_sort_key_ids = old_sort_key_ids
            .map(std::ops::Deref::deref)
            .unwrap_or_default();

        // This `match` will go away when all partitions have hash IDs in the database.
        let query = sqlx::query_as::<_, Partition>(
            r#"
UPDATE partition
SET sort_key_ids = $1
WHERE id = $2 AND sort_key_ids = $3
RETURNING id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at;
        "#,
        )
        .bind(new_sort_key_ids) // $1
        .bind(partition_id) // $2
        .bind(old_sort_key_ids); // $3;

        let res = query.fetch_one(&mut self.inner).await;

        let partition = match res {
            Ok(v) => v,
            Err(sqlx::Error::RowNotFound) => {
                // This update may have failed either because:
                //
                // * A row with the specified ID did not exist at query time
                //   (but may exist now!)
                // * The sort key does not match.
                //
                // To differentiate, we submit a get partition query, returning
                // the actual sort key if successful.
                //
                // NOTE: this is racy, but documented - this might return "Sort
                // key differs! Old key: <old sort key you provided>"
                let partition = (self as &mut dyn PartitionRepo)
                    .get_by_id(partition_id)
                    .await
                    .map_err(CasFailure::QueryError)?
                    .ok_or(CasFailure::QueryError(Error::NotFound {
                        descr: partition_id.to_string(),
                    }))?;
                return Err(CasFailure::ValueMismatch(
                    partition.sort_key_ids().cloned().unwrap_or_default(),
                ));
            }
            Err(e) => {
                return Err(CasFailure::QueryError(Error::External {
                    source: Arc::new(e),
                }))
            }
        };

        debug!(
            ?partition_id,
            ?new_sort_key_ids,
            "partition sort key cas successful"
        );

        Ok(partition)
    }

    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        sqlx::query(
            r#"
INSERT INTO skipped_compactions
    ( partition_id, reason, num_files, limit_num_files, limit_num_files_first_in_partition, estimated_bytes, limit_bytes, skipped_at )
VALUES
    ( $1, $2, $3, $4, $5, $6, $7, extract(epoch from NOW()) )
ON CONFLICT ( partition_id )
DO UPDATE
SET
reason = EXCLUDED.reason,
num_files = EXCLUDED.num_files,
limit_num_files = EXCLUDED.limit_num_files,
limit_num_files_first_in_partition = EXCLUDED.limit_num_files_first_in_partition,
estimated_bytes = EXCLUDED.estimated_bytes,
limit_bytes = EXCLUDED.limit_bytes,
skipped_at = EXCLUDED.skipped_at;
        "#,
        )
        .bind(partition_id) // $1
        .bind(reason)
        .bind(num_files as i64)
        .bind(limit_num_files as i64)
        .bind(limit_num_files_first_in_partition as i64)
        .bind(estimated_bytes as i64)
        .bind(limit_bytes as i64)
        .execute(&mut self.inner)
        .await?;
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let rec = sqlx::query_as::<_, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id = ANY($1);"#,
        )
        .bind(partition_ids) // $1
        .fetch_all(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(Vec::new());
        }

        let skipped_partition_records = rec?;

        Ok(skipped_partition_records)
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
SELECT * FROM skipped_compactions
        "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
DELETE FROM skipped_compactions
WHERE partition_id = $1
RETURNING *
        "#,
        )
        .bind(partition_id)
        .fetch_optional(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        sqlx::query_as(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at
FROM partition
ORDER BY id DESC
LIMIT $1;"#,
        )
        .bind(n as i64) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let sql = format!(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            WHERE p.new_file_at > $1
            {}
            "#,
            maximum_time
                .map(|_| "AND p.new_file_at < $2")
                .unwrap_or_default()
        );

        sqlx::query_as(&sql)
            .bind(minimum_time) // $1
            .bind(maximum_time) // $2
            .fetch_all(&mut self.inner)
            .await
            .map_err(Error::from)
    }

    async fn partitions_needing_cold_compact(
        &mut self,
        maximum_time: Timestamp,
        n: usize,
    ) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
SELECT p.id as partition_id
FROM partition p
WHERE p.new_file_at != 0
AND p.new_file_at <= $1
AND (p.cold_compact_at = 0 OR p.cold_compact_at < p.new_file_at)
AND p.id not in (select partition_id from skipped_compactions)
ORDER BY p.new_file_at ASC
limit $2;"#,
        )
        .bind(maximum_time) // $1
        .bind(n as i64) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn update_cold_compact(
        &mut self,
        partition_id: PartitionId,
        cold_compact_at: Timestamp,
    ) -> Result<()> {
        sqlx::query(
            r#"
UPDATE partition
SET cold_compact_at = $2
WHERE id = $1;
        "#,
        )
        .bind(partition_id) // $1
        .bind(cold_compact_at) // $2
        .execute(&mut self.inner)
        .await?;

        Ok(())
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        // Correctness: the main caller of this function, the partition bloom
        // filter, relies on all partitions being made available to it.
        //
        // This function MUST return the full set of old partitions to the
        // caller - do NOT apply a LIMIT to this query.
        //
        // The load this query saves vastly outsizes the load this query causes.
        sqlx::query_as(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at
FROM partition
WHERE hash_id IS NULL
ORDER BY id DESC;"#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn delete_by_retention(&mut self) -> Result<Vec<(TableId, PartitionId)>> {
        let mut tx = self.inner.pool.begin().await?;

        let mut stream = sqlx::query_as::<_, (PartitionId, PartitionKey, TableId, i64, TablePartitionTemplateOverride)>(
            r#"
SELECT partition.id, partition.partition_key, table_name.id, namespace.retention_period_ns, table_name.partition_template
FROM partition
JOIN table_name on partition.table_id = table_name.id
JOIN namespace on table_name.namespace_id = namespace.id
WHERE NOT EXISTS (SELECT 1 from parquet_file where partition_id = partition.id)
    AND retention_period_ns IS NOT NULL;
        "#,
        )
            .fetch(&mut *tx);

        let now = self.time_provider.now();
        let mut to_remove = Vec::with_capacity(MAX_PARTITION_SELECTED_ONCE_FOR_DELETE);

        while let Some((p_id, key, t_id, retention, template)) = stream.next().await.transpose()? {
            if should_delete_partition(&key, &template, retention, now) {
                to_remove.push((t_id, p_id));

                if to_remove.len() == MAX_PARTITION_SELECTED_ONCE_FOR_DELETE {
                    break;
                }
            }
        }
        drop(stream);

        let ids: Vec<_> = to_remove.iter().map(|(_, p)| p.get()).collect();

        sqlx::query(r#"DELETE FROM partition WHERE id = ANY($1);"#)
            .bind(ids) // $1
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(to_remove)
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        let mut tx = self.inner.pool.begin().await?;

        let rec =
            sqlx::query_as::<_, Partition>("SELECT * from partition WHERE id = $1 FOR UPDATE;")
                .bind(partition_id) // $1
                .fetch_one(&mut *tx)
                .await;
        if let Err(sqlx::Error::RowNotFound) = rec {
            return Err(Error::NotFound {
                descr: format!("partition: {partition_id}"),
            });
        }
        let partition = rec?;

        let files =
            sqlx::query_as::<_, ParquetFile>("SELECT * from parquet_file where partition_id = $1 AND parquet_file.to_delete IS NULL;")
                .bind(partition_id) // $1
                .fetch_all(&mut *tx)
                .await?;

        let sc = sqlx::query_as::<_, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id = $1;"#,
        )
        .bind(partition_id) // $1
        .fetch_optional(&mut *tx)
        .await?;

        let (generation, namespace_id): (i64,NamespaceId) = sqlx::query_as(
            "UPDATE partition SET generation = partition.generation + 1 from table_name where partition.id = $1 and table_name.id = partition.table_id RETURNING partition.generation, table_name.namespace_id;",
        )
        .bind(partition_id) // $1
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(PartitionSnapshot::encode(
            namespace_id,
            partition,
            files,
            sc,
            generation as _,
        )?)
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresTxn {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let flagged_at = Timestamp::from(self.time_provider.now());
        // TODO - include check of table retention period once implemented
        let flagged = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT parquet_file.object_store_id
    FROM namespace, parquet_file
    WHERE namespace.retention_period_ns IS NOT NULL
    AND parquet_file.to_delete IS NULL
    AND parquet_file.max_time < $1 - namespace.retention_period_ns
    AND namespace.id = parquet_file.namespace_id
    LIMIT $2
)
UPDATE parquet_file
SET to_delete = $1
WHERE object_store_id IN (SELECT object_store_id FROM parquet_file_ids)
RETURNING partition_id, object_store_id;
            "#,
        )
        .bind(flagged_at) // $1
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION) // $2
        .fetch_all(&mut self.inner)
        .await?;

        let flagged = flagged
            .into_iter()
            .map(|row| (row.get("partition_id"), row.get("object_store_id")))
            .collect();
        Ok(flagged)
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        // see https://www.crunchydata.com/blog/simulating-update-or-delete-with-limit-in-postgres-ctes-to-the-rescue
        let deleted = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT object_store_id
    FROM parquet_file
    WHERE to_delete < $1
    LIMIT $2
)
DELETE FROM parquet_file
WHERE object_store_id IN (SELECT object_store_id FROM parquet_file_ids)
RETURNING object_store_id;
             "#,
        )
        .bind(older_than) // $1
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE) // $2
        .fetch_all(&mut self.inner)
        .await?;

        let deleted = deleted
            .into_iter()
            .map(|row| row.get("object_store_id"))
            .collect();
        Ok(deleted)
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
            (
                SELECT parquet_file.id, namespace_id, parquet_file.table_id, partition_id, partition_hash_id,
                    object_store_id, min_time, max_time, parquet_file.to_delete, file_size_bytes, row_count,
                    compaction_level, created_at, column_set, max_l0_created_at, source
                FROM parquet_file
                WHERE parquet_file.partition_id = ANY($1)
                  AND parquet_file.to_delete IS NULL
                  AND compaction_level != 0
            )
            UNION ALL
            (
                SELECT parquet_file.id, namespace_id, parquet_file.table_id, partition_id, partition_hash_id,
                    object_store_id, min_time, max_time, parquet_file.to_delete, file_size_bytes, row_count,
                    compaction_level, created_at, column_set, max_l0_created_at, source
                FROM parquet_file
                WHERE parquet_file.partition_id = ANY($1)
                  AND parquet_file.to_delete IS NULL
                  AND compaction_level = 0
                ORDER BY max_l0_created_at
                LIMIT $2
            )
        "#,
        )
        .bind(partition_ids) // $1
        .bind(MAX_PARQUET_L0_FILES_PER_PARTITION) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, partition_hash_id, object_store_id, min_time,
       max_time, to_delete, file_size_bytes, row_count, compaction_level, created_at, column_set,
       max_l0_created_at, source
FROM parquet_file
WHERE object_store_id = $1;
             "#,
        )
        .bind(object_store_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let parquet_file = rec?;

        Ok(Some(parquet_file))
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        sqlx::query(
            // sqlx's readme suggests using PG's ANY operator instead of IN; see link below.
            // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
            r#"
SELECT object_store_id
FROM parquet_file
WHERE object_store_id = ANY($1);
             "#,
        )
        .bind(object_store_ids) // $1
        .map(|pgr| pgr.get::<ObjectStoreId, _>("object_store_id"))
        .fetch_all(&mut self.inner)
        .await
        .map_err(Error::from)
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let delete_set: HashSet<_> = delete.iter().map(|d| d.get_uuid()).collect();
        let upgrade_set: HashSet<_> = upgrade.iter().map(|u| u.get_uuid()).collect();

        assert!(
            delete_set.is_disjoint(&upgrade_set),
            "attempted to upgrade a file scheduled for delete"
        );

        let mut tx = self.inner.pool.begin().await?;

        let marked_at = Timestamp::from(self.time_provider.now());
        flag_for_delete(&mut *tx, partition_id, delete, marked_at).await?;

        update_compaction_level(&mut *tx, partition_id, upgrade, target_level).await?;

        let mut ids = Vec::with_capacity(create.len());
        for file in create {
            if file.partition_id != partition_id {
                return Err(Error::Malformed {
                    descr: format!("Inconsistent ParquetFileParams, expected PartitionId({partition_id}) got PartitionId({})", file.partition_id),
                });
            }
            let id = create_parquet_file(&mut *tx, partition_id, file).await?;
            ids.push(id);
        }

        tx.commit().await?;

        Ok(ids)
    }
}

// The following three functions are helpers to the create_upgrade_delete method.
// They are also used by the respective create/flag_for_delete/update_compaction_level methods.
async fn create_parquet_file<'q, E>(
    executor: E,
    partition_id: PartitionId,
    parquet_file_params: &ParquetFileParams,
) -> Result<ParquetFileId>
where
    E: Executor<'q, Database = Postgres>,
{
    let ParquetFileParams {
        namespace_id,
        table_id,
        partition_id: _,
        partition_hash_id,
        object_store_id,
        min_time,
        max_time,
        file_size_bytes,
        row_count,
        compaction_level,
        created_at,
        column_set,
        max_l0_created_at,
        source,
    } = parquet_file_params;

    let query = sqlx::query_scalar::<_, ParquetFileId>(
        r#"
INSERT INTO parquet_file (
    table_id, partition_id, partition_hash_id, object_store_id,
    min_time, max_time, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at, source )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING id;
        "#,
    )
    .bind(table_id) // $1
    .bind(partition_id) // $2
    .bind(partition_hash_id.as_ref()) // $3
    .bind(object_store_id) // $4
    .bind(min_time) // $5
    .bind(max_time) // $6
    .bind(file_size_bytes) // $7
    .bind(row_count) // $8
    .bind(compaction_level) // $9
    .bind(created_at) // $10
    .bind(namespace_id) // $11
    .bind(column_set) // $12
    .bind(max_l0_created_at) // $13
    .bind(source); // $14

    let parquet_file_id = query.fetch_one(executor).await.map_err(|e| {
        if is_unique_violation(&e) {
            Error::AlreadyExists {
                descr: object_store_id.to_string(),
            }
        } else if is_fk_violation(&e) {
            Error::NotFound {
                descr: e.to_string(),
            }
        } else {
            Error::External {
                source: Arc::new(e),
            }
        }
    })?;

    Ok(parquet_file_id)
}

async fn flag_for_delete<'q, E>(
    executor: E,
    partition_id: PartitionId,
    ids: &[ObjectStoreId],
    marked_at: Timestamp,
) -> Result<()>
where
    E: Executor<'q, Database = Postgres>,
{
    let updated =
        sqlx::query_as::<_, (i64,)>(r#"UPDATE parquet_file SET to_delete = $1 WHERE object_store_id = ANY($2) AND partition_id = $3 AND to_delete is NULL RETURNING id;"#)
            .bind(marked_at) // $1
            .bind(ids) // $2
            .bind(partition_id) // $3
            .fetch_all(executor)
            .await?;

    if updated.len() != ids.len() {
        return Err(Error::NotFound {
            descr: "parquet file(s) not found for delete".to_string(),
        });
    }

    Ok(())
}

async fn update_compaction_level<'q, E>(
    executor: E,
    partition_id: PartitionId,
    parquet_file_ids: &[ObjectStoreId],
    compaction_level: CompactionLevel,
) -> Result<()>
where
    E: Executor<'q, Database = Postgres>,
{
    let updated = sqlx::query_as::<_, (i64,)>(
        r#"
UPDATE parquet_file
SET compaction_level = $1
WHERE object_store_id = ANY($2) AND partition_id = $3 AND to_delete is NULL RETURNING id;
        "#,
    )
    .bind(compaction_level) // $1
    .bind(parquet_file_ids) // $2
    .bind(partition_id) // $3
    .fetch_all(executor)
    .await?;

    if updated.len() != parquet_file_ids.len() {
        return Err(Error::NotFound {
            descr: "parquet file(s) not found for upgrade".to_string(),
        });
    }

    Ok(())
}

/// The error code returned by Postgres for a unique constraint violation.
///
/// See <https://www.postgresql.org/docs/9.2/errcodes-appendix.html>
const PG_UNIQUE_VIOLATION: &str = "23505";

/// Returns true if `e` is a unique constraint violation error.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_UNIQUE_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Error code returned by Postgres for a foreign key constraint violation.
const PG_FK_VIOLATION: &str = "23503";

fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_FK_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Test helpers postgres testing.
#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use rand::Rng;
    use sqlx::migrate::MigrateDatabase;

    pub(crate) const TEST_DSN_ENV: &str = "TEST_INFLUXDB_IOX_CATALOG_DSN";

    /// Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
    /// variables are not set.
    macro_rules! maybe_skip_integration {
        ($panic_msg:expr) => {{
            dotenvy::dotenv().ok();

            let required_vars = [crate::postgres::test_utils::TEST_DSN_ENV];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match std::env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping Postgres integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );

                let panic_msg: &'static str = $panic_msg;
                if !panic_msg.is_empty() {
                    panic!("{}", panic_msg);
                }

                return;
            }
        }};
        () => {
            maybe_skip_integration!("")
        };
    }

    pub(crate) use maybe_skip_integration;

    pub(crate) async fn create_db(dsn: &str) {
        // Create the catalog database if it doesn't exist
        if !Postgres::database_exists(dsn).await.unwrap() {
            // Ignore failure if another test has already created the database
            let _ = Postgres::create_database(dsn).await;
        }
    }

    pub(crate) async fn setup_db_no_migration() -> PostgresCatalog {
        setup_db_no_migration_with_app_name("test").await
    }

    pub(crate) async fn setup_db_no_migration_with_app_name(
        app_name: &'static str,
    ) -> PostgresCatalog {
        setup_db_no_migration_with_overrides(|opts| PostgresConnectionOptions {
            app_name: app_name.to_string(),
            ..opts
        })
        .await
    }

    pub(crate) async fn setup_db_no_migration_with_overrides(
        f: impl FnOnce(PostgresConnectionOptions) -> PostgresConnectionOptions + Send,
    ) -> PostgresCatalog {
        // create a random schema for this particular pool
        let schema_name = {
            // use scope to make it clear to clippy / rust that `rng` is
            // not carried past await points
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(rand::distributions::Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
                .to_ascii_lowercase()
        };
        info!(schema_name, "test schema");

        let metrics = Arc::new(metric::Registry::default());
        let dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();

        create_db(&dsn).await;

        let options = f(PostgresConnectionOptions {
            schema_name: schema_name.clone(),
            dsn,
            max_conns: 3,
            ..Default::default()
        });
        let pg = PostgresCatalog::connect(options, metrics)
            .await
            .expect("failed to connect catalog");

        // Create the test schema
        pg.pool
            .execute(format!("CREATE SCHEMA {schema_name};").as_str())
            .await
            .expect("failed to create test schema");

        // Ensure the test user has permission to interact with the test schema.
        pg.pool
            .execute(
                format!(
                    "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                )
                .as_str(),
            )
            .await
            .expect("failed to grant privileges to schema");

        pg
    }

    pub(crate) async fn setup_db() -> PostgresCatalog {
        let pg = setup_db_no_migration().await;
        // Run the migrations against this random schema.
        pg.setup().await.expect("failed to initialise database");
        pg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::test_utils::setup_db_no_migration_with_overrides;
    use crate::{
        interface::ParquetFileRepoExt,
        postgres::test_utils::{
            create_db, maybe_skip_integration, setup_db, setup_db_no_migration,
            setup_db_no_migration_with_app_name,
        },
        test_helpers::{arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table},
    };
    use assert_matches::assert_matches;
    use data_types::partition_template::TemplatePart;
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use metric::{Observation, RawReporter};
    use std::{io::Write, ops::Deref, sync::Arc, time::Instant};
    use tempfile::NamedTempFile;
    use test_helpers::{assert_contains, maybe_start_logging};

    /// Small no-op test just to print out the migrations.
    ///
    /// This is helpful to look up migration checksums and debug parsing of the migration files.
    #[test]
    fn print_migrations() {
        println!("{:#?}", MIGRATOR.deref());
    }

    #[tokio::test]
    async fn test_migration() {
        maybe_skip_integration!();
        maybe_start_logging();

        let postgres = setup_db_no_migration().await;

        // 1st setup
        postgres.setup().await.unwrap();

        // 2nd setup
        postgres.setup().await.unwrap();
    }

    #[tokio::test]
    async fn test_migration_generic() {
        use crate::migrate::test_utils::test_migration;

        maybe_skip_integration!();
        maybe_start_logging();

        test_migration::<_, _, HotSwapPool<Postgres>>(&MIGRATOR, || async {
            setup_db_no_migration().await.into_pool()
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_catalog() {
        maybe_skip_integration!();
        maybe_start_logging();

        let postgres = setup_db().await;

        // Validate the connection time zone is the expected UTC value.
        let tz: String = sqlx::query_scalar("SHOW TIME ZONE;")
            .fetch_one(&postgres.pool)
            .await
            .expect("read time zone");
        assert_eq!(tz, "UTC");

        let pool = postgres.pool.clone();
        let schema_name = postgres.schema_name().to_string();

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        crate::interface_tests::test_catalog(|| async {
            // Clean the schema.
            pool
                .execute(format!("DROP SCHEMA {schema_name} CASCADE").as_str())
                .await
                .expect("failed to clean schema between tests");

            // Recreate the test schema
            pool
                .execute(format!("CREATE SCHEMA {schema_name};").as_str())
                .await
                .expect("failed to create test schema");

            // Ensure the test user has permission to interact with the test schema.
            pool
                .execute(
                    format!(
                        "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                    )
                    .as_str(),
                )
                .await
                .expect("failed to grant privileges to schema");

            // Run the migrations against this random schema.
            postgres.setup().await.expect("failed to initialise database");

            Arc::clone(&postgres)
        })
        .await;
    }

    #[tokio::test]
    async fn existing_partitions_without_hash_id() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories();

        let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
        let table = arbitrary_table(&mut *repos, "table", &namespace).await;
        let table_id = table.id;
        let key = PartitionKey::from("francis-scott-key-key");

        // Create a partition record in the database that has `NULL` for its `hash_id`
        // value, which is what records existing before the migration adding that column will have.
        sqlx::query(
            r#"
INSERT INTO partition
    (partition_key, table_id, sort_key_ids)
VALUES
    ( $1, $2, '{}')
ON CONFLICT ON CONSTRAINT partition_key_unique
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at;
        "#,
        )
        .bind(&key) // $1
        .bind(table_id) // $2
        .fetch_one(&pool)
        .await
        .unwrap();

        // Check that the hash_id being null in the database doesn't break querying for partitions.
        let table_partitions = repos.partitions().list_by_table_id(table_id).await.unwrap();
        assert_eq!(table_partitions.len(), 1);
        let partition = &table_partitions[0];
        assert!(partition.hash_id().is_none());

        // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent
        // and that the hash_id still doesn't get set.
        let inserted_again = repos
            .partitions()
            .create_or_get(key, table_id)
            .await
            .expect("idempotent write should succeed");

        // Test: sort_key_ids from freshly insert with empty value
        assert!(inserted_again.sort_key_ids().is_none());

        assert_eq!(partition, &inserted_again);

        // Create a Parquet file record in this partition to ensure we don't break new data
        // ingestion for old-style partitions
        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, partition);
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();
        assert_eq!(parquet_file.partition_hash_id, None);

        // Add a partition record WITH a hash ID
        repos
            .partitions()
            .create_or_get(PartitionKey::from("Something else"), table_id)
            .await
            .unwrap();

        // Ensure we can list only the old-style partitions
        let old_style_partitions = repos.partitions().list_old_style().await.unwrap();
        assert_eq!(old_style_partitions.len(), 1);
        assert_eq!(old_style_partitions[0].id, partition.id);
    }

    #[test]
    fn test_parse_dsn_file() {
        assert_eq!(
            get_dsn_file_path("dsn-file:///tmp/my foo.txt"),
            Some("/tmp/my foo.txt".to_owned()),
        );
        assert_eq!(get_dsn_file_path("dsn-file:blah"), None,);
        assert_eq!(get_dsn_file_path("postgres://user:pw@host/db"), None,);
    }

    #[tokio::test]
    async fn test_reload() {
        maybe_skip_integration!();
        maybe_start_logging();

        const POLLING_INTERVAL: Duration = Duration::from_millis(10);

        // fetch dsn from envvar
        let test_dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();
        create_db(&test_dsn).await;
        println!("TEST_DSN={test_dsn}");

        // create a temp file to store the initial dsn
        let mut dsn_file = NamedTempFile::new().expect("create temp file");
        dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");

        const TEST_APPLICATION_NAME: &str = "test_application_name";
        let dsn_good = format!("dsn-file://{}", dsn_file.path().display());
        println!("dsn_good={dsn_good}");

        // create a hot swap pool with test application name and dsn file pointing to tmp file.
        // we will later update this file and the pool should be replaced.
        let options = PostgresConnectionOptions {
            app_name: TEST_APPLICATION_NAME.to_owned(),
            schema_name: String::from("test"),
            dsn: dsn_good,
            max_conns: 3,
            hotswap_poll_interval: POLLING_INTERVAL,
            ..Default::default()
        };
        let metrics = Arc::new(metric::Registry::new());
        let (pool, _refresh_task) = new_pool(&options, metrics).await.expect("connect");
        println!("got a pool");

        // ensure the application name is set as expected
        let application_name: String =
            sqlx::query_scalar("SELECT current_setting('application_name') as application_name;")
                .fetch_one(&pool)
                .await
                .expect("read application_name");
        assert_eq!(application_name, TEST_APPLICATION_NAME);

        // pool for long running queries
        let mut long_running_queries = JoinSet::new();

        // try refreshing twice
        for i in 0..2 {
            // try to block update
            create_long_running_query(&pool, &mut long_running_queries).await;
            create_long_running_query(&pool, &mut long_running_queries).await;

            // create a new temp file object with updated dsn and overwrite the previous tmp file
            let test_application_name_new = format!("changed_application_name_{i}");
            let mut new_dsn_file = NamedTempFile::new().expect("create temp file");
            new_dsn_file
                .write_all(test_dsn.as_bytes())
                .expect("write temp file");
            new_dsn_file
                .write_all(format!("?application_name={test_application_name_new}").as_bytes())
                .expect("write temp file");
            new_dsn_file
                .persist(dsn_file.path())
                .expect("overwrite new dsn file");

            // wait until the hotswap machinery has reloaded the updated DSN file and
            // successfully performed a new connection with the new DSN.
            let mut application_name = "".to_string();
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(5)
                && application_name != test_application_name_new
            {
                tokio::time::sleep(POLLING_INTERVAL).await;

                application_name = sqlx::query_scalar(
                    "SELECT current_setting('application_name') as application_name;",
                )
                .fetch_one(&pool)
                .await
                .expect("read application_name");
            }
            assert_eq!(application_name, test_application_name_new);
        }

        long_running_queries.shutdown().await;
    }

    async fn create_long_running_query(pool: &HotSwapPool<Postgres>, tasks: &mut JoinSet<()>) {
        let pool = pool.clone();
        tasks.spawn(async move {
            sqlx::query("SELECT pg_sleep(10);")
                .execute(&pool)
                .await
                .expect("query works");
        });

        // give query some time to get started
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    #[tokio::test]
    async fn test_billing_summary_on_parqet_file_creation() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories();
        let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
        let table = arbitrary_table(&mut *repos, "table", &namespace).await;
        let key = "bananas";
        let partition = repos
            .partitions()
            .create_or_get(key.into(), table.id)
            .await
            .unwrap();

        // parquet file to create- all we care about here is the size
        let mut p1 = arbitrary_parquet_file_params(&namespace, &table, &partition);
        p1.file_size_bytes = 1337;
        let f1 = repos.parquet_files().create(p1.clone()).await.unwrap();
        // insert the same again with a different size; we should then have 3x1337 as total file
        // size
        p1.object_store_id = ObjectStoreId::new();
        p1.file_size_bytes *= 2;
        let _f2 = repos
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");

        // after adding two files we should have 3x1337 in the summary
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 3);

        // flag f1 for deletion and assert that the total file size is reduced accordingly.
        repos
            .parquet_files()
            .create_upgrade_delete(
                partition.id,
                &[f1.object_store_id],
                &[],
                &[],
                CompactionLevel::Initial,
            )
            .await
            .expect("flag parquet file for deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        // we marked the first file of size 1337 for deletion leaving only the second that was 2x
        // that
        assert_eq!(total_file_size_bytes, 1337 * 2);

        // actually deleting shouldn't change the total
        let older_than = p1.created_at + 1;
        repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .expect("parquet file deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 2);
    }

    #[tokio::test]
    async fn namespace_partition_template_null_is_the_default_in_the_database() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories();

        let namespace_name = "apples";

        // Create a namespace record in the database that has `NULL` for its `partition_template`
        // value, which is what records existing before the migration adding that column will have.
        let insert_null_partition_template_namespace = sqlx::query(
            r#"
INSERT INTO namespace (
    name, retention_period_ns, partition_template
)
VALUES ( $1, $2, NULL )
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
            "#,
        )
        .bind(namespace_name) // $1
        .bind(None::<Option<i64>>); // $2

        insert_null_partition_template_namespace
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_namespace = repos
            .namespaces()
            .get_by_name(namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .unwrap();
        // When fetching this namespace from the database, the `FromRow` impl should set its
        // `partition_template` to the default.
        assert_eq!(
            lookup_namespace.partition_template,
            NamespacePartitionTemplateOverride::default()
        );

        // When creating a namespace through the catalog functions without specifying a custom
        // partition template,
        let created_without_custom_template = repos
            .namespaces()
            .create(
                &"lemons".try_into().unwrap(),
                None, // no partition template
                None,
                None,
            )
            .await
            .unwrap();

        // it should have the default template in the application,
        assert_eq!(
            created_without_custom_template.partition_template,
            NamespacePartitionTemplateOverride::default()
        );

        // and store NULL in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM namespace WHERE id = $1;")
            .bind(created_without_custom_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(created_without_custom_template.name, name);
        let partition_template: Option<NamespacePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert!(partition_template.is_none());

        // When explicitly setting a template that happens to be equal to the application default,
        // assume it's important that it's being specially requested and store it rather than NULL.
        let namespace_custom_template_name = "kumquats";
        let custom_partition_template_equal_to_default =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat(
                        "%Y-%m-%d".to_owned(),
                    )),
                }],
            })
            .unwrap();
        let namespace_custom_template = repos
            .namespaces()
            .create(
                &namespace_custom_template_name.try_into().unwrap(),
                Some(custom_partition_template_equal_to_default.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            namespace_custom_template.partition_template,
            custom_partition_template_equal_to_default
        );
        let record = sqlx::query("SELECT name, partition_template FROM namespace WHERE id = $1;")
            .bind(namespace_custom_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(namespace_custom_template.name, name);
        let partition_template: Option<NamespacePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert_eq!(
            partition_template.unwrap(),
            custom_partition_template_equal_to_default
        );
    }

    #[tokio::test]
    async fn table_partition_template_null_is_the_default_in_the_database() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories();

        let namespace_default_template_name = "oranges";
        let namespace_default_template = repos
            .namespaces()
            .create(
                &namespace_default_template_name.try_into().unwrap(),
                None, // no partition template
                None,
                None,
            )
            .await
            .unwrap();

        let namespace_custom_template_name = "limes";
        let namespace_custom_template = repos
            .namespaces()
            .create(
                &namespace_custom_template_name.try_into().unwrap(),
                Some(
                    NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                        parts: vec![proto::TemplatePart {
                            part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                        }],
                    })
                    .unwrap(),
                ),
                None,
                None,
            )
            .await
            .unwrap();

        // In a namespace that also has a NULL template, create a table record in the database that
        // has `NULL` for its `partition_template` value, which is what records existing before the
        // migration adding that column will have.
        let table_name = "null_template";
        let insert_null_partition_template_table = sqlx::query(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
VALUES ( $1, $2, NULL )
RETURNING *;
            "#,
        )
        .bind(table_name) // $1
        .bind(namespace_default_template.id); // $2

        insert_null_partition_template_table
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_table = repos
            .tables()
            .get_by_namespace_and_name(namespace_default_template.id, table_name)
            .await
            .unwrap()
            .unwrap();
        // When fetching this table from the database, the `FromRow` impl should set its
        // `partition_template` to the system default (because the namespace didn't have a template
        // either).
        assert_eq!(
            lookup_table.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // In a namespace that has a custom template, create a table record in the database that
        // has `NULL` for its `partition_template` value.
        //
        // THIS ACTUALLY SHOULD BE IMPOSSIBLE because:
        //
        // * Namespaces have to exist before tables
        // * `partition_tables` are immutable on both namespaces and tables
        // * When the migration adding the `partition_table` column is deployed, namespaces can
        //   begin to be created with `partition_templates`
        // * *Then* tables can be created with `partition_templates` or not
        // * When tables don't get a custom table partition template but their namespace has one,
        //   their database record will get the namespace partition template.
        //
        // In other words, table `partition_template` values in the database is allowed to possibly
        // be `NULL` IFF their namespace's `partition_template` is `NULL`.
        //
        // That said, this test creates this hopefully-impossible scenario to ensure that the
        // defined, expected behavior if a table record somehow exists in the database with a `NULL`
        // `partition_template` value is that it will have the application default partition
        // template *even if the namespace `partition_template` is not null*.
        let table_name = "null_template";
        let insert_null_partition_template_table = sqlx::query(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
VALUES ( $1, $2, NULL )
RETURNING *;
            "#,
        )
        .bind(table_name) // $1
        .bind(namespace_custom_template.id); // $2

        insert_null_partition_template_table
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_table = repos
            .tables()
            .get_by_namespace_and_name(namespace_custom_template.id, table_name)
            .await
            .unwrap()
            .unwrap();
        // When fetching this table from the database, the `FromRow` impl should set its
        // `partition_template` to the system default *even though the namespace has a
        // template*, because this should be impossible as detailed above.
        assert_eq!(
            lookup_table.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // # Table template false, namespace template true
        //
        // When creating a table through the catalog functions *without* a custom table template in
        // a namespace *with* a custom partition template,
        let table_no_template_with_namespace_template = repos
            .tables()
            .create(
                "pomelo",
                TablePartitionTemplateOverride::try_from_existing(
                    None, // no custom partition template
                    &namespace_custom_template.partition_template,
                )
                .unwrap(),
                namespace_custom_template.id,
            )
            .await
            .unwrap();

        // it should have the namespace's template
        assert_eq!(
            table_no_template_with_namespace_template.partition_template,
            TablePartitionTemplateOverride::try_from_existing(
                None,
                &namespace_custom_template.partition_template
            )
            .unwrap()
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_no_template_with_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_no_template_with_namespace_template.name, name);
        let partition_template: Option<TablePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert_eq!(
            partition_template.unwrap(),
            TablePartitionTemplateOverride::try_from_existing(
                None,
                &namespace_custom_template.partition_template
            )
            .unwrap()
        );

        // # Table template true, namespace template false
        //
        // When creating a table through the catalog functions *with* a custom table template in
        // a namespace *without* a custom partition template,
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("chemical".into())),
            }],
        };
        let table_with_template_no_namespace_template = repos
            .tables()
            .create(
                "tangerine",
                TablePartitionTemplateOverride::try_from_existing(
                    Some(custom_table_template), // with custom partition template
                    &namespace_default_template.partition_template,
                )
                .unwrap(),
                namespace_default_template.id,
            )
            .await
            .unwrap();

        // it should have the custom table template
        let table_template_parts: Vec<_> = table_with_template_no_namespace_template
            .partition_template
            .parts()
            .collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "chemical"
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_with_template_no_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_with_template_no_namespace_template.name, name);
        let partition_template = record
            .try_get::<Option<TablePartitionTemplateOverride>, _>("partition_template")
            .unwrap()
            .unwrap();
        let table_template_parts: Vec<_> = partition_template.parts().collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "chemical"
        );

        // # Table template true, namespace template true
        //
        // When creating a table through the catalog functions *with* a custom table template in
        // a namespace *with* a custom partition template,
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("vegetable".into())),
            }],
        };
        let table_with_template_with_namespace_template = repos
            .tables()
            .create(
                "nectarine",
                TablePartitionTemplateOverride::try_from_existing(
                    Some(custom_table_template), // with custom partition template
                    &namespace_custom_template.partition_template,
                )
                .unwrap(),
                namespace_custom_template.id,
            )
            .await
            .unwrap();

        // it should have the custom table template
        let table_template_parts: Vec<_> = table_with_template_with_namespace_template
            .partition_template
            .parts()
            .collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "vegetable"
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_with_template_with_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_with_template_with_namespace_template.name, name);
        let partition_template = record
            .try_get::<Option<TablePartitionTemplateOverride>, _>("partition_template")
            .unwrap()
            .unwrap();
        let table_template_parts: Vec<_> = partition_template.parts().collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "vegetable"
        );

        // # Table template false, namespace template false
        //
        // When creating a table through the catalog functions *without* a custom table template in
        // a namespace *without* a custom partition template,
        let table_no_template_no_namespace_template = repos
            .tables()
            .create(
                "grapefruit",
                TablePartitionTemplateOverride::try_from_existing(
                    None, // no custom partition template
                    &namespace_default_template.partition_template,
                )
                .unwrap(),
                namespace_default_template.id,
            )
            .await
            .unwrap();

        // it should have the default template in the application,
        assert_eq!(
            table_no_template_no_namespace_template.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // and store NULL in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_no_template_no_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_no_template_no_namespace_template.name, name);
        let partition_template: Option<TablePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert!(partition_template.is_none());
    }

    #[tokio::test]
    async fn test_cold_compact() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories();
        let namespace = arbitrary_namespace(&mut *repos, "ns5").await;
        let table = arbitrary_table(&mut *repos, "table", &namespace).await;
        let key = "NoBananas";
        let partition = repos
            .partitions()
            .create_or_get(key.into(), table.id)
            .await
            .unwrap();

        // Set the new_file_at time to be 1000, so we can test queries for cold partitions before & after
        let time: i64 = 1000;
        sqlx::query(
            r#"
UPDATE partition
SET new_file_at = $3
WHERE partition_key = $1
AND table_id = $2
RETURNING id, hash_id, table_id, partition_key, sort_key_ids, new_file_at, cold_compact_at;
        "#,
        )
        .bind(key) // $1
        .bind(table.id) // $2
        .bind(time) // $3
        .fetch_one(&pool)
        .await
        .unwrap();

        // Getting partitions colder than `partition`'s time should find none.
        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time - 1), 1)
            .await
            .unwrap();

        assert_eq!(need_cold.len(), 0);

        // Getting cold partitions up to `partition`'s coldness should find it
        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time), 1)
            .await
            .unwrap();
        assert_eq!(need_cold.len(), 1);
        assert_eq!(need_cold[0], partition.id);

        // Getting cold including warmer than `partition`'s coldness should find it
        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time + 1), 1)
            .await
            .unwrap();
        assert_eq!(need_cold.len(), 1);
        assert_eq!(need_cold[0], partition.id);

        // Give `partition` a cold_compact_at time that's invalid (older than its new_file_at)
        repos
            .partitions()
            .update_cold_compact(partition.id, Timestamp::new(time - 1))
            .await
            .unwrap();

        // Getting cold partitions up to `partition`'s age still find it when it has an invalid cold compact time.
        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time), 1)
            .await
            .unwrap();
        assert_eq!(need_cold.len(), 1);
        assert_eq!(need_cold[0], partition.id);

        // Give `partition` a cold_compact_at time that's valid (newer than new_file_at)
        repos
            .partitions()
            .update_cold_compact(partition.id, Timestamp::new(time + 1))
            .await
            .unwrap();

        // With a valid cold compact time, `partition` isn't returned.
        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time), 1)
            .await
            .unwrap();
        assert_eq!(need_cold.len(), 0);

        // Now mark need_cold[0] as skipped and ensure it's not returned
        let reason = "test";
        repos
            .partitions()
            .record_skipped_compaction(partition.id, reason, 0, 0, 0, 0, 0)
            .await
            .unwrap();

        let need_cold = repos
            .partitions()
            .partitions_needing_cold_compact(Timestamp::new(time), 1)
            .await
            .unwrap();
        assert_eq!(need_cold.len(), 0);
    }

    #[tokio::test]
    async fn test_metrics() {
        maybe_skip_integration!();

        let postgres = setup_db_no_migration().await;

        let mut reporter = RawReporter::default();
        postgres.metrics().report(&mut reporter);
        assert_eq!(
            reporter
                .metric("sqlx_postgres_connections")
                .unwrap()
                .observation(&[("pool_id", "0"), ("state", "min")])
                .unwrap(),
            &Observation::U64Gauge(1),
        );
        assert_eq!(
            reporter
                .metric("sqlx_postgres_connections")
                .unwrap()
                .observation(&[("pool_id", "0"), ("state", "max")])
                .unwrap(),
            &Observation::U64Gauge(3),
        );
    }

    #[tokio::test]
    async fn test_active_applications() {
        maybe_skip_integration!();

        const APP_1: &str = "test_other_apps_1";
        const APP_2: &str = "test_other_apps_2";

        let c1 = setup_db_no_migration_with_app_name(APP_1).await;
        let _c2 = setup_db_no_migration_with_app_name(APP_2).await;

        let apps = c1.active_applications().await.unwrap();
        assert!(apps.contains(APP_1));
        assert!(apps.contains(APP_2));
    }

    #[tokio::test]
    async fn test_timeout() {
        maybe_skip_integration!();

        let c = setup_db_no_migration_with_overrides(|opts| PostgresConnectionOptions {
            statement_timeout: Duration::from_millis(1),
            ..opts
        })
        .await;

        let err = c
            .pool
            .execute(sqlx::query("select PG_SLEEP(1)"))
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(err, "canceling statement due to statement timeout");
    }
}
