//! CLI handling for object store config (via CLI arguments and environment variables).

use async_trait::async_trait;
use non_empty_string::NonEmptyString;
use object_store::{
    DynObjectStore,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use object_store_size_hinting::ObjectStoreStripSizeHinting;
use observability_deps::tracing::{info, warn};
use snafu::{ResultExt, Snafu};
use std::{
    convert::Infallible, fs, num::NonZeroUsize, path::PathBuf, pin::Pin, sync::Arc, time::Duration,
};
use url::Url;

use aws_config::{BehaviorVersion, load_defaults};
use aws_sdk_s3::client::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::config::Region;
use aws_sdk_sts::config::ProvideCredentials;

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum ParseError {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create local store {:?}: {}", path, source))]
    CreateLocalFileSystem {
        path: PathBuf,
        source: object_store::Error,
    },

    #[snafu(display(
        "Specified {:?} for the object store, required configuration missing for {}",
        object_store,
        missing
    ))]
    MissingObjectStoreConfig {
        object_store: ObjectStoreType,
        missing: String,
    },

    #[snafu(display("Object store not specified"))]
    UnspecifiedObjectStore,

    // Creating a new S3 object store can fail if the region is *specified* but
    // not *parseable* as a rusoto `Region`. The other object store constructors
    // don't return `Result`.
    #[snafu(display("Error configuring Amazon S3: {}", source))]
    InvalidS3Config { source: object_store::Error },

    #[snafu(display("Error configuring GCS: {}", source))]
    InvalidGCSConfig { source: object_store::Error },

    #[snafu(display("Error configuring Microsoft Azure: {}", source))]
    InvalidAzureConfig { source: object_store::Error },
}

/// The AWS region to use for Amazon S3 based object storage if none is
/// specified.
pub const FALLBACK_AWS_REGION: &str = "us-east-1";

/// A `clap` `value_parser` which returns `None` when given an empty string and
/// `Some(NonEmptyString)` otherwise.
fn parse_optional_string(s: &str) -> Result<Option<NonEmptyString>, Infallible> {
    Ok(NonEmptyString::new(s.to_string()).ok())
}

/// Endpoint for S3 & Co.
///
/// This is a [`Url`] without a trailing slash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint(String);

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Endpoint> for String {
    fn from(value: Endpoint) -> Self {
        value.0
    }
}

impl std::str::FromStr for Endpoint {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // try to parse it
        Url::parse(s)?;

        // strip trailing slash
        let s = s.strip_suffix("/").unwrap_or(s);

        Ok(Self(s.to_owned()))
    }
}

/// Creation of an `ObjectStoreConfig` struct for `clap` argument handling.
///
/// This allows for multiple object store configurations to be produced when
/// needed, denoted by a particular use-case prefix.
macro_rules! object_store_config {
    ($prefix:expr) => {
        object_store_config_inner!($prefix);
    };
    () => {
        // Creates the original ObjectStoreConfig to maintain backwards
        // compatibility.
        object_store_config_inner!("_");
    };
}

/// Helper macro to generate the relevant name, used by the ID/long attributes
/// for `clap`.
macro_rules! gen_name {
    ($prefix:expr, $name:expr) => {
        paste::paste! {
            if $prefix == "_" {
                $name
            } else {
                concat!(stringify!([<$prefix:lower>]), "-", $name)
            }
        }
    };
}

/// Helper macro to generate the appropriate environment variable, used by the
/// env attribute for `clap`.
macro_rules! gen_env {
    ($prefix:expr, $name:expr) => {
        paste::paste! {
            if $prefix == "_" {
                $name
            } else {
                concat!(stringify!([<$prefix:upper>]), "_", $name)
            }
        }
    };
}

macro_rules! object_store_config_inner {
    ($prefix:expr) => {
        paste::paste! {
            /// CLI config for object stores.
            #[derive(Debug, Clone, clap::Parser)]
            pub struct [<$prefix:camel ObjectStoreConfig>] {
                /// Which object storage to use. If not specified, defaults to memory.
                ///
                /// Possible values (case insensitive):
                ///
                /// * memory (default): Effectively no object persistence.
                /// * memorythrottled: Like `memory` but with latency and throughput that somewhat resamble a cloud
                ///   object store. Useful for testing and benchmarking.
                /// * file: Stores objects in the local filesystem. Must also set `--data-dir`.
                /// * s3: Amazon S3. Must also set `--bucket`, `--aws-access-key-id`, `--aws-secret-access-key`, and
                ///   possibly `--aws-default-region`.
                /// * google: Google Cloud Storage. Must also set `--bucket` and `--google-service-account`.
                /// * azure: Microsoft Azure blob storage. Must also set `--bucket`, `--azure-storage-account`,
                ///   and `--azure-storage-access-key`.
                #[clap(
                    value_enum,
                    id = gen_name!($prefix, "object-store"),
                    long = gen_name!($prefix, "object-store"),
                    env = gen_env!($prefix, "INFLUXDB_IOX_OBJECT_STORE"),
                    ignore_case = true,
                    action,
                    verbatim_doc_comment
                )]
                pub object_store: Option<ObjectStoreType>,

                /// Name of the bucket to use for the object store. Must also set
                /// `--object-store` to a cloud object storage to have any effect.
                ///
                /// If using Google Cloud Storage for the object store, this item as well
                /// as `--google-service-account` must be set.
                ///
                /// If using S3 for the object store, must set this item as well
                /// as `--aws-access-key-id` and `--aws-secret-access-key`. Can also set
                /// `--aws-default-region` if not using the fallback region.
                ///
                /// If using Azure for the object store, set this item to the name of a
                /// container you've created in the associated storage account, under
                /// Blob Service > Containers. Must also set `--azure-storage-account` and
                /// `--azure-storage-access-key`.
                #[clap(
                    id = gen_name!($prefix, "bucket"),
                    long = gen_name!($prefix, "bucket"),
                    env = gen_env!($prefix, "INFLUXDB_IOX_BUCKET"),
                    action
                )]
                pub bucket: Option<String>,

                /// The location InfluxDB IOx will use to store files locally.
                #[clap(
                    id = gen_name!($prefix, "data-dir"),
                    long = gen_name!($prefix, "data-dir"),
                    env = gen_env!($prefix, "INFLUXDB_IOX_DB_DIR"),
                    action
                )]
                pub database_directory: Option<PathBuf>,

                /// When using Amazon S3 as the object store, set this to an access key that
                /// has permission to read from and write to the specified S3 bucket.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, and
                /// `--aws-secret-access-key`. Can also set `--aws-default-region` if not
                /// using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                ///
                /// An empty string value is equivalent to omitting the flag.
                /// Note: must refer to std::option::Option explicitly, see <https://github.com/clap-rs/clap/issues/4626>
                #[clap(
                    id = gen_name!($prefix, "aws-access-key-id"),
                    long = gen_name!($prefix, "aws-access-key-id"),
                    env = gen_env!($prefix, "AWS_ACCESS_KEY_ID"),
                    value_parser = parse_optional_string,
                    default_value = "",
                    action
                )]
                pub aws_access_key_id: std::option::Option<NonEmptyString>,

                /// When using Amazon S3 as the object store, set this to the secret access
                /// key that goes with the specified access key ID.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`.
                /// Can also set `--aws-default-region` if not using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                ///
                /// An empty string value is equivalent to omitting the flag.
                /// Note: must refer to std::option::Option explicitly, see <https://github.com/clap-rs/clap/issues/4626>
                #[clap(
                    id = gen_name!($prefix, "aws-secret-access-key"),
                    long = gen_name!($prefix, "aws-secret-access-key"),
                    env = gen_env!($prefix, "AWS_SECRET_ACCESS_KEY"),
                    value_parser = parse_optional_string,
                    default_value = "",
                    action
                )]
                pub aws_secret_access_key: std::option::Option<NonEmptyString>,

                /// When using Amazon S3 as the object store, set this to the region
                /// that goes with the specified bucket if different from the fallback
                /// value.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`,
                /// and `--aws-secret-access-key`.
                #[clap(
                    id = gen_name!($prefix, "aws-default-region"),
                    long = gen_name!($prefix, "aws-default-region"),
                    env = gen_env!($prefix, "AWS_DEFAULT_REGION"),
                    default_value = FALLBACK_AWS_REGION,
                    action
                )]
                pub aws_default_region: String,

                /// When using Amazon S3 compatibility storage service, set this to the
                /// endpoint.
                ///
                /// Must also set `--object-store=s3`, `--bucket`. Can also set `--aws-default-region`
                /// if not using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "aws-endpoint"),
                    long = gen_name!($prefix, "aws-endpoint"),
                    env = gen_env!($prefix, "AWS_ENDPOINT"),
                    action
                )]
                pub aws_endpoint: Option<Endpoint>,

                /// When using Amazon S3 as an object store, set this to the session token. This is handy when using a federated
                /// login / SSO and you fetch credentials via the UI.
                ///
                /// Is it assumed that the session is valid as long as the IOx server is running.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "aws-session-token"),
                    long = gen_name!($prefix, "aws-session-token"),
                    env = gen_env!($prefix, "AWS_SESSION_TOKEN"),
                    action
                )]
                pub aws_session_token: Option<String>,

                /// Allow unencrypted HTTP connection to AWS.
                #[clap(
                    id = gen_name!($prefix, "aws-allow-http"),
                    long = gen_name!($prefix, "aws-allow-http"),
                    env = gen_env!($prefix, "AWS_ALLOW_HTTP"),
                    action
                )]
                pub aws_allow_http: bool,

                /// If enabled, S3 stores will not fetch credentials and will not sign requests.
                ///
                /// This can be useful when interacting with public S3 buckets that deny authorized requests or for when working
                /// with in-cluster proxies that handle the credentials already.
                #[clap(
                    id = gen_name!($prefix, "aws-skip-signature"),
                    long = gen_name!($prefix, "aws-skip-signature"),
                    env = gen_env!($prefix, "AWS_SKIP_SIGNATURE"),
                    action
                )]
                pub aws_skip_signature: bool,

                /// When using Google Cloud Storage as the object store, set this to the
                /// path to the JSON file that contains the Google credentials.
                ///
                /// Must also set `--object-store=google` and `--bucket`.
                #[clap(
                    id = gen_name!($prefix, "google-service-account"),
                    long = gen_name!($prefix, "google-service-account"),
                    env = gen_env!($prefix, "GOOGLE_SERVICE_ACCOUNT"),
                    action
                )]
                pub google_service_account: Option<String>,

                /// When using Microsoft Azure as the object store, set this to the
                /// name you see when going to All Services > Storage accounts > `[name]`.
                ///
                /// Must also set `--object-store=azure`, `--bucket`, and
                /// `--azure-storage-access-key`.
                #[clap(
                    id = gen_name!($prefix, "azure-storage-account"),
                    long = gen_name!($prefix, "azure-storage-account"),
                    env = gen_env!($prefix, "AZURE_STORAGE_ACCOUNT"),
                    action
                )]
                pub azure_storage_account: Option<String>,

                /// When using Microsoft Azure as the object store, set this to one of the
                /// Key values in the Storage account's Settings > Access keys.
                ///
                /// Must also set `--object-store=azure`, `--bucket`, and
                /// `--azure-storage-account`.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "azure-storage-access-key"),
                    long = gen_name!($prefix, "azure-storage-access-key"),
                    env = gen_env!($prefix, "AZURE_STORAGE_ACCESS_KEY"),
                    action
                )]
                pub azure_storage_access_key: Option<String>,

                /// When using a network-based object store, limit the number of connection to this value.
                #[clap(
                    id = gen_name!($prefix, "object-store-connection-limit"),
                    long = gen_name!($prefix, "object-store-connection-limit"),
                    env = gen_env!($prefix, "OBJECT_STORE_CONNECTION_LIMIT"),
                    default_value = "1024",
                    action
                )]
                pub object_store_connection_limit: NonZeroUsize,

                /// Force HTTP/2 connection to network-based object stores.
                ///
                /// This implies "prior knowledge" as per RFC7540 section 3.4.
                #[clap(
                    id = gen_name!($prefix, "object-store-http2-only"),
                    long = gen_name!($prefix, "object-store-http2-only"),
                    env = gen_env!($prefix, "OBJECT_STORE_HTTP2_ONLY"),
                    action
                )]
                pub http2_only: bool,

                /// Set max frame size (in bytes/octets) for HTTP/2 connection.
                ///
                /// If not set, this uses the `object_store`/`reqwest` default.
                ///
                /// Usually you want to set this as high as possible -- the maximum allowed by the standard is `2^24-1 = 16,777,215`.
                /// However under some circumstances (like buggy middleware or upstream providers getting unhappy), you may be
                /// required to pick something else.
                #[clap(
                    id = gen_name!($prefix, "object-store-http2-max-frame-size"),
                    long = gen_name!($prefix, "object-store-http2-max-frame-size"),
                    env = gen_env!($prefix, "OBJECT_STORE_HTTP2_MAX_FRAME_SIZE"),
                    action
                )]
                pub http2_max_frame_size: Option<u32>,

                /// The maximum number of times to retry a request
                ///
                /// Set to 0 to disable retries
                #[clap(
                    id = gen_name!($prefix, "object-store-max-retries"),
                    long = gen_name!($prefix, "object-store-max-retries"),
                    env = gen_env!($prefix, "OBJECT_STORE_MAX_RETRIES"),
                    action
                )]
                pub max_retries: Option<usize>,

                /// The maximum length of time from the initial request
                /// after which no further retries will be attempted
                ///
                /// This not only bounds the length of time before a server
                /// error will be surfaced to the application, but also bounds
                /// the length of time a request's credentials must remain valid.
                ///
                /// As requests are retried without renewing credentials or
                /// regenerating request payloads, this number should be kept
                /// below 5 minutes to avoid errors due to expired credentials
                /// and/or request payloads
                #[clap(
                    id = gen_name!($prefix, "object-store-retry-timeout"),
                    long = gen_name!($prefix, "object-store-retry-timeout"),
                    env = gen_env!($prefix, "OBJECT_STORE_RETRY_TIMEOUT"),
                    value_parser = humantime::parse_duration,
                    action
                )]
                pub retry_timeout: Option<Duration>,

                /// Set a request timeout
                ///
                /// The timeout is applied from when the request starts connecting until the
                /// response body has finished.
                ///
                /// `object_store` default is 30 seconds.
                #[clap(
                    id = gen_name!($prefix, "object-store-request-timeout"),
                    long = gen_name!($prefix, "object-store-request-timeout"),
                    env = gen_env!($prefix, "OBJECT_STORE_REQUEST_TIMEOUT"),
                    value_parser = humantime::parse_duration,
                    action
                )]
                pub request_timeout: Option<Duration>,

                /// Set a timeout for only the connect phase of a client.
                ///
                /// `object_store` default is 5 seconds.
                #[clap(
                    id = gen_name!($prefix, "object-store-connect-timeout"),
                    long = gen_name!($prefix, "object-store-connect-timeout"),
                    env = gen_env!($prefix, "OBJECT_STORE_CONNECT_TIMEOUT"),
                    value_parser = humantime::parse_duration,
                    action
                )]
                pub connect_timeout: Option<Duration>,

                /// Endpoint of an S3 compatible, HTTP/2 enabled object store cache.
                #[clap(
                    id = gen_name!($prefix, "object-store-cache-endpoint"),
                    long = gen_name!($prefix, "object-store-cache-endpoint"),
                    env = gen_env!($prefix, "OBJECT_STORE_CACHE_ENDPOINT"),
                    action
                )]
                pub cache_endpoint: Option<Endpoint>,
            }

            impl [<$prefix:camel ObjectStoreConfig>] {

                /// Create a new instance for all-in-one mode, only allowing some arguments.
                pub fn new(database_directory: Option<PathBuf>) -> Self {
                    match &database_directory {
                        Some(dir) => info!("Object store: File-based in `{}`", dir.display()),
                        None => info!("Object store: In-memory"),
                    }

                    let object_store = database_directory.as_ref().map(|_| ObjectStoreType::File);

                    Self {
                        aws_access_key_id: Default::default(),
                        aws_allow_http: Default::default(),
                        aws_default_region: Default::default(),
                        aws_endpoint: Default::default(),
                        aws_secret_access_key: Default::default(),
                        aws_session_token: Default::default(),
                        aws_skip_signature: Default::default(),
                        azure_storage_access_key: Default::default(),
                        azure_storage_account: Default::default(),
                        bucket: Default::default(),
                        database_directory,
                        google_service_account: Default::default(),
                        object_store,
                        object_store_connection_limit: NonZeroUsize::new(1024).unwrap(),
                        http2_only: Default::default(),
                        http2_max_frame_size: Default::default(),
                        max_retries: Default::default(),
                        retry_timeout: Default::default(),
                        request_timeout: Default::default(),
                        connect_timeout: Default::default(),
                        cache_endpoint: Default::default(),
                    }
                }

                fn client_options(&self) -> object_store::ClientOptions {
                    let mut options = object_store::ClientOptions::new();

                    if self.http2_only {
                        options = options.with_http2_only();
                    }
                    if let Some(sz) = self.http2_max_frame_size {
                        options = options.with_http2_max_frame_size(sz);
                    }
                    if let Some(t) = self.request_timeout {
                        options = options.with_timeout(t);
                    }
                    if let Some(t) = self.connect_timeout {
                        options = options.with_connect_timeout(t);
                    }

                    options
                }

                fn new_gcs(&self, bucket_override: Option<String>) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::gcp::GoogleCloudStorageBuilder;
                    use object_store::limit::LimitStore;

                    info!(bucket=?self.bucket, object_store_type="GCS", "Object Store");

                    let mut builder = GoogleCloudStorageBuilder::new().with_client_options(self.client_options()).with_retry(self.retry_config());

                    if let Some(bucket) = bucket_override {
                        builder = builder.with_bucket_name(bucket);
                    } else if let Some(bucket) = &self.bucket {
                        builder = builder.with_bucket_name(bucket);
                    }
                    if let Some(account) = &self.google_service_account {
                        builder = builder.with_service_account_path(account);
                    }

                    Ok(Arc::new(LimitStore::new(
                        builder.build().context(InvalidGCSConfigSnafu)?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                fn new_s3(&self, bucket_override: Option<String>) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::limit::LimitStore;

                    info!(
                        bucket=?self.bucket,
                        endpoint=?self.aws_endpoint,
                        object_store_type="S3",
                        "Object Store"
                    );

                    Ok(Arc::new(LimitStore::new(
                        self.build_s3(bucket_override)?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                fn retry_config(&self) -> object_store::RetryConfig {
                    let mut retry_config = object_store::RetryConfig::default();

                    if let Some(max_retries) = self.max_retries {
                        retry_config.max_retries = max_retries;
                    }

                    if let Some(retry_timeout) = self.retry_timeout {
                        retry_config.retry_timeout = retry_timeout;
                    }

                    retry_config
                }

                /// If further configuration of S3 is needed beyond what this module provides, use this function
                /// to create an [`object_store::aws::AmazonS3Builder`] and further customize, then call `.build()`
                /// directly.
                pub fn s3_builder(&self, bucket_override: Option<String>) -> object_store::aws::AmazonS3Builder {
                    use object_store::aws::AmazonS3Builder;

                    let mut builder = AmazonS3Builder::new()
                        .with_client_options(self.client_options())
                        .with_allow_http(self.aws_allow_http)
                        .with_region(&self.aws_default_region)
                        .with_retry(self.retry_config())
                        .with_skip_signature(self.aws_skip_signature)
                        .with_imdsv1_fallback();

                    if let Some(bucket) = bucket_override {
                        builder = builder.with_bucket_name(bucket);
                    } else if let Some(bucket) = &self.bucket {
                        builder = builder.with_bucket_name(bucket);
                    }
                    if let Some(key_id) = &self.aws_access_key_id {
                        builder = builder.with_access_key_id(key_id.get());
                    }
                    if let Some(token) = &self.aws_session_token {
                        builder = builder.with_token(token);
                    }
                    if let Some(secret) = &self.aws_secret_access_key {
                        builder = builder.with_secret_access_key(secret.get());
                    }
                    if let Some(endpoint) = &self.aws_endpoint {
                        builder = builder.with_endpoint(endpoint.clone());
                    }

                    builder
                }

                fn build_s3(&self, bucket_override: Option<String>) -> Result<object_store::aws::AmazonS3, ParseError> {
                    let builder = self.s3_builder(bucket_override);

                    builder.build().context(InvalidS3ConfigSnafu)
                }

                fn new_azure(&self, bucket_override: Option<String>) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::azure::MicrosoftAzureBuilder;
                    use object_store::limit::LimitStore;

                    info!(bucket=?self.bucket, account=?self.azure_storage_account,
                          object_store_type="Azure", "Object Store");

                    let mut builder = MicrosoftAzureBuilder::new().with_client_options(self.client_options());

                    if let Some(bucket) = bucket_override {
                        builder = builder.with_container_name(bucket);
                    } else if let Some(bucket) = &self.bucket {
                        builder = builder.with_container_name(bucket);
                    }
                    if let Some(account) = &self.azure_storage_account {
                        builder = builder.with_account(account)
                    }
                    if let Some(key) = &self.azure_storage_access_key {
                        builder = builder.with_access_key(key)
                    }

                    Ok(Arc::new(LimitStore::new(
                        builder.build().context(InvalidAzureConfigSnafu)?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                /// Build cache store.
                pub fn make_cache_store(
                    &self
                ) -> Result<Option<Arc<DynObjectStore>>, ParseError> {
                    let Some(endpoint) = &self.cache_endpoint else {
                        return Ok(None);
                    };

                    let store = Arc::new(object_store::aws::AmazonS3Builder::new()
                        // bucket name is ignored by our cache server
                        .with_bucket_name(self.bucket.as_deref().unwrap_or("dummy"))
                        .with_client_options(
                            object_store::ClientOptions::new()
                                .with_allow_http(true)
                                .with_http2_only()
                                // this is the maximum that is allowed by the HTTP/2 standard and is meant to lower the overhead of
                                // submitting TCP packages to the kernel
                                .with_http2_max_frame_size(16777215),
                        )
                        .with_endpoint(endpoint.clone())
                        .with_retry(object_store::RetryConfig {
                            max_retries: 3,
                            ..Default::default()
                        })
                        .with_skip_signature(true)
                        .build()
                        .context(InvalidS3ConfigSnafu)?);

                    // size hint stripping
                    //
                    // This is a temporary workaround until https://github.com/influxdata/influxdb_iox/issues/13771 is fixed.
                    let store = Arc::new(ObjectStoreStripSizeHinting::new(store));

                    Ok(Some(store))
                }

                /// Create config-dependent object store.
                pub fn make_object_store(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    self.make_object_store_custom_bucket(None)
                }

                /// Make a config-dependent object store with an optional bucket override.
                pub fn make_object_store_custom_bucket(&self, bucket_override: Option<String>) -> Result<Arc<DynObjectStore>, ParseError> {
                    if let Some(data_dir) = &self.database_directory {
                        if !matches!(&self.object_store, Some(ObjectStoreType::File)) {
                            warn!(?data_dir, object_store_type=?self.object_store,
                                  "--data-dir / `INFLUXDB_IOX_DB_DIR` ignored. It only affects 'file' object stores");
                        }
                    }

                    let object_store: Arc<DynObjectStore> = match &self.object_store {
                        None => return Err(ParseError::UnspecifiedObjectStore),
                        Some(ObjectStoreType::Memory) => {
                            info!(object_store_type = "Memory", "Object Store");
                            Arc::new(InMemory::new())
                        }
                        Some(ObjectStoreType::MemoryThrottled) => {
                            let config = ThrottleConfig {
                                // for every call: assume a 100ms latency
                                wait_delete_per_call: Duration::from_millis(100),
                                wait_get_per_call: Duration::from_millis(100),
                                wait_list_per_call: Duration::from_millis(100),
                                wait_list_with_delimiter_per_call: Duration::from_millis(100),
                                wait_put_per_call: Duration::from_millis(100),

                                // for list operations: assume we need 1 call per 1k entries at 100ms
                                wait_list_per_entry: Duration::from_millis(100) / 1_000,
                                wait_list_with_delimiter_per_entry: Duration::from_millis(100) / 1_000,

                                // for upload/download: assume 1GByte/s
                                wait_get_per_byte: Duration::from_secs(1) / 1_000_000_000,
                            };

                            info!(?config, object_store_type = "Memory", "Object Store");
                            Arc::new(ThrottledStore::new(InMemory::new(), config))
                        }

                        Some(ObjectStoreType::Google) => self.new_gcs(bucket_override)?,
                        Some(ObjectStoreType::S3) => self.new_s3(bucket_override)?,
                        Some(ObjectStoreType::Azure) => self.new_azure(bucket_override)?,
                        Some(ObjectStoreType::File) => self.new_local_file_system()?,
                    };

                    // size hint stripping
                    //
                    // This is a temporary workaround until https://github.com/influxdata/influxdb_iox/issues/13771 is fixed.
                    let object_store = Arc::new(ObjectStoreStripSizeHinting::new(object_store));

                    Ok(object_store)
                }

                /// Return a closure to build an S3 client.
                /// This function returns a function that return an S3 client (actually a future that when waited
                /// on returns an S3 client).  To do this, capture and clone whatever is necessary so the returned
                /// function can be called later to build clients.
                pub fn s3_client_builder(
                    &self,
                ) -> Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<S3Client, String>> + Send>> + Send + Sync> {
                    let aws_default_region = self.aws_default_region.clone();
                    let aws_endpoint = self.aws_endpoint.clone();

                    Arc::new(move || {
                        let aws_default_region = aws_default_region.clone();
                        let aws_endpoint = aws_endpoint.clone();
                        Box::pin(async move {
                            // Load AWS configuration
                            let config = load_defaults(BehaviorVersion::latest()).await;

                            // Extract credentials
                            let credentials = if let Some(provider) = config.credentials_provider() {
                                match provider.provide_credentials().await {
                                    Ok(credentials) => credentials,
                                    Err(e) => return Err(format!("Failed to authenticate: {:?}", e)),
                                }
                            } else {
                                return Err("No credentials provider found in the configuration.".to_string());
                            };

                            // Create the S3 config builder
                            let mut config_builder = S3ConfigBuilder::new()
                                .region(Region::new(aws_default_region))
                                .credentials_provider(credentials);

                            // Add optional endpoint if provided
                            if let Some(endpoint) = aws_endpoint {
                                config_builder = config_builder.endpoint_url(endpoint.to_string());
                            }

                            // Build the S3 client
                            let config = config_builder
                                .behavior_version(BehaviorVersion::v2025_01_17())
                                .build();
                            Ok(S3Client::from_conf(config))
                        })
                    })
                }

                fn new_local_file_system(&self) -> Result<Arc<LocalFileSystem>, ParseError> {
                    match self.database_directory.as_ref() {
                        Some(db_dir) => {
                            info!(?db_dir, object_store_type = "Directory", "Object Store");
                            fs::create_dir_all(db_dir).context(CreatingDatabaseDirectorySnafu { path: db_dir })?;

                            let store = LocalFileSystem::new_with_prefix(db_dir)
                                .context(CreateLocalFileSystemSnafu { path: db_dir })?;
                            Ok(Arc::new(store))
                        }
                        None => MissingObjectStoreConfigSnafu {
                            object_store: ObjectStoreType::File,
                            missing: "data-dir",
                        }
                        .fail()?,
                    }
                }

            }
        }
    };
}

object_store_config!("source"); // SourceObjectStoreConfig
object_store_config!("sink"); // SinkObjectStoreConfig
object_store_config!(); // ObjectStoreConfig

/// Object-store type.
#[derive(Debug, Copy, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum ObjectStoreType {
    /// In-memory.
    Memory,

    /// In-memory with additional throttling applied for testing
    MemoryThrottled,

    /// Filesystem.
    File,

    /// AWS S3.
    S3,

    /// GCS.
    Google,

    /// Azure object store.
    Azure,
}

impl ObjectStoreType {
    /// Map enum variant to static string, followed inverse of clap parsing rules.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Memory => "memory",
            Self::MemoryThrottled => "memory-throttled",
            Self::File => "file",
            Self::S3 => "s3",
            Self::Google => "google",
            Self::Azure => "azure",
        }
    }
}

/// The `object_store::signer::Signer` trait is implemented for AWS and local file systems, so when
/// the AWS feature is enabled and the configured object store is S3 or the local file system,
/// return a signer.
pub fn make_presigned_url_signer(
    config: &ObjectStoreConfig,
) -> Result<Option<Arc<dyn object_store::signer::Signer>>, ParseError> {
    match &config.object_store {
        Some(ObjectStoreType::S3) => Ok(Some(Arc::new(config.build_s3(None)?))),
        Some(ObjectStoreType::File) => Ok(Some(Arc::new(LocalUploadSigner::new(config)?))),
        _ => Ok(None),
    }
}

/// An implementation of `object_store::signer::Signer` suitable for local testing, where IOx
/// all-in-one mode is running on the same machine that `bulk_ingester` is running. Does NOT
/// actually create presigned URLs; only returns the given path resolved to an absolute `file://`
/// URL that the bulk ingester can write directly to only if the bulk ingester is running on the
/// same system.
///
/// Again, will not work and not intended to work in production, but is useful in local testing.
#[derive(Debug)]
pub struct LocalUploadSigner {
    inner: Arc<LocalFileSystem>,
}

impl LocalUploadSigner {
    fn new(config: &ObjectStoreConfig) -> Result<Self, ParseError> {
        Ok(Self {
            inner: config.new_local_file_system()?,
        })
    }
}

#[async_trait]
impl object_store::signer::Signer for LocalUploadSigner {
    async fn signed_url(
        &self,
        _method: http::Method,
        path: &Path,
        _expires_in: Duration,
    ) -> Result<Url, object_store::Error> {
        self.inner.path_to_filesystem(path).and_then(|path| {
            Url::from_file_path(&path).map_err(|_| object_store::Error::InvalidPath {
                source: object_store::path::Error::InvalidPath { path },
            })
        })
    }
}

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum CheckError {
    #[snafu(display("Cannot read from object store: {}", source))]
    CannotReadObjectStore { source: object_store::Error },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use object_store::ObjectStore;
    use std::{env, str::FromStr};
    use tempfile::TempDir;

    /// The current object store store configurations.
    enum StoreConfigs {
        Base(ObjectStoreConfig),
        Source(SourceObjectStoreConfig),
        Sink(SinkObjectStoreConfig),
    }

    impl StoreConfigs {
        pub(crate) fn make_object_store(&self) -> Result<Arc<dyn ObjectStore>, ParseError> {
            self.object_store_inner(None)
        }
        pub(crate) fn make_object_store_custom_bucket(
            &self,
            bucket_override: Option<String>,
        ) -> Result<Arc<dyn ObjectStore>, ParseError> {
            self.object_store_inner(bucket_override)
        }

        fn object_store_inner(
            &self,
            bucket_override: Option<String>,
        ) -> Result<Arc<dyn ObjectStore>, ParseError> {
            match self {
                Self::Base(o) => o.make_object_store_custom_bucket(bucket_override),
                Self::Source(o) => o.make_object_store_custom_bucket(bucket_override),
                Self::Sink(o) => o.make_object_store_custom_bucket(bucket_override),
            }
        }
    }

    #[test]
    fn object_store_flag_is_required() {
        let configs = vec![
            StoreConfigs::Base(ObjectStoreConfig::try_parse_from(["server"]).unwrap()),
            StoreConfigs::Source(SourceObjectStoreConfig::try_parse_from(["server"]).unwrap()),
            StoreConfigs::Sink(SinkObjectStoreConfig::try_parse_from(["server"]).unwrap()),
        ];
        for config in configs {
            let err = config.make_object_store().unwrap_err().to_string();
            assert_eq!(err, "Object store not specified");
        }
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "memory",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from(["server", "--sink-object-store", "memory"])
                    .unwrap(),
            ),
        ];
        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(&object_store.to_string(), "strip_size_hinting(InMemory)")
        }
    }

    #[test]
    fn default_url_signer_is_none() {
        let config = ObjectStoreConfig::try_parse_from(["server"]).unwrap();

        let signer = make_presigned_url_signer(&config).unwrap();
        assert!(signer.is_none(), "Expected None, got {signer:?}");
    }

    #[test]
    fn valid_s3_config() {
        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "s3",
                    "--bucket",
                    "mybucket",
                    "--aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "s3",
                    "--source-bucket",
                    "mybucket",
                    "--source-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--source-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "s3",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--sink-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(
                &object_store.to_string(),
                "strip_size_hinting(LimitStore(1024, AmazonS3(mybucket)))"
            )
        }
    }

    #[test]
    fn s3_config_bucket_override() {
        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "s3",
                    "--bucket",
                    "mybucket",
                    "--aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "s3",
                    "--source-bucket",
                    "mybucket",
                    "--source-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--source-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "s3",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--sink-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config
                .make_object_store_custom_bucket(Some("my_other_bucket".to_string()))
                .unwrap();
            assert_eq!(
                &object_store.to_string(),
                "strip_size_hinting(LimitStore(1024, AmazonS3(my_other_bucket)))"
            )
        }
    }

    #[test]
    fn valid_s3_endpoint_url() {
        ObjectStoreConfig::try_parse_from(["server", "--aws-endpoint", "http://whatever.com"])
            .expect("must successfully parse config with absolute AWS endpoint URL");
    }

    #[test]
    fn invalid_s3_endpoint_url_fails_clap_parsing() {
        let result =
            ObjectStoreConfig::try_parse_from(["server", "--aws-endpoint", "whatever.com"]);
        assert!(result.is_err(), "{result:?}");
        let result = SourceObjectStoreConfig::try_parse_from([
            "server",
            "--source-aws-endpoint",
            "whatever.com",
        ]);
        assert!(result.is_err(), "{result:?}");
        let result = SinkObjectStoreConfig::try_parse_from([
            "server",
            "--sink-aws-endpoint",
            "whatever.com",
        ]);
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn s3_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Amazon S3: Generic S3 error: Missing bucket name"
        );
    }

    #[test]
    fn valid_s3_url_signer() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "s3",
            "--bucket",
            "mybucket",
            "--aws-access-key-id",
            "NotARealAWSAccessKey",
            "--aws-secret-access-key",
            "NotARealAWSSecretAccessKey",
        ])
        .unwrap();

        assert!(make_presigned_url_signer(&config).unwrap().is_some());

        // Even with the aws feature on, object stores (other than local files) shouldn't create a
        // signer.
        let config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

        let signer = make_presigned_url_signer(&config).unwrap();
        assert!(signer.is_none(), "Expected None, got {signer:?}");
    }

    #[test]
    fn s3_url_signer_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = make_presigned_url_signer(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Amazon S3: Generic S3 error: Missing bucket name"
        );
    }

    #[test]
    fn valid_google_config() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().expect("tempfile should be created");
        const FAKE_KEY: &str = r#"{"private_key": "private_key", "private_key_id": "private_key_id", "client_email":"client_email", "disable_oauth":true}"#;
        writeln!(file, "{FAKE_KEY}").unwrap();
        let path = file.path().to_str().expect("file path should exist");

        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "google",
                    "--bucket",
                    "mybucket",
                    "--google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "google",
                    "--source-bucket",
                    "mybucket",
                    "--source-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "google",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(
                &object_store.to_string(),
                "strip_size_hinting(LimitStore(1024, GoogleCloudStorage(mybucket)))"
            )
        }
    }

    #[test]
    fn google_config_bucket_override() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().expect("tempfile should be created");
        const FAKE_KEY: &str = r#"{"private_key": "private_key", "private_key_id": "private_key_id", "client_email":"client_email", "disable_oauth":true}"#;
        writeln!(file, "{FAKE_KEY}").unwrap();
        let path = file.path().to_str().expect("file path should exist");

        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "google",
                    "--bucket",
                    "mybucket",
                    "--google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "google",
                    "--source-bucket",
                    "mybucket",
                    "--source-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "google",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config
                .make_object_store_custom_bucket(Some("my_other_bucket".to_string()))
                .unwrap();
            assert_eq!(
                &object_store.to_string(),
                "strip_size_hinting(LimitStore(1024, GoogleCloudStorage(my_other_bucket)))"
            )
        }
    }

    #[test]
    fn google_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "google"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring GCS: Generic GCS error: Missing bucket name"
        );
    }

    #[test]
    fn valid_azure_config() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "azure",
            "--bucket",
            "mybucket",
            "--azure-storage-account",
            "NotARealStorageAccount",
            "--azure-storage-access-key",
            "Zm9vYmFy", // base64 encoded "foobar"
        ])
        .unwrap();

        let object_store = config.make_object_store().unwrap();
        assert_eq!(
            &object_store.to_string(),
            "strip_size_hinting(LimitStore(1024, MicrosoftAzure { account: NotARealStorageAccount, container: mybucket }))"
        )
    }

    #[test]
    fn azure_config_bucket_override() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "azure",
            "--bucket",
            "mybucket",
            "--azure-storage-account",
            "NotARealStorageAccount",
            "--azure-storage-access-key",
            "Zm9vYmFy", // base64 encoded "foobar"
        ])
        .unwrap();

        let object_store = config
            .make_object_store_custom_bucket(Some("my_other_bucket".to_string()))
            .unwrap();
        assert_eq!(
            &object_store.to_string(),
            "strip_size_hinting(LimitStore(1024, MicrosoftAzure { account: NotARealStorageAccount, container: my_other_bucket }))"
        )
    }

    #[test]
    fn azure_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "azure"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Microsoft Azure: Generic MicrosoftAzure error: Container name must be specified"
        );
    }

    #[test]
    fn valid_file_config() {
        let root = TempDir::new().unwrap();
        let root_path = root.path().to_str().unwrap();

        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "file",
            "--data-dir",
            root_path,
        ])
        .unwrap();

        let object_store = config.make_object_store().unwrap().to_string();
        assert!(
            object_store.starts_with("strip_size_hinting(LocalFileSystem"),
            "{}",
            object_store
        )
    }

    #[test]
    fn file_config_missing_params() {
        // this test tests for failure to configure the object store because of data-dir configuration missing
        // if the INFLUXDB_IOX_DB_DIR env variable is set, the test fails because the configuration is
        // actually present.
        // SAFETY: this is not actually necessarily sound. It's just extremely difficult (if not
        // outright impossible with the guarantees that rust's test harness gives us) to architect
        // tests to ensure that they are completely single-threaded and that this operation can
        // actually be sound
        unsafe {
            env::remove_var("INFLUXDB_IOX_DB_DIR");
        }

        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from(["server", "--object-store", "file"]).unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "file",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from(["server", "--sink-object-store", "file"])
                    .unwrap(),
            ),
        ];

        for config in configs {
            let err = config.make_object_store().unwrap_err().to_string();
            assert_eq!(
                err,
                "Specified File for the object store, required configuration missing for \
            data-dir"
            );
        }
    }

    #[tokio::test]
    async fn local_url_signer() {
        let root = TempDir::new().unwrap();
        let root_path = root.path().to_str().unwrap();
        let parquet_file_path = "1/2/something.parquet";

        let signer = make_presigned_url_signer(
            &ObjectStoreConfig::try_parse_from([
                "server",
                "--object-store",
                "file",
                "--data-dir",
                root_path,
            ])
            .unwrap(),
        )
        .unwrap()
        .unwrap();

        let object_store_parquet_file_path = Path::parse(parquet_file_path).unwrap();
        let upload_url = signer
            .signed_url(
                http::Method::PUT,
                &object_store_parquet_file_path,
                Duration::from_secs(100),
            )
            .await
            .unwrap();

        assert_eq!(
            upload_url.as_str(),
            &format!(
                "file://{}",
                std::fs::canonicalize(root.path())
                    .unwrap()
                    .join(parquet_file_path)
                    .display()
            )
        );
    }

    #[test]
    fn endpoint() {
        assert_eq!(
            Endpoint::from_str("http://localhost:8080")
                .unwrap()
                .to_string(),
            "http://localhost:8080",
        );
        assert_eq!(
            Endpoint::from_str("http://localhost:8080/")
                .unwrap()
                .to_string(),
            "http://localhost:8080",
        );
        assert_eq!(
            Endpoint::from_str("whatever.com").unwrap_err().to_string(),
            "relative URL without a base",
        );
    }
}
