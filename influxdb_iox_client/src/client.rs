/// Client for interacting with a remote catalog
pub mod catalog;

/// Client for the compactor API
pub mod compactor;

/// Client for delete API
pub mod delete;

/// Errors for the client
pub mod error;

#[cfg(feature = "flight")]
/// Client for IOx Flight API (native query API)
pub mod flight;

#[cfg(feature = "flight")]
/// Client for Flight SQL
pub mod flightsql;

/// Client for health checking API
pub mod health;

/// Client for the ingester API
pub mod ingester;

/// Client for namespace API
pub mod namespace;

/// Client for query log API.
pub mod query_log;

/// Client for reflection API
pub mod reflection;

/// Client for schema API
pub mod schema;

/// Client for interacting with a remote object store
pub mod store;

/// Client for table API
pub mod table;

/// Client for testing purposes.
pub mod test;

/// Client for write API
pub mod write;
