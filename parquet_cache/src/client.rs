//! Contains the cache client.

/// Interface for the object store. Consumed by Iox components.
pub mod object_store;
/// Interface for write hinting. Consumed by Iox components.
pub mod write_hints;

/// Connection to remote data cache. Used by the ObjectStore cache impl.
pub(crate) mod cache_connector;
pub(crate) mod http;
pub(crate) mod keyspace;
pub(crate) mod request;

/// Mocks used for internal testing
#[cfg(test)]
pub(crate) mod mock;

use ::object_store::ObjectStore;
use std::sync::Arc;

use crate::client::{cache_connector::build_cache_connector, object_store::DataCacheObjectStore};

/// Build a cache client.
pub fn make_client(
    namespace_service_address: String,
    object_store: Arc<dyn ObjectStore>,
) -> Arc<DataCacheObjectStore> {
    let server_connection = build_cache_connector(namespace_service_address);
    Arc::new(DataCacheObjectStore::new(server_connection, object_store))
}
