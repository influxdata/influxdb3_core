//! Memory cache implementation for [`ObjectStore`](object_store::ObjectStore)

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod cache_system;
mod meta_store;
mod store;

pub use meta_store::{MetaIndexCache, MetaIndexCacheParams};
pub use store::{MemCacheObjectStore, MemCacheObjectStoreParams};
