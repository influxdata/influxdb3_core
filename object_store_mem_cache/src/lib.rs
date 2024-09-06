//! Memory cache implementation for [`ObjectStore`](object_store::ObjectStore)

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod cache_system;
pub mod object_store_cache_tests;
pub mod object_store_helpers;
pub mod store;

pub use store::{MemCacheObjectStore, MemCacheObjectStoreParams};
