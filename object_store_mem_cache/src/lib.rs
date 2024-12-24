//! Memory cache implementation for [`ObjectStore`](object_store::ObjectStore)

// paste is only used in the tests, but cannot be hidden behind #[cfg(test)]
// because we also use those functions in another module.
use paste as _;

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use clap as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

pub mod attributes;
pub mod cache_system;
pub mod object_store_cache_tests;
pub mod object_store_helpers;
pub mod store;

pub use store::{MemCacheObjectStore, MemCacheObjectStoreParams};
