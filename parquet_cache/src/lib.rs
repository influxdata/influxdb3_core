//! IOx parquet cache client.
//!
//! ParquetCache client interface to be used by IOx components to
//! get and put parquet files into the cache.

#![warn(missing_docs)]
#![allow(rustdoc::private_intra_doc_links, unreachable_pub)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod client;
pub use client::{make_client, write_hints::WriteHintingObjectStore};

pub mod controller;

pub(crate) mod data_types;
pub use data_types::PolicyConfig;

mod server;
pub use server::{build_cache_server, ParquetCacheServer, ParquetCacheServerConfig, ServerError};

#[cfg(test)]
pub(crate) use server::mock::MockCacheServer;
#[cfg(test)]
mod tests;
