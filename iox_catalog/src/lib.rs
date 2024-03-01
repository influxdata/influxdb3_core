//! The IOx catalog keeps track of the namespaces, tables, columns, parquet files,
//! and deletes in the system. Configuration information for distributing ingest, query
//! and compaction is also stored here.
#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod cache;
pub mod constants;
pub mod grpc;
pub mod interface;
pub mod mem;
pub mod metrics;
pub mod migrate;
pub mod postgres;
pub mod sqlite;
pub mod test_helpers;
pub mod util;

#[cfg(test)]
pub(crate) mod interface_tests;
