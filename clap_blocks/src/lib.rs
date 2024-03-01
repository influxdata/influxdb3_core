//! Building blocks for [`clap`]-driven configs.
//!
//! They can easily be re-used using `#[clap(flatten)]`.
#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod bulk_ingest;
pub mod catalog_cache;
pub mod catalog_dsn;
pub mod compactor;
pub mod compactor_scheduler;
pub mod controller;
pub mod garbage_collector;
pub mod gossip;
pub mod ingester;
pub mod ingester_address;
pub mod memory_size;
pub mod object_store;
pub mod parquet_cache;
pub mod parquet_write_hint;
pub mod querier;
pub mod router;
pub mod run_config;
pub mod single_tenant;
pub mod socket_addr;
