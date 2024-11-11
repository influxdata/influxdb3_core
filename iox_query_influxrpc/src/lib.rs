//! Query frontend for InfluxDB Storage gRPC requests

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod error;
mod exec;
mod extension;
mod extension_planner;
mod physical_optimizer;
mod plan;
mod planner;
pub mod schema;

pub use error::InfluxRpcError;
pub use extension::InfluxRpcExtension;
pub use planner::InfluxRpcPlanner;

/// Name of the single column that is produced by queries
/// that form a set of string values.
const STRING_VALUE_COLUMN_NAME: &str = "string_value";
