//! Contains the IOx InfluxQL query planner

use arrow::datatypes::DataType;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod aggregate;
mod error;
pub mod frontend;
pub mod plan;
mod window;

/// A list of the numeric types supported by InfluxQL that can be be used
/// as input to user-defined functions.
static NUMERICS: &[DataType] = &[DataType::Int64, DataType::UInt64, DataType::Float64];
