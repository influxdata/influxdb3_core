//! Rules specific to [`SortExec`].
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

pub mod order_union_sorted_inputs;
pub mod order_union_sorted_inputs_for_constants;
pub mod parquet_sortness;
pub mod util;
