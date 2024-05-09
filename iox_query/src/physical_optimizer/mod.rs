use std::sync::Arc;

use datafusion::{execution::context::SessionState, physical_optimizer::PhysicalOptimizerRule};

pub(crate) use self::limits::ParquetFileMetrics;
use self::{
    cached_parquet_data::CachedParquetData,
    dedup::{
        dedup_null_columns::DedupNullColumns, dedup_sort_order::DedupSortOrder, split::SplitDedup,
    },
    limits::CheckLimits,
    predicate_pushdown::PredicatePushdown,
    projection_pushdown::ProjectionPushdown,
    sort::{
        order_union_sorted_inputs::OrderUnionSortedInputs,
        order_union_sorted_inputs_for_constants::OrderUnionSortedInputsForConstants,
        parquet_sortness::ParquetSortness,
    },
    union::{nested_union::NestedUnion, one_union::OneUnion},
};

mod cached_parquet_data;
mod chunk_extraction;
mod dedup;
mod limits;
mod predicate_pushdown;
mod projection_pushdown;
mod sort;
mod union;

#[cfg(test)]
mod test_util;

#[cfg(test)]
mod tests;

/// Register IOx-specific [`PhysicalOptimizerRule`]s with the SessionContext
pub fn register_iox_physical_optimizers(state: SessionState) -> SessionState {
    // prepend IOx-specific rules to DataFusion builtins
    // The optimizer rules have to be done in this order
    let mut optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
        Arc::new(SplitDedup),
        Arc::new(DedupNullColumns),
        Arc::new(DedupSortOrder),
        Arc::new(PredicatePushdown),
        Arc::new(ProjectionPushdown),
        Arc::new(ParquetSortness) as _,
        Arc::new(NestedUnion),
        Arc::new(OneUnion),
    ];

    // Append DataFusion physical rules to the IOx-specific rules
    optimizers.append(&mut state.physical_optimizers().to_vec());

    // install cached parquet readers AFTER DataFusion (re-)creates ParquetExec's
    optimizers.push(Arc::new(CachedParquetData));

    // Add a rule to optimize plan that use ProgressiveEval
    // for limit query
    optimizers.push(Arc::new(OrderUnionSortedInputs));
    // for show tag values query
    optimizers.push(Arc::new(OrderUnionSortedInputsForConstants));

    // Perform the limits check last giving the other rules the best chance
    // to keep the under the limit.
    optimizers.push(Arc::new(CheckLimits));

    state.with_physical_optimizer_rules(optimizers)
}
