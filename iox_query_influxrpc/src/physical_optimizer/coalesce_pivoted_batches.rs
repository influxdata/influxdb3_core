use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

use crate::exec::SeriesPivotExec;

/// Physical optimizer rule that coalesces batches after the SeriesPivotExec node.
/// SeriesPivotExec may filter a large number of nodes leading to many small
/// batches being produced.
#[derive(Debug, Default)]
pub(crate) struct CoalescePivotedBatches {}

impl PhysicalOptimizerRule for CoalescePivotedBatches {
    fn name(&self) -> &str {
        "coalesce_pivoted_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }
        plan.transform_up(|plan| {
            if plan.as_any().is::<SeriesPivotExec>() {
                Ok(Transformed::yes(Arc::new(CoalesceBatchesExec::new(
                    plan,
                    config.execution.batch_size,
                ))))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .map(|transformed| transformed.data)
    }
}
