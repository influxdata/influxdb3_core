use crate::extension_planner::InfluxRpcExtensionPlanner;
use crate::physical_optimizer::CoalescePivotedBatches;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_planner::ExtensionPlanner;
use iox_query::Extension;
use std::sync::Arc;

/// Extension for the InfluxRPC query specifiv functionality.
#[derive(Debug, Default, Copy, Clone)]
pub struct InfluxRpcExtension {}

impl Extension for InfluxRpcExtension {
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        Some(Arc::new(InfluxRpcExtensionPlanner {}))
    }

    fn physical_optimizer_rules(&self) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        vec![Arc::new(CoalescePivotedBatches::default())]
    }
}
