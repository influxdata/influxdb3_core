use datafusion::{
    optimizer::{AnalyzerRule, OptimizerRule},
    physical_optimizer::PhysicalOptimizerRule,
    physical_planner::ExtensionPlanner,
};
use std::sync::Arc;

/// Trait implemented by extension that add functionality
/// to the IOx querier.
pub trait Extension {
    /// Optional physical planner to configure for the extension.
    ///
    /// The default implementation returns `None`.
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        None
    }

    /// Analyzer rules to add for the extension.
    ///
    /// The default implementation returns an empty list.
    fn analyzer_rules(&self) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        vec![]
    }

    /// Optimizer rules to add for the extension.
    ///
    /// The default implementation returns an empty list.
    fn optimizer_rules(&self) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        vec![]
    }

    /// Physical optimizer rules to add for the extension.
    ///
    /// The default implementation returns an empty list.
    fn physical_optimizer_rules(&self) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        vec![]
    }
}

/// Empty extension that does not add any functionality.
#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct EmptyExtension {}

impl Extension for EmptyExtension {}

impl<T> Extension for Arc<T>
where
    T: Extension,
{
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        (**self).planner()
    }

    fn analyzer_rules(&self) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        (**self).analyzer_rules()
    }

    fn optimizer_rules(&self) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        (**self).optimizer_rules()
    }

    fn physical_optimizer_rules(&self) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        (**self).physical_optimizer_rules()
    }
}

impl<T> Extension for Box<T>
where
    T: Extension,
{
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        (**self).planner()
    }

    fn analyzer_rules(&self) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        (**self).analyzer_rules()
    }

    fn optimizer_rules(&self) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        (**self).optimizer_rules()
    }

    fn physical_optimizer_rules(&self) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        (**self).physical_optimizer_rules()
    }
}
