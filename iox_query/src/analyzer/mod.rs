use std::sync::Arc;

use datafusion::execution::context::SessionState;

use self::{extract_sleep::ExtractSleep, handle_gapfill::HandleGapFill};

mod extract_sleep;
mod handle_gapfill;
pub use handle_gapfill::range_predicate;

/// Register IOx-specific [`AnalyzerRule`]s with the SessionContext
///
/// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
pub fn register_iox_analyzers(mut state: SessionState) -> SessionState {
    state.add_analyzer_rule(Arc::new(ExtractSleep::new()));

    state
        .add_analyzer_rule(Arc::new(HandleGapFill::new()))
        .clone()
}
