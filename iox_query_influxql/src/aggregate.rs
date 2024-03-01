//! User defined aggregate functions implementing influxQL features.

use datafusion::logical_expr::AggregateUDF;
use once_cell::sync::Lazy;
use std::sync::Arc;

mod mode;
mod percentile;
mod spread;

/// Definition of the `PERCENTILE` user-defined aggregate function.
pub(crate) static PERCENTILE: Lazy<Arc<AggregateUDF>> =
    Lazy::new(|| Arc::new(AggregateUDF::new_from_impl(percentile::PercentileUDF::new())));

pub(crate) static SPREAD: Lazy<Arc<AggregateUDF>> =
    Lazy::new(|| Arc::new(AggregateUDF::new_from_impl(spread::SpreadUDF::new())));

pub(crate) static MODE: Lazy<Arc<AggregateUDF>> =
    Lazy::new(|| Arc::new(AggregateUDF::new_from_impl(mode::ModeUDF::new())));
